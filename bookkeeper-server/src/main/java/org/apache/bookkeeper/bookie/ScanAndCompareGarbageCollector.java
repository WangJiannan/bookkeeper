/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.bookie;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.SnapshotMap;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Garbage collector implementation using scan and compare mechanism.
 *
 * <p>The idea behind this implementation is:<ul>
 * <li>Assume the ledger id list in local bookie server is <b>LocalLedgers</b></li>
 * <li>At the same time, the ledger id list at metadata storage is <b>LiveLedgers</b></li>
 * <li>Then the ledgers require garbage collection are <b>LocalLedgers - LiveLedgers</b></li>
 * </ul></p>
 */
public class ScanAndCompareGarbageCollector implements GarbageCollector{

    static final Logger LOG = LoggerFactory.getLogger(ScanAndCompareGarbageCollector.class);
    private SnapshotMap<Long, Boolean> activeLedgers;
    private LedgerManager ledgerManager;

    public ScanAndCompareGarbageCollector(LedgerManager ledgerManager, SnapshotMap<Long, Boolean> activeLedgers) {
        this.ledgerManager = ledgerManager;
        this.activeLedgers = activeLedgers;
    }

    @Override
    public void gc(GarbageCleaner garbageCleaner) {
        LOG.info("Start ScanAndCompare garbage collection...");

        // Take a snapshot on current active ledgers to do GC
        NavigableMap<Long, Boolean> snapshot = activeLedgers.snapshot();

        // Notice that the snapshot is a reference object so we need to make a copy
        final ConcurrentSkipListSet<Long> removedLedgers = new ConcurrentSkipListSet<Long>(snapshot.keySet());

        final int successRc = 0;
        final int failureRc = 1;
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicInteger result = new AtomicInteger();

        ledgerManager.asyncProcessLedgers(new Processor<Long>() {
            @Override
            public void process(Long data, VoidCallback cb) {
                removedLedgers.remove(data);
                cb.processResult(successRc, null, null);
            }
        }, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                result.set(rc);
                countDownLatch.countDown();
            }
        }, null, successRc, failureRc);

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            LOG.error("Get InterruptedException when looping all ledgers in metadata storage");
            return;
        }

        if (result.get() != successRc) {
            LOG.error("Failed to loop all ledgers in metadata storage: {}", result.get());
            return;
        }

        LOG.debug("Garbage collect on ledgers: {}", removedLedgers);
        for (Long lid : removedLedgers) {
            activeLedgers.remove(lid);
            garbageCleaner.clean(lid);
        }

        LOG.info("Finish ScanAndCompare garbage collection, remove {} ledgers", removedLedgers.size());
    }
}
