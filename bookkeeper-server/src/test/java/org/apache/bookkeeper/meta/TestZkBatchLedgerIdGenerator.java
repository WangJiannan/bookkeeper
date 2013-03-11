/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.meta;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestZkBatchLedgerIdGenerator extends TestCase {

    static CountDownLatch countDownFatchBatchID = null;

    static class HackZkBatchLedgerIdGenerator extends ZkBatchLedgerIdGenerator {
        @Override
        protected void finishFetchBatchID(int slot, long start, int batch) {
            super.finishFetchBatchID(slot, start, batch);
            if (countDownFatchBatchID != null) {
                countDownFatchBatchID.countDown();
            }
        }
    }

    AbstractConfiguration conf = new ServerConfiguration() {
        public int getLedgerIdBatchSize() {
            return 1;
        }
    };

    ZooKeeperUtil zkutil;
    ZooKeeper zk;

    ZkBatchLedgerIdGenerator ledgerIdGenerator;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        zkutil = new ZooKeeperUtil();
        zkutil.startServer();
        zk = zkutil.getZooKeeperClient();

        ledgerIdGenerator = new HackZkBatchLedgerIdGenerator();
        ledgerIdGenerator.initialize(conf, zk);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        zk.close();
        zkutil.killServer();

        super.tearDown();
    }

    @Test
    public void testGenerateLedgerId() throws Exception {
        for (int slot = 0; slot < ZkBatchLedgerIdGenerator.NUM_SLOTS; slot++) {
            for (int id = 0; id < 10; id++) {
                countDownFatchBatchID = new CountDownLatch(1);
                ledgerIdGenerator.fetchLedgerIdBatch(slot);
                assertTrue("Wait ledger id generator fetch batch ids timeout : ",
                        countDownFatchBatchID.await(5, TimeUnit.SECONDS));

                final AtomicLong ledgerId = new AtomicLong();
                final AtomicBoolean error = new AtomicBoolean(true);
                final CountDownLatch countDownLatch = new CountDownLatch(1);
                ledgerIdGenerator.generateLedgerId(new GenericCallback<Long>() {
                    @Override
                    public void operationComplete(int rc, Long result) {
                        if (Code.OK.intValue() == rc) {
                            error.set(false);
                            ledgerId.set(result);
                        }
                        countDownLatch.countDown();
                    }
                });
                assertTrue("Wait ledger id generation callback timeout : ",
                        countDownLatch.await(5, TimeUnit.SECONDS));
                assertFalse("Error happen during ledger id generation : ", error.get());
                assertEquals("Unexpected ledger id generated, slot=" + slot + ", id=" + id + " : ",
                        ZkBatchLedgerIdGenerator.getLedgerID(slot, id), ledgerId.get());
            }
        }
    }

    @Test
    public void testFetchBatchIdsInSameSlot() throws Exception {
        ZkBatchLedgerIdGenerator anotherLedgerIdGenerator = new HackZkBatchLedgerIdGenerator();
        anotherLedgerIdGenerator.initialize(conf, zk);

        final ConcurrentLinkedQueue<Long> ledgerIds = new ConcurrentLinkedQueue<Long>();
        final int slot = 5;
        for (int id = 0; id < 1000; id++) {
            // fetch batch id in the same slot
            countDownFatchBatchID = new CountDownLatch(2);
            ledgerIdGenerator.fetchLedgerIdBatch(slot);
            anotherLedgerIdGenerator.fetchLedgerIdBatch(slot);
            assertTrue("Wait ledger id generator fetch batch ids timeout : ",
                    countDownFatchBatchID.await(5, TimeUnit.SECONDS));

            final AtomicInteger errCount = new AtomicInteger(0);
            final CountDownLatch countDownLatch = new CountDownLatch(2);

            GenericCallback<Long> cb = new GenericCallback<Long>() {
                @Override
                public void operationComplete(int rc, Long result) {
                    if (Code.OK.intValue() == rc) {
                        ledgerIds.add(result);
                    } else {
                        errCount.incrementAndGet();
                    }
                    countDownLatch.countDown();
                }
            };
            ledgerIdGenerator.generateLedgerId(cb);
            anotherLedgerIdGenerator.generateLedgerId(cb);
            assertTrue("Wait ledger id generation callback timeout : ",
                    countDownLatch.await(5, TimeUnit.SECONDS));
            assertEquals("Error occur during ledger id generation : ", 0, errCount.get());
        }

        Set<Long> ledgers = new HashSet<Long>();
        while (!ledgerIds.isEmpty()) {
            Long ledger = ledgerIds.poll();
            assertNotNull("Generated ledger id is null : ", ledger);
            assertFalse("Ledger id [" + ledger + "] conflict : ", ledgers.contains(ledger));
            ledgers.add(ledger);
        }
    }

}
