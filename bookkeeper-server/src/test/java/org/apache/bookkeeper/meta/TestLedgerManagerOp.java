/**
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
 */
package org.apache.bookkeeper.meta;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManagerTestUtils.CallbackResult;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLedgerManagerOp extends LedgerManagerTestCase {
    static Logger LOG = LoggerFactory.getLogger(TestLedgerManagerOp.class);

    final long nLedgerId = 123456789;

    public TestLedgerManagerOp(Class<? extends LedgerManagerFactory> lmFactoryCls) {
        super(lmFactoryCls);
    }

    @Test
    public void testCreateLedgerMetadata() throws Exception {
        final LedgerManager ledgerManager = getLedgerManager();
        LedgerMetadata metadata = new LedgerMetadata(3, 2, 1, BookKeeper.DigestType.MAC, new byte[0]);
        int rc;

        // Case 1: normal create ledger metadata
        LOG.info("Test testCreateLedgerMetadata: normal create ledger metadata");
        rc = LedgerManagerTestUtils.createLedgerMetadata(ledgerManager, nLedgerId, metadata);
        assertEquals("Failed to create ledger metadata : ", BKException.Code.OK, rc);

        // Case 2: create with an exist ledger id
        LOG.info("Test testCreateLedgerMetadata: create with an exist ledger id");
        rc = LedgerManagerTestUtils.createLedgerMetadata(ledgerManager, nLedgerId, metadata);
        assertEquals("Expect LedgerExistException when creating ledger with an exist ledger id : ",
                BKException.Code.LedgerExistException, rc);

        // Case 3: stop zk cluster and expect create ledger metadata response
        // ZKException
        if (!LedgerManagerTestUtils.isZkBasedLedgerManager(ledgerManager)) {
            return;
        }
        LOG.info("Test testCreateLedgerMetadata: stop zk cluster and expect create ledger metadata response");
        stopZKCluster();
        rc = LedgerManagerTestUtils.createLedgerMetadata(ledgerManager, nLedgerId, metadata);
        assertEquals("Expect ZKException since zk is stopped : ", BKException.Code.ZKException, rc);
    }

    @Test
    public void testRemoveLedgerMetadata() throws Exception {
        final LedgerManager ledgerManager = getLedgerManager();
        LedgerMetadata metadata = new LedgerMetadata(3, 2, 1, BookKeeper.DigestType.MAC, new byte[0]);
        int rc;

        // Case 1: delete on not exist ledger
        LOG.info("Test testRemoveLedgerMetadata: delete on not exist ledger");
        rc = LedgerManagerTestUtils.removeLedgerMetadata(ledgerManager, nLedgerId);
        assertEquals("delete on not exist ledger get unexpected result : ",
                BKException.Code.NoSuchLedgerExistsException, rc);

        // Case 2: normal delete
        LOG.info("Test testRemoveLedgerMetadata: normal delete");
        rc = LedgerManagerTestUtils.createLedgerMetadata(ledgerManager, nLedgerId, metadata);
        assertEquals("Failed to create ledger metadata : ", BKException.Code.OK, rc);

        rc = LedgerManagerTestUtils.removeLedgerMetadata(ledgerManager, nLedgerId);
        assertEquals("Delete ledger failed : ", BKException.Code.OK, rc);
        CallbackResult result = LedgerManagerTestUtils.readLedgerMetadata(ledgerManager, nLedgerId);
        assertEquals("Ledger is not deleted : ", BKException.Code.NoSuchLedgerExistsException,
                result.returnCode);

        // Case 3: stop zk cluster and expect delete ledger metadata response
        // ZKException
        if (!LedgerManagerTestUtils.isZkBasedLedgerManager(ledgerManager)) {
            return;
        }
        LOG.info("Test testRemoveLedgerMetadata: stop zk cluster and expect delete ledger metadata response");
        stopZKCluster();
        rc = LedgerManagerTestUtils.removeLedgerMetadata(ledgerManager, nLedgerId);
        assertEquals("Expect ZKException since zk is stopped : ", BKException.Code.ZKException, rc);
    }

    @Test
    public void testReadLedgerMetadata() throws Exception {
        final LedgerManager ledgerManager = getLedgerManager();
        LedgerMetadata metadata = new LedgerMetadata(3, 2, 1, BookKeeper.DigestType.MAC, new byte[0]);
        int rc;
        CallbackResult result;

        // Case 1: read on not exist ledger
        LOG.info("Test testReadLedgerMetadata: read on not exist ledger");
        result = LedgerManagerTestUtils.readLedgerMetadata(ledgerManager, nLedgerId);
        assertEquals("Ledger should not exist : ", BKException.Code.NoSuchLedgerExistsException,
                result.returnCode);

        // Case 2: normal read
        LOG.info("Test testReadLedgerMetadata: normal read");
        rc = LedgerManagerTestUtils.createLedgerMetadata(ledgerManager, nLedgerId, metadata);
        assertEquals("Failed to create ledger metadata : ", BKException.Code.OK, rc);

        result = LedgerManagerTestUtils.readLedgerMetadata(ledgerManager, nLedgerId);
        assertEquals("Read ledger metadata failed : ", BKException.Code.OK, result.returnCode);
        assertEquals("Write and read metadata does not match : ", new String(metadata.serialize()),
                new String(result.metadata.serialize()));

        // Case 3: stop zk cluster and expect read ledger metadata response
        // ZKException
        if (!LedgerManagerTestUtils.isZkBasedLedgerManager(ledgerManager)) {
            return;
        }
        LOG.info("Test testReadLedgerMetadata: stop zk cluster and expect delete ledger metadata response");
        stopZKCluster();
        result = LedgerManagerTestUtils.readLedgerMetadata(ledgerManager, nLedgerId);
        assertEquals("Expect ZKException since zk is stopped : ", BKException.Code.ZKException,
                result.returnCode);
    }

    @Test
    public void testWriteLedgerMetadata() throws Exception {
        final LedgerManager ledgerManager = getLedgerManager();
        LedgerMetadata metadata = new LedgerMetadata(3, 2, 1, BookKeeper.DigestType.MAC, new byte[0]);
        int rc;
        CallbackResult result;

        rc = LedgerManagerTestUtils.createLedgerMetadata(ledgerManager, nLedgerId, metadata);
        assertEquals("Failed to create ledger metadata : ", BKException.Code.OK, rc);

        // Case 1: normal write
        LOG.info("Test testWriteLedgerMetadata: normal write");
        LedgerMetadata newMeta = new LedgerMetadata(4, 4, 4, BookKeeper.DigestType.CRC32, "pwd".getBytes());
        newMeta.setVersion(metadata.getVersion());
        rc = LedgerManagerTestUtils.writeLedgerMetadata(ledgerManager, nLedgerId, newMeta);
        assertEquals("Write ledger metadata failed : ", BKException.Code.OK, rc);
        result = LedgerManagerTestUtils.readLedgerMetadata(ledgerManager, nLedgerId);
        assertEquals("Read ledger metadata failed : ", BKException.Code.OK, result.returnCode);
        assertEquals("Write and read metadata does not match : ", new String(newMeta.serialize()),
                new String(result.metadata.serialize()));

        // Case 2: Bad version
        LOG.info("Test testWriteLedgerMetadata: Bad version");
        newMeta.setVersion(metadata.getVersion());
        rc = LedgerManagerTestUtils.writeLedgerMetadata(ledgerManager, nLedgerId, newMeta);
        assertEquals("Expect bad version for writing ledger metadata : ",
                BKException.Code.MetadataVersionException, rc);

        // Case 3: stop zk cluster and expect write ledger metadata response
        // ZKException
        if (!LedgerManagerTestUtils.isZkBasedLedgerManager(ledgerManager)) {
            return;
        }
        LOG.info("Test testWriteLedgerMetadata: stop zk cluster and expect delete ledger metadata response");
        stopZKCluster();
        rc = LedgerManagerTestUtils.writeLedgerMetadata(ledgerManager, nLedgerId, newMeta);
        assertEquals("Expect ZKException since zk is stopped : ", BKException.Code.ZKException, rc);
    }

    @Test
    public void testAsyncProcessLedgersWithNoLedger() throws Exception {
        final int numLedgers = 10;
        asyncProcessLedgers(numLedgers, numLedgers);
    }

    @Test
    public void testAsyncProcessLedgers() throws Exception {
        final int numLedgers = 100;
        Random random = new Random();
        int numDeleted = random.nextInt(numLedgers) + 1;
        asyncProcessLedgers(numLedgers, numDeleted);
    }

    public void asyncProcessLedgers(int numLedgers, int numDeleted) throws Exception {
        final LedgerManager ledgerManager = getLedgerManager();
        final LedgerMetadata metadata = new LedgerMetadata(3, 2, 1, BookKeeper.DigestType.MAC, new byte[0]);

        // Step 1: create some ledgers
        List<Long> allLedgers = new LinkedList<Long>();
        for (long ledgerId = 0; ledgerId < numLedgers; ledgerId++) {
            int rc = LedgerManagerTestUtils.createLedgerMetadata(ledgerManager, ledgerId, metadata);
            assertEquals("Failed to create ledger metadata : ", BKException.Code.OK, rc);
            allLedgers.add(ledgerId);
        }

        // Step 2: randomly delete ledgers
        Collections.shuffle(allLedgers);
        for (int i = 0; i < numDeleted; i++) {
            int rc = LedgerManagerTestUtils.removeLedgerMetadata(ledgerManager, allLedgers.get(i));
            assertEquals("Delete ledger failed : ", BKException.Code.OK, rc);
        }
        final Set<Long> activeLedgers = Collections.synchronizedSet(new TreeSet<Long>(allLedgers.subList(
                numDeleted, numLedgers)));

        // Step 3: get all ledger list and check
        final int successRc = Code.OK;
        final int failureRc = Code.ZKException;
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final CallbackResult result = new CallbackResult();
        ledgerManager.asyncProcessLedgers(new Processor<Long>() {
            @Override
            public void process(Long ledgerId, VoidCallback cb) {
                if (activeLedgers.contains(ledgerId)) {
                    LOG.debug("Async process ledger {}.", ledgerId);
                    activeLedgers.remove(ledgerId);
                    cb.processResult(successRc, null, null);
                } else {
                    LOG.error("Unexpected ledger {} exist.", ledgerId);
                    cb.processResult(failureRc, null, null);
                }
            }
        }, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                result.returnCode = rc;
                countDownLatch.countDown();
            }
        }, null, successRc, failureRc);

        assertTrue("Wait asyncProcessLedgers callback timeout : ", countDownLatch.await(10, TimeUnit.SECONDS));
        LOG.debug("Unprocessed ledgers: {}", activeLedgers);
        assertEquals("Some ledgers are processed failed : ", successRc, result.returnCode);
        assertTrue("Not all ledgers are processed : ", activeLedgers.isEmpty());
    }

}
