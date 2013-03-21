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

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManagerTestUtils.CallbackResult;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

public class TestZkRadixTreeLedgerManager extends BookKeeperClusterTestCase {
    LedgerManagerFactory ledgerManagerFactory;
    LedgerManager ledgerManager;

    public TestZkRadixTreeLedgerManager() {
        super(0);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseConf.setLedgerManagerFactoryClass(ZkRadixTreeLedgerManagerFactory.class);
        ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(baseConf, zkc);
        ledgerManager = ledgerManagerFactory.newLedgerManager();
    }

    @Override
    public void tearDown() throws Exception {
        ledgerManager.close();
        ledgerManagerFactory.uninitialize();
        super.tearDown();
    }

    @Test
    public void testCreateWithMultiNode() throws Exception {
        final long radix = ZkRadixTreeLedgerManager.RADIX;
        createLedgerMetadata(((radix * 1 + 2) * radix + 3) * radix + 4);
    }

    @Test
    public void testDeleteInnerNode() throws Exception {
        final LedgerMetadata metadata = new LedgerMetadata(3, 2, 1, BookKeeper.DigestType.MAC, new byte[0]);
        final long radix = ZkRadixTreeLedgerManager.RADIX;

        final long[] ledgers = new long[] {
                radix * 0 + 2,
                (radix * 0 + 2) * radix + 5,
                ((radix * 0 + 2) * radix + 5) * radix + 5,
        };

        // Step 1: create ledgers
        for (int i = 0; i < ledgers.length; i++) {
            createLedgerMetadata(ledgers[i]);
        }

        // Step 2: delete inner node ledgers
        for (int i = 0; i < ledgers.length - 1; i++) {
            int rc = LedgerManagerTestUtils.removeLedgerMetadata(ledgerManager, ledgers[i]);
            assertEquals("Failed to delete ledger metadata : ", BKException.Code.OK, rc);
        }

        // Step 3: check
        for (int i = 0; i < ledgers.length - 1; i++) {
            CallbackResult result = LedgerManagerTestUtils.readLedgerMetadata(ledgerManager, ledgers[i]);
            assertEquals("Ledger is not deleted : ", BKException.Code.NoSuchLedgerExistsException,
                    result.returnCode);
        }

        CallbackResult result = LedgerManagerTestUtils.readLedgerMetadata(ledgerManager,
                ledgers[ledgers.length - 1]);
        assertEquals("Read ledger metadata failed : ", BKException.Code.OK, result.returnCode);
        assertEquals("Write and read metadata does not match : ", new String(metadata.serialize()),
                new String(result.metadata.serialize()));
    }

    protected void createLedgerMetadata(long ledgerId) throws Exception {
        final LedgerMetadata metadata = new LedgerMetadata(3, 2, 1, BookKeeper.DigestType.MAC, new byte[0]);

        int rc = LedgerManagerTestUtils.createLedgerMetadata(ledgerManager, ledgerId, metadata);
        assertEquals("Failed to create ledger metadata : ", BKException.Code.OK, rc);

        CallbackResult result = LedgerManagerTestUtils.readLedgerMetadata(ledgerManager, ledgerId);
        assertEquals("Read ledger metadata failed : ", BKException.Code.OK, result.returnCode);
        assertEquals("Write and read metadata does not match : ", new String(metadata.serialize()),
                new String(result.metadata.serialize()));
    }

}
