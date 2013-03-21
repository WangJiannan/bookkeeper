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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.versioning.Version;
import org.junit.Assert;

public class LedgerManagerTestUtils {
    final static int timeout = 10;

    public static class CallbackResult {
        int returnCode = 0;
        LedgerMetadata metadata = null;
    }

    public static int createLedgerMetadata(LedgerManager ledgerManager, long ledgerId, LedgerMetadata metadata)
            throws InterruptedException {
        final CallbackResult cr = new CallbackResult();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ledgerManager.createLedgerMetadata(ledgerId, metadata, new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                cr.returnCode = rc;
                countDownLatch.countDown();
            }
        });
        Assert.assertTrue("Wait create ledger callback timeout : ",
                countDownLatch.await(timeout, TimeUnit.SECONDS));
        return cr.returnCode;
    }

    public static int removeLedgerMetadata(LedgerManager ledgerManager, long ledgerId) throws InterruptedException {
        final CallbackResult cr = new CallbackResult();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ledgerManager.removeLedgerMetadata(ledgerId, Version.ANY, new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                cr.returnCode = rc;
                countDownLatch.countDown();
            }
        });

        Assert.assertTrue("Wait delete ledger callback timeout : ",
                countDownLatch.await(timeout, TimeUnit.SECONDS));
        return cr.returnCode;
    }

    public static CallbackResult readLedgerMetadata(LedgerManager ledgerManager, long ledgerId)
            throws InterruptedException {
        final CallbackResult cr = new CallbackResult();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ledgerManager.readLedgerMetadata(ledgerId, new GenericCallback<LedgerMetadata>() {
            @Override
            public void operationComplete(int rc, LedgerMetadata result) {
                cr.returnCode = rc;
                cr.metadata = result;
                countDownLatch.countDown();
            }
        });

        Assert.assertTrue("Wait read ledger callback timeout : ",
                countDownLatch.await(timeout, TimeUnit.SECONDS));
        return cr;
    }

    public static int writeLedgerMetadata(LedgerManager ledgerManager, long ledgerId, LedgerMetadata metadata)
            throws InterruptedException {
        final CallbackResult cr = new CallbackResult();

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ledgerManager.writeLedgerMetadata(ledgerId, metadata, new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                cr.returnCode = rc;
                countDownLatch.countDown();
            }
        });

        Assert.assertTrue("Wait write ledger metadata callback timeout : ",
                countDownLatch.await(timeout, TimeUnit.SECONDS));
        return cr.returnCode;
    }

    public static boolean isZkBasedLedgerManager(LedgerManager ledgerManager) {
        return ledgerManager instanceof AbstractZkLedgerManager;
    }

}
