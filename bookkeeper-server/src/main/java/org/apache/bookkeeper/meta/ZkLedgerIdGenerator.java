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

import java.io.IOException;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper based ledger id generator class, which using EPHEMERAL_SEQUENTIAL
 * with <i>(ledgerIdGenPath)/ID-</i> prefix to generate ledger id. Note
 * zookeeper sequential counter has a format of %10d -- that is 10 digits with 0
 * (zero) padding, i.e. "&lt;path&gt;0000000001", so ledger id space is
 * fundamentally limited to 9 billion.
 */
public class ZkLedgerIdGenerator implements LedgerIdGenerator {
    static final Logger LOG = LoggerFactory.getLogger(ZkLedgerIdGenerator.class);

    ZooKeeper zk;
    String ledgerPrefix;

    @Override
    public ZkLedgerIdGenerator initialize(AbstractConfiguration conf, ZooKeeper zk) throws IOException {
        String zkLedgerIdGenPath = conf.getZkLedgerIdGenPath();
        this.zk = zk;
        this.ledgerPrefix = zkLedgerIdGenPath + "/ID-";

        try {
            ZkUtils.createFullPathOptimistic(zk, zkLedgerIdGenPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException nee) {
            // ignore if node exist
        } catch (Exception e) {
            throw new IOException("Failed to create zk ledger id generatio path '" + zkLedgerIdGenPath
                    + "' : ", e);
        }

        return this;
    }

    @Override
    public void generateLedgerId(final GenericCallback<Long> cb) {
        ZkUtils.createFullPathOptimistic(zk, ledgerPrefix, new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL,
                new StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, final String idPathName) {
                        if (rc != KeeperException.Code.OK.intValue()) {
                            LOG.error("Could not generate new ledger id",
                                    KeeperException.create(KeeperException.Code.get(rc), path));
                            cb.operationComplete(BKException.Code.ZKException, null);
                            return;
                        }

                        /*
                         * Extract ledger id from generated path
                         */
                        long ledgerId;
                        try {
                            ledgerId = getLedgerIdFromGenPath(idPathName);
                            cb.operationComplete(BKException.Code.OK, ledgerId);
                        } catch (IOException e) {
                            LOG.error("Could not extract ledger-id from id gen path:" + path, e);
                            cb.operationComplete(BKException.Code.ZKException, null);
                            return;
                        }

                        // delete the znode for id generation
                        zk.delete(idPathName, -1, new AsyncCallback.VoidCallback() {
                            @Override
                            public void processResult(int rc, String path, Object ctx) {
                                if (rc != KeeperException.Code.OK.intValue()) {
                                    LOG.warn("Exception during deleting znode for id generation : ",
                                            KeeperException.create(KeeperException.Code.get(rc), path));
                                } else {
                                    LOG.debug("Deleting znode for id generation : {}", idPathName);
                                }
                            }
                        }, null);
                    }
                }, null);
    }

    // get ledger id from generation path
    private long getLedgerIdFromGenPath(String nodeName) throws IOException {
        long ledgerId;
        try {
            String parts[] = nodeName.split(ledgerPrefix);
            ledgerId = Long.parseLong(parts[parts.length - 1]);
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
        return ledgerId;
    }

    @Override
    public void close() throws IOException {
    }

}
