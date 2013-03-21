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
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkRadixTreeLedgerManager extends AbstractZkLedgerManager {
    static final Logger LOG = LoggerFactory.getLogger(ZkRadixTreeLedgerManager.class);

    static final int RADIX_BIT = 13;
    static final int RADIX = (1 << RADIX_BIT);

    final String ledgerMetaPath;
    final String ledgerMetaPathPrefix;

    static final byte[] EMPTY_ZNODE_DATA = new byte[0];

    final ScheduledExecutorService scheduler;

    protected ZkRadixTreeLedgerManager(AbstractConfiguration conf, ZooKeeper zk,
            ScheduledExecutorService scheduler) {
        super(conf, zk);
        this.ledgerMetaPath = conf.getZkLedgerMetadataPath();
        this.ledgerMetaPathPrefix = this.ledgerMetaPath + "/";
        this.scheduler = scheduler;
    }

    @Override
    protected String getLedgerPath(long ledgerId) {
        boolean negative = (ledgerId < 0);
        if (negative) {
            ledgerId = -ledgerId;
        }

        int w[] = new int[(Long.SIZE + RADIX_BIT - 1) / RADIX_BIT];
        int n = 0;

        do {
            w[n++] = (int) (ledgerId & (RADIX - 1));
            ledgerId >>= RADIX_BIT;
        } while (ledgerId != 0);

        StringBuilder sb = new StringBuilder(ledgerMetaPath);
        while (n-- > 0) {
            sb.append(negative ? "/-" : "/").append(w[n]);
        }
        return sb.toString();
    }

    @Override
    protected long getLedgerId(String ledgerPath) throws IOException {
        if (!ledgerPath.startsWith(ledgerMetaPathPrefix)) {
            throw new IOException("It is not a valid ledger id path : " + ledgerPath);
        }

        String radixPath = ledgerPath.substring(ledgerMetaPathPrefix.length());
        String[] radixParts = radixPath.split("/");

        long ledgerId = 0;
        try {
            for (int i = 0; i < radixParts.length; i++) {
                ledgerId = ledgerId * RADIX + Long.parseLong(radixParts[i]);
            }
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }

        return ledgerId;
    }

    @Override
    public void readLedgerMetadata(final long ledgerId, final GenericCallback<LedgerMetadata> readCb) {
        zk.getData(getLedgerPath(ledgerId), false, new DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (rc == KeeperException.Code.NONODE.intValue()
                        || (rc == KeeperException.Code.OK.intValue() && data.length == 0)) {
                    if (LOG.isDebugEnabled()) {
                        if (rc != KeeperException.Code.OK.intValue()) {
                            LOG.debug("No such ledger: " + ledgerId,
                                    KeeperException.create(KeeperException.Code.get(rc)), path);
                        } else {
                            LOG.debug("No such ledger: {}", ledgerId);
                        }
                    }
                    readCb.operationComplete(BKException.Code.NoSuchLedgerExistsException, null);
                    return;
                }
                if (rc != KeeperException.Code.OK.intValue()) {
                    LOG.error("Could not read metadata for ledger: " + ledgerId,
                            KeeperException.create(KeeperException.Code.get(rc), path));
                    readCb.operationComplete(BKException.Code.ZKException, null);
                    return;
                }

                LedgerMetadata metadata;
                try {
                    metadata = LedgerMetadata.parseConfig(data, new ZkVersion(stat.getVersion()));
                } catch (IOException e) {
                    LOG.error("Could not parse ledger metadata for ledger: " + ledgerId, e);
                    readCb.operationComplete(BKException.Code.ZKException, null);
                    return;
                }
                readCb.operationComplete(BKException.Code.OK, metadata);
            }
        }, null);
    }

    @Override
    public void removeLedgerMetadata(final long ledgerId, final Version version, final GenericCallback<Void> cb) {
        int znodeVersion = -1;
        if (Version.NEW == version) {
            LOG.error("Request to delete ledger {} metadata with version set to the initial one", ledgerId);
            cb.operationComplete(BKException.Code.MetadataVersionException, (Void)null);
            return;
        } else if (Version.ANY != version) {
            if (!(version instanceof ZkVersion)) {
                LOG.info("Not an instance of ZKVersion: {}", ledgerId);
                cb.operationComplete(BKException.Code.MetadataVersionException, (Void)null);
                return;
            } else {
                znodeVersion = ((ZkVersion)version).getZnodeVersion();
            }
        }

        // It's quite simple if the ledger id znode is a leaf node, or else
        // zookeeper will response with NOTEMPTY which means the znode is an
        // inner node in radix tree. And for inner node, we just set its data to
        // empty.
        zk.delete(getLedgerPath(ledgerId), znodeVersion, new VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                int bkRc;
                if (rc == KeeperException.Code.NONODE.intValue()) {
                    LOG.warn("Ledger node does not exist in ZooKeeper: ledgerId={}", ledgerId);
                    bkRc = BKException.Code.NoSuchLedgerExistsException;
                } else if (rc == KeeperException.Code.OK.intValue()) {
                    bkRc = BKException.Code.OK;
                } else if (rc == KeeperException.Code.NOTEMPTY.intValue()) {
                    zk.setData(getLedgerPath(ledgerId), EMPTY_ZNODE_DATA, -1, new StatCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, Stat stat) {
                            int bkRc;
                            if (rc == KeeperException.Code.NONODE.intValue()) {
                                LOG.warn("Ledger node does not exist in ZooKeeper: ledgerId={}", ledgerId);
                                bkRc = BKException.Code.NoSuchLedgerExistsException;
                            } else if (rc == KeeperException.Code.OK.intValue()) {
                                bkRc = BKException.Code.OK;
                            } else {
                                bkRc = BKException.Code.ZKException;
                            }
                            cb.operationComplete(bkRc, null);
                        }
                    }, null);
                    return;
                } else {
                    bkRc = BKException.Code.ZKException;
                }
                cb.operationComplete(bkRc, (Void) null);
            }
        }, null);
    }

    @Override
    public void asyncProcessLedgers(Processor<Long> processor, VoidCallback finalCb, Object context,
            int successRc, int failureRc) {
        ProcessContext pc = new ProcessContext(processor, finalCb, context, successRc, failureRc, scheduler);
        asyncProcessLedgersInNode(ledgerMetaPath, new Stack<Iterator<String>>(), pc);
    }

    @Override
    public void close() {
        LOG.info("closing ZkRadixTreeLedgerManager");
        super.close();
    }

    static class ProcessContext implements AsyncCallback.VoidCallback {
        private final Processor<Long> processor;
        private final VoidCallback finalCb;
        private final Object context;

        private final int successRc;
        private final int failureRc;
        private final AtomicInteger result;

        AtomicInteger counter = new AtomicInteger();
        private final ScheduledExecutorService scheduler;

        public ProcessContext(Processor<Long> processor, VoidCallback finalCb, Object context, int successRc,
                int failureRc, ScheduledExecutorService scheduler) {
            this.processor = processor;
            this.finalCb = finalCb;
            this.context = context;
            this.successRc = successRc;
            this.failureRc = failureRc;
            this.result = new AtomicInteger(successRc);
            this.scheduler = scheduler;
        }

        public void process(final long ledgerId) {
            counter.incrementAndGet();
            scheduler.submit(new Runnable() {
                @Override
                public void run() {
                    processor.process(ledgerId, ProcessContext.this);
                }
            });
        }

        @Override
        public void processResult(int rc, String path, Object ctx) {
            if (successRc != rc) {
                result.compareAndSet(successRc, failureRc);
            }
            int left = counter.decrementAndGet();
            if (left == 0) {
                synchronized (counter) {
                    counter.notify();
                }
            }
        }

        public void failed() {
            processResult(failureRc, null, null);
        }

        public void finish() {
            synchronized (counter) {
                while (counter.get() > 0) {
                    try {
                        counter.wait();
                    } catch (InterruptedException e) {
                    }
                }
            }
            finalCb.processResult(result.get(), null, context);
        }
    }

    /**
     * Process ledgers in a node. Since the process does not require to traverse
     * all ledger id in order, so a simple DFS on the radix tree would be
     * enough.
     */
    protected void asyncProcessLedgersInNode(final String path, final Stack<Iterator<String>> levelStack,
            final ProcessContext pc) {
        // To improve the performance, Children2Callback is used which will
        // return children list as well as Stat of current path.
        getChildrenInNode(zk, path, new GenericCallback<TreeNode>() {
            @Override
            public void operationComplete(int rc, TreeNode node) {
                if (Code.OK.intValue() != rc) {
                    pc.failed();
                    backtrack(path, levelStack, pc);
                    return;
                }

                // a real inner ledger node
                if (node.stat.getDataLength() > 0 && !levelStack.isEmpty()) {
                    try {
                        long ledgerId = getLedgerId(path);
                        pc.process(ledgerId);
                    } catch (IOException e) {
                    }
                }

                List<String> children = node.children;
                if (children.isEmpty()) {
                    backtrack(path, levelStack, pc);
                } else {
                    Iterator<String> iterator = children.iterator();
                    levelStack.push(iterator);
                    // handle the first child recursively
                    String firstChildPath = path + "/" + iterator.next();
                    asyncProcessLedgersInNode(firstChildPath, levelStack, pc);
                }
            }
        });
    }

    protected void backtrack(final String path, final Stack<Iterator<String>> levelStack,
            final ProcessContext pc) {
        int idxPath = path.length();
        Iterator<String> iterator = null;

        while (!levelStack.empty()) {
            // step back to parent path
            while (idxPath > 0 && path.charAt(idxPath - 1) != '/') {
                --idxPath;
            }
            // find next children in parent node
            iterator = levelStack.peek();
            if (iterator.hasNext()) {
                break;
            } else {
                levelStack.pop();
                --idxPath;
            }
        }

        if (levelStack.empty()) {
            // Finish traversal of all ledgers.
            pc.finish();
        } else {
            // backtrack and handle next child in parent
            String nextChildPath = path.substring(0, idxPath) + iterator.next();
            asyncProcessLedgersInNode(nextChildPath, levelStack, pc);
        }
    }

    static class TreeNode {
        List<String> children;
        Stat stat;

        public TreeNode(List<String> children, Stat stat) {
            this.children = children;
            this.stat = stat;
        }
    }

    /**
     * Async get direct children under single node
     *
     * @param zk
     *            zookeeper client
     * @param node
     *            node path
     * @param cb
     *            callback function
     */
    public static void getChildrenInNode(final ZooKeeper zk, final String node,
            final GenericCallback<TreeNode> cb) {
        // Do we really need to sync for each sub node?
        zk.sync(node, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("ZK error syncing nodes when getting children: ",
                            KeeperException.create(KeeperException.Code.get(rc), path));
                    cb.operationComplete(rc, null);
                    return;
                }
                zk.getChildren(node, false, new AsyncCallback.Children2Callback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, List<String> nodes, Stat stat) {
                        if (rc != Code.OK.intValue()) {
                            LOG.error("Error polling ZK for the available nodes: ",
                                    KeeperException.create(KeeperException.Code.get(rc), path));
                            cb.operationComplete(rc, null);
                            return;
                        }

                        cb.operationComplete(rc, new TreeNode(nodes, stat));
                    }
                }, null);
            }
        }, null);
    }

}
