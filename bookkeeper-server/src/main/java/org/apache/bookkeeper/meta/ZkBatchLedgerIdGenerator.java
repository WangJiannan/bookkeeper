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

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper based ledger id generator class with batch generation improvement.
 *
 * The ledger id space is divided into many slots (to reduce competition), each
 * slot maintains a counter alone. The ledger id generator firstly fetch a
 * ledger id range from slots randomly, and then consume the ledger id batch
 * locally.
 */
public class ZkBatchLedgerIdGenerator extends Thread implements LedgerIdGenerator {
    static final Logger LOG = LoggerFactory.getLogger(ZkBatchLedgerIdGenerator.class);

    ZooKeeper zk;
    int batchSize;
    String zkSlotPrefix;

    int currentSlot = -1;
    long startID = 0;
    long endID = 0;
    volatile boolean keepRunning = true;
    CountDownLatch countDownLatch;

    BlockingQueue<GenericCallback<Long>> queue = new LinkedBlockingQueue<GenericCallback<Long>>();

    /**
     * Random number generator used to choose slot
     */
    private Random rand = new Random();

    // The number of slots to generate ledger id.
    // DO NOT CHANGE IT!!!
    final static int NUM_SLOTS = (1<<13);

    @Override
    public LedgerIdGenerator initialize(AbstractConfiguration conf, ZooKeeper zk) throws IOException {
        String IdGenSlots = conf.getZkLedgersRootPath() + "/IdGenSlots";
        this.zk = zk;
        this.batchSize = conf.getLedgerIdBatchSize();
        this.zkSlotPrefix = IdGenSlots + "/";

        try {
            ZkUtils.createFullPathOptimistic(zk, IdGenSlots, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException nee) {
            // ignore if node exist
        } catch (Exception e) {
            throw new IOException("Failed to create zk ledger id generatio path '" + IdGenSlots + "' : ", e);
        }

        start();
        LOG.info("Start ledger id generation thread");
        return this;
    }

    @Override
    public void generateLedgerId(GenericCallback<Long> cb) {
        if (keepRunning) {
            queue.add(cb);
        }
    }

    @Override
    public void run() {
        while (keepRunning) {
            GenericCallback<Long> cb = null;
            try {
                cb = queue.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (cb == null) {
                continue;
            }

            if (startID >= endID) {
                countDownLatch = new CountDownLatch(1);
                fetchLedgerIdBatch();
                while (keepRunning && startID >= endID) {
                    try {
                        countDownLatch.await(1, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                    }
                }
                if (!keepRunning) {
                    return;
                }
                countDownLatch = null;
            }

            cb.operationComplete(KeeperException.Code.OK.intValue(), getLedgerID(currentSlot, startID));
            startID++;
        }
    }

    // Two possible ledger id format:
    //    1. (slot id)0...0(counter id)
    //    2. 0...0(counter id)(slot id)
    // We prefer 2, since it will keep ledger id not too large at the beginning.
    protected final static long getLedgerID(int slot, long id) {
        return id * NUM_SLOTS + slot;
    }

    protected void fetchLedgerIdBatch() {
        // randomly choose a slot
        int slot = rand.nextInt(NUM_SLOTS);
        fetchLedgerIdBatch(slot);
    }

    protected void fetchLedgerIdBatch(final int slot) {
        String slotPath = zkSlotPrefix + slot;
        fetchLedgerIdBatch(slot, slotPath);
    }

    protected void fetchLedgerIdBatch(final int slot, final String slotPath) {
        // Get counter
        zk.getData(slotPath, false, new DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (Code.OK.intValue() == rc) {
                    // Try to fetch batch ledger id
                    final long currentCounter;
                    try {
                        String s = new String(data, UTF_8);
                        currentCounter = Long.parseLong(s);
                    } catch (Exception e) {
                        LOG.error("Parse counter data '{}' in slot {} failed: {}", new Object[] { data, slot,
                                e });
                        fetchLedgerIdBatch(); // retry in other slot
                        return;
                    }

                    // acquire ledger id range with batch size plus current
                    // queue size
                    final int batch = batchSize + queue.size();
                    final long newCounter = currentCounter + batch;
                    zk.setData(slotPath, Long.toString(newCounter).getBytes(UTF_8), stat.getVersion(),
                            new StatCallback() {
                                @Override
                                public void processResult(int rc, String path, Object ctx, Stat stat) {
                                    if (Code.OK.intValue() == rc) {
                                        finishFetchBatchID(slot, currentCounter, batch);
                                    } else if (Code.BADVERSION.intValue() == rc) {
                                        // some other guy just update the
                                        // counter, try again
                                        fetchLedgerIdBatch(slot, slotPath);
                                    } else {
                                        LOG.error(
                                                "Failed to update counter, slot {}, counter {}, new counter {}, : {}",
                                                new Object[] { slot, currentCounter, newCounter,
                                                        KeeperException.create(Code.get(rc)) });
                                        fetchLedgerIdBatch(); // retry in other
                                                              // slot
                                    }
                                }
                            }, null);
                } else if (Code.NONODE.intValue() == rc) {
                    // slot znode does not exist, try to create it
                    LOG.info("Try to create znode '{}' for slot {}.", slotPath, slot);
                    ZkUtils.createFullPathOptimistic(zk, slotPath, "0".getBytes(UTF_8), Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
                                @Override
                                public void processResult(int rc, String path, Object ctx, String name) {
                                    if (Code.OK.intValue() == rc || Code.NODEEXISTS.intValue() == rc) {
                                        // retry to fetch ledger batch after
                                        // path is created
                                        fetchLedgerIdBatch(slot, slotPath);
                                    } else {
                                        LOG.warn(
                                                "Failed to create counter for slot {}, path '{}' : {}",
                                                new Object[] { slot, path,
                                                        KeeperException.create(Code.get(rc)) });
                                        fetchLedgerIdBatch(); // retry in other
                                                              // slot
                                    }
                                }
                            }, null);
                } else {
                    LOG.error("Get counter data in slot {} failed, path '{}' : {}", new Object[] { slot,
                            path, KeeperException.create(Code.get(rc)) });
                    fetchLedgerIdBatch(); // retry in other slot
                }
            }
        }, null);
    }

    protected void finishFetchBatchID(int slot, long start, int batch) {
        currentSlot = slot;
        startID = start;
        endID = start + batch;
        if (countDownLatch != null) {
            countDownLatch.countDown();
        }
    }

    @Override
    public void close() throws IOException {
        LOG.info("Shutdown ledger id generation thread");
        keepRunning = false;
        try {
            this.join();
        } catch (InterruptedException e) {
            throw new IOException("Catch InterruptedException when waiting thread to shutdown : ", e);
        }
    }

}
