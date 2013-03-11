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
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

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
 * The ledger id space is divided into 32 slots, each slot maintains a counter
 * alone. The ledger id generator firstly fetch a ledger id range from slots
 * randomly, and then consume the ledger id batch locally.
 */
public class ZkBatchLedgerIdGenerator implements LedgerIdGenerator {
    static final Logger LOG = LoggerFactory.getLogger(ZkBatchLedgerIdGenerator.class);

    ZooKeeper zk;
    int batchSize;
    String zkSlotPrefix;

    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    ReadLock readLock = readWriteLock.readLock();
    WriteLock writeLock = readWriteLock.writeLock();

    int currentSlot = -1;
    long startID = 0;
    AtomicBoolean fetching = new AtomicBoolean(false);
    AtomicInteger counter = new AtomicInteger(0);

    ConcurrentLinkedQueue<GenericCallback<Long>> queue = new ConcurrentLinkedQueue<GenericCallback<Long>>();

    /**
     * Random number generator used to choose slot
     */
    private Random rand = new Random();

    // The number of slots to generate ledger id.
    // DO NOT CHANGE IT!!!
    final static int NUM_SLOTS = 32;

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

        return this;
    }

    @Override
    public void generateLedgerId(GenericCallback<Long> cb) {
        readLock.lock();
        int offset = counter.decrementAndGet();
        if (offset >= 0) {
            cb.operationComplete(KeeperException.Code.OK.intValue(),
                    getLedgerID(currentSlot, startID + offset));
        } else {
            queue.add(cb);
            if (fetching.compareAndSet(false, true)) {
                fetchLedgerIdBatch();
            }
        }
        readLock.unlock();
    }

    // Two possible ledger id format:
    //    1. (slot id)0...0(counter id)
    //    2. 0...0(counter id)(slot id)
    // We prefer 2, since it will keep ledger id not too large at the beginning.
    protected static long getLedgerID(int slot, long id) {
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
                        String s = new String(data, "UTF-8");
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
                    zk.setData(slotPath, Long.toString(newCounter).getBytes(), stat.getVersion(),
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
                    ZkUtils.createFullPathOptimistic(zk, slotPath, "0".getBytes(), Ids.OPEN_ACL_UNSAFE,
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
        writeLock.lock();
        int num = Math.min(batch, queue.size());
        currentSlot = slot;
        startID = start;
        counter.set(batch - num);
        fetching.set(false);
        writeLock.unlock();

        while (num-- > 0) {
            int offset = --batch;
            GenericCallback<Long> cb = queue.poll();
            cb.operationComplete(KeeperException.Code.OK.intValue(), getLedgerID(slot, start + offset));
        }

        if (!queue.isEmpty() && fetching.compareAndSet(false, true)) {
            fetchLedgerIdBatch();
        }
    }

}
