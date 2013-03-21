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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkRadixTreeLedgerManagerFactory extends LedgerManagerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ZkRadixTreeLedgerManagerFactory.class);

    public static final int CUR_VERSION = 1;

    AbstractConfiguration conf;
    ZooKeeper zk;
    ScheduledExecutorService scheduler;

    @Override
    public int getCurrentVersion() {
        return CUR_VERSION;
    }

    @Override
    public LedgerManagerFactory initialize(AbstractConfiguration conf, ZooKeeper zk, int factoryVersion)
            throws IOException {
        if (CUR_VERSION != factoryVersion) {
            throw new IOException("Incompatible layout version found : " + factoryVersion);
        }
        this.conf = conf;
        this.zk = zk;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        return this;
    }

    @Override
    public void uninitialize() throws IOException {
        LOG.info("uninitialize ZkRadixTreeLedgerManagerFactory...");
        try {
            scheduler.shutdown();
            scheduler = null;
        } catch (Exception e) {
            LOG.warn("Error when uninitialize ZkRadixTreeLedgerManagerFactory : ", e);
        }
    }

    @Override
    public LedgerManager newLedgerManager() {
        return new ZkRadixTreeLedgerManager(conf, zk, scheduler);
    }

    @Override
    public LedgerUnderreplicationManager newLedgerUnderreplicationManager() throws KeeperException,
            InterruptedException, CompatibilityException {
        // TODO check compatibility
        return new ZkLedgerUnderreplicationManager(conf, zk);
    }

}
