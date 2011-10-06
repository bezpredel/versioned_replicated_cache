package com.bezpredel.versioned.example;

import com.bezpredel.versioned.cache.replication.SlaveCacheService;
import com.bezpredel.versioned.cache.replication.SlaveCacheServiceInitializer;
import com.bezpredel.versioned.cache.replication.SlaveCacheSyncClient;
import com.bezpredel.versioned.cache.replication.VMTransportStrategyImpl;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;

/**
 * Date: 10/4/11
 * Time: 3:14 PM
 */
public class SlaveAssembly {
    public final SlaveCacheService service;
    public final SlaveCacheSyncClient syncClient;

    public SlaveAssembly(SlaveCacheServiceInitializer serviceInitializer, MasterAssembly masterAssembly) {
        VMTransportStrategyImpl vmTransportStrategy = new VMTransportStrategyImpl(
            Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true).build()),
            masterAssembly.updateDistributor,
            masterAssembly.syncServer
        );

        service = new SlaveCacheService(serviceInitializer);

        syncClient = new SlaveCacheSyncClient(vmTransportStrategy, service);
    }
}
