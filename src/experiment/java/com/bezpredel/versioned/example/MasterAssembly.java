package com.bezpredel.versioned.example;

import com.bezpredel.utils.MultiDistributor;
import com.bezpredel.versioned.cache.SingleCacheService;
import com.bezpredel.versioned.cache.CacheServiceInitializer;
import com.bezpredel.versioned.cache.UpdateDescriptor;
import com.bezpredel.versioned.cache.replication.MasterCacheSyncServer;

/**
 * Date: 10/4/11
 * Time: 3:09 PM
 */
public class MasterAssembly {
    public final SingleCacheService cacheService;
    public final MultiDistributor<UpdateDescriptor> updateDistributor;
    public final MasterCacheSyncServer syncServer;

    public MasterAssembly(CacheServiceInitializer initializer) {
        cacheService = new SingleCacheService(initializer);
        updateDistributor = new MultiDistributor<UpdateDescriptor>();
        syncServer = new MasterCacheSyncServer(cacheService, updateDistributor);
    }
}
