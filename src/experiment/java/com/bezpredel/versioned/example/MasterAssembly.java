package com.bezpredel.versioned.example;

import com.bezpredel.utils.MultiDistributor;
import com.bezpredel.versioned.cache.CacheService;
import com.bezpredel.versioned.cache.CacheServiceInitializer;
import com.bezpredel.versioned.cache.UpdateDescriptor;
import com.bezpredel.versioned.cache.replication.MasterCacheSyncServer;

/**
 * Date: 10/4/11
 * Time: 3:09 PM
 */
public class MasterAssembly {
    public final CacheService cacheService;
    public final MultiDistributor<UpdateDescriptor> updateDistributor;
    public final MasterCacheSyncServer syncServer;

    public MasterAssembly(CacheServiceInitializer initializer) {
        cacheService = new CacheService(initializer);
        updateDistributor = new MultiDistributor<UpdateDescriptor>();
        syncServer = new MasterCacheSyncServer(cacheService, updateDistributor);
    }
}
