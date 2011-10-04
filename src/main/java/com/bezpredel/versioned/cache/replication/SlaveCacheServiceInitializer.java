package com.bezpredel.versioned.cache.replication;

import com.bezpredel.versioned.cache.CacheServiceInitializer;
import com.bezpredel.versioned.cache.OneToManyID;
import com.bezpredel.versioned.cache.OneToOneID;
import com.bezpredel.versioned.datastore.StorageSystem;

public class SlaveCacheServiceInitializer extends CacheServiceInitializer {
    @Override
    public StorageSystem<OneToOneID, OneToManyID> initializeVirtualWriter(StorageSystem<OneToOneID, OneToManyID> storage) {
        return storage;
    }
}
