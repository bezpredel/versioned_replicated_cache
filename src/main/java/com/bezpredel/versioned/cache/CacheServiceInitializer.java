package com.bezpredel.versioned.cache;

import com.bezpredel.collections.PseudoEnumMap;
import com.bezpredel.versioned.cache.def.CacheSpec;
import com.bezpredel.versioned.cache.def.IndexSpec;
import com.bezpredel.versioned.datastore.StorageSystem;
import com.bezpredel.versioned.datastore.actual.StorageSystemImpl;
import com.bezpredel.versioned.datastore.virtual.VirtualWriteStorageSystem;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

public class CacheServiceInitializer {
    private Set<CacheSpec> cacheSpecs;
    private Object writeLock;
    private Executor unlockExecutor;
    private boolean unlockAsynchronously;
    private String name;

    public void setWriteLock(Object writeLock) {
        this.writeLock = writeLock;
    }

    public void setUnlockExecutor(Executor unlockExecutor) {
        this.unlockExecutor = unlockExecutor;
    }

    public void setCacheSpecs(Set<CacheSpec> cacheSpecs) {
        this.cacheSpecs = cacheSpecs;
    }

    public boolean isUnlockAsynchronously() {
        return unlockAsynchronously;
    }

    public void setUnlockAsynchronously(boolean unlockAsynchronously) {
        this.unlockAsynchronously = unlockAsynchronously;
    }

    public StorageSystem<OneToOneID, OneToManyID> initializeStorageSystem() {
        Set<OneToOneID> cacheNames = new HashSet<OneToOneID>();
        Set<OneToManyID> indexNames = new HashSet<OneToManyID>();

        for(CacheSpec cacheSpec : cacheSpecs) {
            cacheNames.add(cacheSpec.getType().getDataStoreId());
            for(IndexSpec indexSpec : cacheSpec.getIndices()) {
                if(indexSpec.isOneToMany()) {
                    indexNames.add(indexSpec.getBasicOneToManyIndexIdentifier().getDataStoreId());
                }else{
                    cacheNames.add(indexSpec.getBasicOneToOneIndexIdentifier().getDataStoreId());
                }

            }
        }

        return new StorageSystemImpl<OneToOneID, OneToManyID>(
            OneToOneID.class,
            OneToManyID.class,
            PseudoEnumMap.factory(),
            cacheNames,
            indexNames,
            writeLock,
            unlockExecutor
        );
    }

    public StorageSystem<OneToOneID, OneToManyID> initializeVirtualWriter(StorageSystem<OneToOneID, OneToManyID> storage) {
        return new VirtualWriteStorageSystem<OneToOneID, OneToManyID>(storage, writeLock);
    }


    public Map<OneToOneID, CacheImpl> initializeCaches() {
        Map<OneToOneID, CacheImpl> map = PseudoEnumMap.factory().createMap(OneToOneID.class);

        for(CacheSpec cacheSpec : cacheSpecs) {
            Set<AbstractIndex> indices = new HashSet<AbstractIndex>();
            for(IndexSpec indexSpec : cacheSpec.getIndices()) {
                if(indexSpec.isOneToMany()) {
                    indices.add(new OneToManyIndexImpl(indexSpec.getBasicOneToManyIndexIdentifier(), indexSpec.getMapFunction(), indexSpec.isSupportNullKeys(), indexSpec.isStaticMapping()));
                }else{
                    indices.add(new OneToOneIndexImpl(indexSpec.getBasicOneToOneIndexIdentifier(), indexSpec.getMapFunction(), indexSpec.isSupportNullKeys(), indexSpec.isStaticMapping()));
                }
            }

            map.put(cacheSpec.getType().getDataStoreId(), new CacheImpl(cacheSpec.getType(), indices));
        }

        return map;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
