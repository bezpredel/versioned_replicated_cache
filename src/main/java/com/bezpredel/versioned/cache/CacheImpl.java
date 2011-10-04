package com.bezpredel.versioned.cache;


import com.bezpredel.versioned.datastore.StorageSystem;

import java.util.Collection;

public class CacheImpl {
    private final BasicCacheIdentifier cacheIdentifier;
    private final AbstractIndex[] indices;

    public CacheImpl(BasicCacheIdentifier cacheIdentifier, Collection<AbstractIndex> indices) {
        this.cacheIdentifier = cacheIdentifier;
        this.indices = indices.toArray(new AbstractIndex[indices.size()]);
    }



    public BasicCacheIdentifier getCacheIdentifier() {
        return cacheIdentifier;
    }

    public ImmutableCacheableObject<BasicCacheIdentifier> put(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, ImmutableCacheableObject<BasicCacheIdentifier> newVal) {
        ImmutableCacheableObject<BasicCacheIdentifier> oldVal = context.put(cacheIdentifier.getDataStoreId(), newVal);

        updateIndices(context, oldVal, newVal);

        newVal.stored();

        return oldVal;
    }

    public ImmutableCacheableObject<BasicCacheIdentifier> remove(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, Object key) {
        ImmutableCacheableObject<BasicCacheIdentifier> oldVal = context.remove(cacheIdentifier.getDataStoreId(), key);

        updateIndices(context, oldVal, null);

        return oldVal;
    }

    private void updateIndices(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, ImmutableCacheableObject<BasicCacheIdentifier> oldVal, ImmutableCacheableObject<BasicCacheIdentifier> newVal) {
        for(AbstractIndex index : indices) {
            index.update(context, oldVal, newVal);
        }
    }



}
