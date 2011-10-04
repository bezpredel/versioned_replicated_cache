package com.bezpredel.versioned.cache;

import com.bezpredel.versioned.datastore.StorageSystem;
import com.google.common.base.Function;

public class OneToManyIndexImpl extends AbstractIndex {
    private final BasicOneToManyIndexIdentifier indexIdentifier;
    public OneToManyIndexImpl(BasicOneToManyIndexIdentifier indexIdentifier, Function<ImmutableCacheableObject, Object> mapFunction, boolean supportNullKeys, boolean isStaticMapping) {
        super(mapFunction, supportNullKeys, isStaticMapping);
        this.indexIdentifier = indexIdentifier;
    }

    @Override
    protected void addToIndex(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, Object newLeafKey, ImmutableCacheableObject newValue) {
        context.addToIndex(indexIdentifier.getDataStoreId(), newLeafKey, newValue);
    }

    @Override
    protected void removeFromIndex(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, Object oldLeafKey, ImmutableCacheableObject oldValue) {
        context.removeFromIndex(indexIdentifier.getDataStoreId(), oldLeafKey, oldValue.getKey());
    }
}
