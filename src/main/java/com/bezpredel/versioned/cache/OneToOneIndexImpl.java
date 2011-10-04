package com.bezpredel.versioned.cache;

import com.bezpredel.versioned.datastore.StorageSystem;
import com.google.common.base.Function;


public class OneToOneIndexImpl extends AbstractIndex {
    protected final BasicOneToOneIndexIdentifier indexIdentifier;
    public OneToOneIndexImpl(BasicOneToOneIndexIdentifier indexIdentifier, Function<ImmutableCacheableObject, Object> mapFunction, boolean supportNullKeys, boolean isStaticMapping) {
        super(mapFunction, supportNullKeys, isStaticMapping);
        this.indexIdentifier = indexIdentifier;
    }

    @Override
    protected void addToIndex(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, Object newLeafKey, ImmutableCacheableObject newValue) {
        context.put(indexIdentifier.getDataStoreId(), new KeyDecorator(newLeafKey, newValue));
    }

    @Override
    protected void removeFromIndex(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, Object oldLeafKey, ImmutableCacheableObject oldValue) {
        context.remove(indexIdentifier.getDataStoreId(), oldLeafKey);
    }

    @Override
    protected AbstractIndexIdentifier getIdentifier() {
        return indexIdentifier;
    }
}
