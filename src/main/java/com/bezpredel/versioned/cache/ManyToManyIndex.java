package com.bezpredel.versioned.cache;

import com.bezpredel.versioned.datastore.StorageSystem;
import com.google.common.base.Function;

import java.util.Collection;

public class ManyToManyIndex {
    private final static Object[] EMPTY_ARRAY = new Object[0];
    private final Function<ImmutableCacheableObject, Object[]> mapFunction;
    private final boolean staticMapping;
    private final BasicManyToManyIndexIdentifier identifier;

    public ManyToManyIndex(BasicManyToManyIndexIdentifier identifier, Function<ImmutableCacheableObject, Object[]> mapFunction, boolean supportNullKeys, boolean staticMapping) {
        this.mapFunction = mapFunction;
        this.staticMapping = staticMapping;
        this.identifier = identifier;
    }

    public void update(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, ImmutableCacheableObject oldValue, ImmutableCacheableObject newValue) {
        Object[] oldLeafKeys = toKey(oldValue);
        Object[] newLeafKeys = toKey(newValue);

        if (staticMapping && oldValue != null && newValue != null) {
            if (oldLeafKeys != newLeafKeys) {
                throw new AbstractIndex.StaticIndexMappingChangedException(identifier, oldValue, newValue);
            }
        }

        if (oldLeafKeys != newLeafKeys) {
            for (Object oldLeafKey : oldLeafKeys) {
                if (oldLeafKey != null) {
                    context.removeFromIndex(identifier.getDataStoreId(), oldLeafKey, oldValue.getKey());
                }
            }
        }

        for(Object newLeafKey : newLeafKeys) {
            if (newLeafKey != null) {
                context.addToIndex(identifier.getDataStoreId(), newLeafKey, newValue);
            }
        }
    }

    protected Object[] toKey(ImmutableCacheableObject value) {
        if(value==null) {
            return EMPTY_ARRAY;
        } else {
            return mapFunction.apply(value);
        }
    }

    protected AbstractIndexIdentifier getIdentifier() {
        return identifier;
    }
}
