package com.bezpredel.versioned.cache;

import com.bezpredel.versioned.datastore.StorageSystem;
import com.google.common.base.Function;

public abstract class AbstractIndex {
    private final Function<ImmutableCacheableObject, Object> mapFunction;
    private final boolean supportNullKeys;
    private final boolean staticMapping;

    public AbstractIndex(Function<ImmutableCacheableObject, Object> mapFunction, boolean supportNullKeys, boolean staticMapping) {
        this.mapFunction = mapFunction;
        this.supportNullKeys = supportNullKeys;
        this.staticMapping = staticMapping;
    }

    public void update(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, ImmutableCacheableObject oldValue, ImmutableCacheableObject newValue) {
        Object oldLeafKey = toKey(oldValue);
        Object newLeafKey = toKey(newValue);

        if (staticMapping && oldValue != null && newValue != null) {
            if (oldLeafKey != newLeafKey && (oldLeafKey == null || !oldLeafKey.equals(newLeafKey))) {
                throw new StaticIndexMappingChangedException(getIdentifier(), oldValue, newValue);
            }
        }

        if(oldLeafKey != null && !oldLeafKey.equals(newLeafKey)) {
            removeFromIndex(context, oldLeafKey, oldValue);
        } else {
            // the next call will just replace
        }

        if (newLeafKey != null) {
            addToIndex(context, newLeafKey, newValue);
        }
    }

    protected abstract void addToIndex(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, Object newLeafKey, ImmutableCacheableObject newValue);

    protected abstract void removeFromIndex(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, Object oldLeafKey, ImmutableCacheableObject oldValue);


    protected Object toKey(ImmutableCacheableObject value) {
        if(value==null) {
            return null;
        } else {
            Object key = mapFunction.apply(value);

            if(supportNullKeys) {
                return KeyDecorator.wrap(key);
            } else {
                return key;
            }
        }
    }

    protected abstract AbstractIndexIdentifier getIdentifier();

    public static class StaticIndexMappingChangedException extends RuntimeException {
        private static final long serialVersionUID = -3645393760004357313L;

        private final AbstractIndexIdentifier index;
        private final ImmutableCacheableObject oldValue;
        private final ImmutableCacheableObject newValue;

        public StaticIndexMappingChangedException(AbstractIndexIdentifier index, ImmutableCacheableObject oldValue, ImmutableCacheableObject newValue) {
            this.index = index;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }
    }
}
