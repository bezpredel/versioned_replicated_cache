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
                throw new StaticIndexMappingChangedException(this, oldValue, newValue);
            }
        }

        if(shouldOldLeafBeCleanedUp(oldLeafKey, newLeafKey)) {
            removeFromIndex(context, oldLeafKey, oldValue);
        }

        if (newLeafKey != null) {
            addToIndex(context, newLeafKey, newValue);
        }
    }

    protected abstract void addToIndex(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, Object newLeafKey, ImmutableCacheableObject newValue);

    protected abstract void removeFromIndex(StorageSystem.WriteContext<OneToOneID, OneToManyID> context, Object oldLeafKey, ImmutableCacheableObject oldValue);

    private boolean shouldOldLeafBeCleanedUp(Object oldKey, Object newKey) {
        return oldKey != null && !oldKey.equals(newKey);
    }

    protected Object toKey(ImmutableCacheableObject oldValue) {
        if(oldValue==null) {
            return null;
        } else {
            Object key = mapFunction.apply(oldValue);

            if(supportNullKeys) {
                return KeyDecorator.wrap(key);
            } else {
                return key;
            }
        }
    }

    public static class StaticIndexMappingChangedException extends RuntimeException {
        private static final long serialVersionUID = -3645393760004357313L;

        private final AbstractIndex index;
        private final ImmutableCacheableObject oldValue;
        private final ImmutableCacheableObject newValue;

        public StaticIndexMappingChangedException(AbstractIndex index, ImmutableCacheableObject oldValue, ImmutableCacheableObject newValue) {
            this.index = index;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }
    }
}
