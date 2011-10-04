package com.bezpredel.versioned.cache;

import com.bezpredel.collections.SerializablePseudoEnum;

public final class BasicCacheIdentifier extends SerializablePseudoEnum implements CacheIdentifierType {
    private static final long serialVersionUID = -2109783981326552008L;
    private static final Integer classIdentity = 2;

    private final String cacheName;
    private final Class<? extends ImmutableCacheableObject<BasicCacheIdentifier>> clazz;
    private final OneToOneID dataStoreId;

    public BasicCacheIdentifier(String cacheName, Class<? extends ImmutableCacheableObject<BasicCacheIdentifier>> clazz) {
        super(cacheName);
        this.cacheName = cacheName;
        this.clazz = clazz;
        this.dataStoreId = new OneToOneID(this);
    }

    public Class<? extends ImmutableCacheableObject> getCacheObjectType() {
        return clazz;
    }

    public String getCacheName() {
        return cacheName;
    }

    @Override
    public String toString() {
        return cacheName;
    }

    protected OneToOneID getDataStoreId() {
        return dataStoreId;
    }

    @Override
    protected Object getClassIdentity() {
        return classIdentity;
    }
}
