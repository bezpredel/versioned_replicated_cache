package com.bezpredel.versioned.cache;

public interface CacheIdentifierType {
    Class<? extends ImmutableCacheableObject> getCacheObjectType();
}
