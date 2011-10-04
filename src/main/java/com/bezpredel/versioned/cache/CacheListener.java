package com.bezpredel.versioned.cache;

public interface CacheListener {
    void objectReplaced(ImmutableCacheableObject<BasicCacheIdentifier> oldVersion, ImmutableCacheableObject<BasicCacheIdentifier> newVersion);
}
