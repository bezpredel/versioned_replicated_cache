package com.bezpredel.versioned.cache;

import com.bezpredel.versioned.datastore.Keyed;

public interface ImmutableCacheableObject<T extends CacheIdentifierType> extends Keyed{
    Object getKey();
    T getCacheType();
    void stored();
}
