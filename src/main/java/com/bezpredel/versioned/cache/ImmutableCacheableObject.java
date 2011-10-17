package com.bezpredel.versioned.cache;

import com.bezpredel.versioned.datastore.Keyed;

public interface ImmutableCacheableObject<T extends CacheIdentifierType> extends Keyed {
    Object getKey();

    /**
     * @return the cache type of this object.
     */
    T getCacheType();
    void stored();
}
