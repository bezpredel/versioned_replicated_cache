package com.bezpredel.versioned.cache.queries;

import com.bezpredel.versioned.cache.CacheIdentifierType;

public interface AbstractCollectionQuery<T extends CacheIdentifierType> {
    T getType();
}
