package com.bezpredel.versioned.cache.queries;

import com.bezpredel.versioned.cache.CacheIdentifierType;

public interface AbstractItemQuery<T extends CacheIdentifierType> {
    T getType();
}
