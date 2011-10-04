package com.bezpredel.versioned.cache.queries;

import com.bezpredel.versioned.cache.CacheIdentifierType;

import java.util.Set;

public interface AbstractMultiQuery<T extends CacheIdentifierType> {
    Set<T> getTypes();
}
