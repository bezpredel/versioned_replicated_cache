package com.bezpredel.versioned.cache;

public interface IndexIdentifierType<C extends CacheIdentifierType> {
    C getCacheType();
    boolean isOneToMany();
}
