package com.bezpredel.versioned.cache;

/**
 * Marker interface for a type that may belong to one (but only one) of several storage systems
 */
public interface PartitionedImmutableCacheableObject<T extends CacheIdentifierType> extends ImmutableCacheableObject<T> {
    /**
     * @return the cache type of this object, which can be one of several types, but always the same for a particular instance.
     */
    T getCacheType();

}
