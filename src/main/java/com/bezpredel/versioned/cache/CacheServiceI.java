package com.bezpredel.versioned.cache;

import com.bezpredel.collections.PseudoEnum;
import com.bezpredel.versioned.datastore.StorageSystem;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public interface CacheServiceI {
    Set<BasicCacheIdentifier> getCacheNames();
    Set<IndexIdentifierType<BasicCacheIdentifier>> getSupportedIndexes();

    String getName();

    public interface ReadContext {
        <T extends ImmutableCacheableObject<BasicCacheIdentifier>> T get(BasicCacheIdentifier cache, Object key);
        <T extends ImmutableCacheableObject<BasicCacheIdentifier>> Iterator<T> values(BasicCacheIdentifier cache);
        <T extends ImmutableCacheableObject<BasicCacheIdentifier>> T getByIndex(BasicOneToOneIndexIdentifier cache, Object leafKey);
        <T extends ImmutableCacheableObject<BasicCacheIdentifier>> Iterator<T> valuesByIndex(BasicOneToManyIndexIdentifier cache, Object leafKey);

        int getVersion();
    }

    public interface WriteContext extends ReadContext {
        <T extends ImmutableCacheableObject<BasicCacheIdentifier>> T put(T object);
        <T extends ImmutableCacheableObject<BasicCacheIdentifier>> T remove(T object);
        <T extends ImmutableCacheableObject<BasicCacheIdentifier>> T remove(BasicCacheIdentifier cache, Object key);
        int clear(BasicCacheIdentifier cache);

        Collection<BasicCacheIdentifier> getAllCacheNames();
    }

    public interface WriteCommand {
        void execute(WriteContext context);
    }

    public interface ReadCommand<R> {
        R execute(ReadContext context);
    }

    public static final class CSID extends PseudoEnum {
        private final CacheServiceI cacheService;
        public CSID(CacheServiceI cacheService) {
            this.cacheService = cacheService;
        }

        @Override
        public String toString() {
            return "CSID#" + cacheService.toString();
        }
    }
}
