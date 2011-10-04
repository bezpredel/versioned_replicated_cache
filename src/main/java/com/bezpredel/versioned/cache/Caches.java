package com.bezpredel.versioned.cache;


public class Caches {
    public static void clearAll(CacheService.WriteContext writeContext) {
        for(BasicCacheIdentifier identifier : writeContext.getCacheService().getCacheNames()) {
            writeContext.clear(identifier);
        }
    }
}
