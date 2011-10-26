package com.bezpredel.versioned.cache;


public class Caches {
    public static void clearAll(SingleCacheService.WriteContext writeContext) {
        for(BasicCacheIdentifier identifier : writeContext.getAllCacheNames()) {
            writeContext.clear(identifier);
        }
    }
}
