package com.bezpredel.versioned.cache.replication;

import com.bezpredel.versioned.cache.SingleCacheService;
import com.bezpredel.versioned.cache.CacheServiceInitializer;
import com.bezpredel.versioned.cache.Caches;
import com.bezpredel.versioned.cache.UpdateDescriptor;

public class SlaveCacheService extends SingleCacheService {
    public SlaveCacheService(CacheServiceInitializer cacheServiceInitializer) {
        super(cacheServiceInitializer);
    }

    public void replay(final UpdateDescriptor updateDescriptor) {
        super.executeWrite(new WriteCommand() {
            public void execute(WriteContext context) {
                updateDescriptor.applyTo(context);
            }
        });
    }

    public void clear() {
        super.executeWrite(new ClearAllCachesCommand());
    }

    @Override
    public void executeWrite(WriteCommand command) {
        throw new UnsupportedOperationException("Direct write is not supported by this class");
    }

    public String toString() {
        return "SlaveCacheService#" + getName() + "#" + System.identityHashCode(this);
    }


    private static class ClearAllCachesCommand implements SingleCacheService.WriteCommand {
        public void execute(SingleCacheService.WriteContext context) {
            Caches.clearAll(context);
        }
    }
}
