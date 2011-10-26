package com.bezpredel.versioned.cache.replication;

import com.bezpredel.versioned.cache.BasicCacheIdentifier;
import com.bezpredel.versioned.cache.SingleCacheService;
import com.bezpredel.versioned.cache.ImmutableCacheableObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

public class SnapshotRetrieverTask implements SingleCacheService.ReadCommand<SnapshotUpdateDescriptor> {
    private final SingleCacheService cacheService;
    private final Object sessionIdentifier;

    public SnapshotRetrieverTask(SingleCacheService cacheService, Object sessionIdentifier) {
        this.cacheService = cacheService;
        this.sessionIdentifier = sessionIdentifier;
    }

    public SnapshotUpdateDescriptor execute(SingleCacheService.ReadContext context) {
        Set<BasicCacheIdentifier> cacheNames = cacheService.getCacheNames();
        ArrayList<ImmutableCacheableObject<BasicCacheIdentifier>> objects = new ArrayList<ImmutableCacheableObject<BasicCacheIdentifier>>();

        for(BasicCacheIdentifier identifier : cacheNames) {
            Iterator<ImmutableCacheableObject<BasicCacheIdentifier>> valuesIter = context.values(identifier);
            while(valuesIter.hasNext()) {
                objects.add(valuesIter.next());
            }
        }

        return new SnapshotUpdateDescriptor(sessionIdentifier, context.getVersion(), objects);
    }

}
