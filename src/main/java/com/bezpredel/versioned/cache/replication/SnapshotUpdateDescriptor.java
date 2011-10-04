package com.bezpredel.versioned.cache.replication;

import com.bezpredel.versioned.cache.BasicCacheIdentifier;
import com.bezpredel.versioned.cache.CacheService;
import com.bezpredel.versioned.cache.ImmutableCacheableObject;

import java.util.Collection;

public class SnapshotUpdateDescriptor extends UpdateDescriptorImpl {
    private static final long serialVersionUID = 835880220256030798L;

    public SnapshotUpdateDescriptor(Object sessionIdentifier, int version, Collection<ImmutableCacheableObject<BasicCacheIdentifier>> inserts) {
        super(sessionIdentifier, version, inserts, null);
    }

    @Override
    public void applyTo(CacheService.WriteContext writeContext) {
        super.applyTo(writeContext);
    }


}
