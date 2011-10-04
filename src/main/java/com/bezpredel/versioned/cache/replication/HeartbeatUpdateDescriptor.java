package com.bezpredel.versioned.cache.replication;

import com.bezpredel.versioned.cache.BasicCacheIdentifier;
import com.bezpredel.versioned.cache.CacheService;
import com.bezpredel.versioned.cache.ImmutableCacheableObject;
import com.bezpredel.versioned.cache.UpdateDescriptor;

import java.util.Collection;
import java.util.Map;

class HeartbeatUpdateDescriptor implements UpdateDescriptor {
    private static final long serialVersionUID = -2895660870897169547L;
    private final int version;
    private final Object sessionIdentifier;

    public HeartbeatUpdateDescriptor(Object sessionIdentifier, int version) {
        this.version = version;
        this.sessionIdentifier = sessionIdentifier;
    }

    public int getVersion() {
        return version;
    }

    public Object getSessionIdentifier() {
        return sessionIdentifier;
    }

    public void applyTo(CacheService.WriteContext writeContext) {
    }
}
