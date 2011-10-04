package com.bezpredel.versioned.cache.replication;

import com.bezpredel.versioned.cache.BasicCacheIdentifier;
import com.bezpredel.versioned.cache.CacheService;
import com.bezpredel.versioned.cache.ImmutableCacheableObject;
import com.bezpredel.versioned.cache.UpdateDescriptor;

import java.util.Collection;
import java.util.Map;

class UpdateDescriptorImpl implements UpdateDescriptor {
    private static final long serialVersionUID = -2895660870897169547L;
    private final int version;
    private final Object sessionIdentifier;
    private final Collection<ImmutableCacheableObject<BasicCacheIdentifier>> inserts;
    private final Map<BasicCacheIdentifier, Collection<Object>> removedKeys;


    public UpdateDescriptorImpl(Object sessionIdentifier, int version, Collection<ImmutableCacheableObject<BasicCacheIdentifier>> inserts, Map<BasicCacheIdentifier, Collection<Object>> removedKeys) {
        this.sessionIdentifier = sessionIdentifier;
        this.version = version;
        this.inserts = inserts;
        this.removedKeys = removedKeys;
    }

    public int getVersion() {
        return version;
    }

    public Object getSessionIdentifier() {
        return sessionIdentifier;
    }

    public void applyTo(CacheService.WriteContext writeContext) {
        if(removedKeys!=null) {
            for(Map.Entry<BasicCacheIdentifier, Collection<Object>> entry : removedKeys.entrySet()) {
                BasicCacheIdentifier identifier = entry.getKey();
                for(Object key : entry.getValue()) {
                    writeContext.remove(identifier, key);
                }
            }
        }

        if(inserts!=null) {
            for(ImmutableCacheableObject<BasicCacheIdentifier> obj : inserts) {
                writeContext.put(obj);
            }
        }
    }

}
