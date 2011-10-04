package com.bezpredel.versioned.cache.replication;

import com.bezpredel.collections.Pair;
import com.bezpredel.utils.Distributor;
import com.bezpredel.versioned.cache.BasicCacheIdentifier;
import com.bezpredel.versioned.cache.CacheService;
import com.bezpredel.versioned.cache.ImmutableCacheableObject;
import com.bezpredel.versioned.cache.UpdateDescriptor;

import java.util.*;

public class MasterCacheSyncServer implements CacheService.DataListener {
    private final CacheService cacheService;
    private final Distributor<UpdateDescriptor> distributor;
    private final String sessionIdentifier = "" + System.currentTimeMillis();


    public MasterCacheSyncServer(CacheService cacheService, Distributor<UpdateDescriptor> distributor) {
        this.cacheService = cacheService;
        this.distributor = distributor;
        this.cacheService.addDataListener(this);

    }

    public SnapshotUpdateDescriptor retrieveSnapshotUpdateDescriptor() {
        return cacheService.executeRead(new SnapshotRetrieverTask(cacheService, sessionIdentifier));
    }

    public HeartbeatUpdateDescriptor createHeartbeat() {
        return new HeartbeatUpdateDescriptor(sessionIdentifier, 0);
    }

    public void onDataChanged(CacheService source, int version, List<Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>>> changes) {
        distributor.distribute(convertIntoUpdate(version, changes));
    }

    private UpdateDescriptor convertIntoUpdate(int version, List<Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>>> changes) {
        // assert no repeats
        List<ImmutableCacheableObject<BasicCacheIdentifier>> inserts = new ArrayList<ImmutableCacheableObject<BasicCacheIdentifier>>();
        Map<BasicCacheIdentifier, Collection<Object>> removedKeys = new HashMap<BasicCacheIdentifier, Collection<Object>>();

        for(Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>> pair : changes) {
            if (pair.getSecond() != null) {
                inserts.add(pair.getSecond());
            } else {
                ImmutableCacheableObject<BasicCacheIdentifier> before = pair.getFirst();

                assert before !=null;
                Collection<Object> leaf = removedKeys.get(before.getCacheType());
                if(leaf == null) {
                    removedKeys.put(before.getCacheType(), leaf = new ArrayList<Object>());
                }
                leaf.add(before.getKey());
            }
        }

        return new UpdateDescriptorImpl(sessionIdentifier, version, inserts, removedKeys);
    }


}
