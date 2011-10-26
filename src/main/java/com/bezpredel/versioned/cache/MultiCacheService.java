package com.bezpredel.versioned.cache;


import com.bezpredel.collections.PseudoEnumMap;
import com.bezpredel.versioned.datastore.AsyncCommand;

import java.util.*;


public class MultiCacheService implements CacheServiceI {
    private final String name;
    private final SingleCacheService[] cacheServices;

    private final Map<BasicCacheIdentifier, SingleCacheService> cacheToServiceMap;


    public MultiCacheService(String name, Collection<SingleCacheService> cacheServices) {
        this.name = name;
        this.cacheServices = cacheServices.toArray(new SingleCacheService[cacheServices.size()]);

        cacheToServiceMap = new HashMap<BasicCacheIdentifier, SingleCacheService>();

        for(SingleCacheService cacheService : cacheServices) {
            for(BasicCacheIdentifier cacheIdentifier : cacheService.getCacheNames()) {
                cacheToServiceMap.put(cacheIdentifier, cacheService);
            }
        }
    }


    public <T> T executeRead(ReadCommand<T> command, StorageGroup storageGroup) {
        if(storageGroup.getCacheServices().length == 1) {
            return storageGroup.getCacheServices()[0].executeRead(command);
        } else {
            return new MultiReadCommand<T>(storageGroup.getCacheServices(), command).go();
        }
    }

    public <T> AsyncCommand<T> startAsyncRead(ReadCommand<T> command, StorageGroup storageGroup) {
        //todo: implement
        throw new UnsupportedOperationException();
    }

    private class MultiReadCommand<R> implements ReadCommand<R>, ReadContext {
        private final ReadCommand<R> readCommand;
        private final SingleCacheService[] cacheServices;
        private final Map<CSID, ReadContext> contextMap;
        private int index = 0;


        private MultiReadCommand(SingleCacheService[] cacheServices, ReadCommand<R> readCommand) {
            this.cacheServices = cacheServices;
            this.readCommand = readCommand;
            this.contextMap = PseudoEnumMap.factory().createMap(CSID.class);
        }

        private R go() {

            if(index < cacheServices.length) {
                return cacheServices[index].executeRead(this);
            } else {
                return readCommand.execute(this);
            }
        }

        public R execute(ReadContext context) {
            contextMap.put(cacheServices[index].getId(), context);
            index++;
            return go();
        }


        private ReadContext getContext(BasicCacheIdentifier cache) {
            SingleCacheService cacheService = cacheToServiceMap.get(cache);
            if(cacheService==null) throw new IllegalArgumentException("Cache " + cache + " not supported by " + getName());
            ReadContext context = contextMap.get(cacheService.getId());
            if(context==null) throw new IllegalStateException("Cache " + cache + " is not locked");
            return context;
        }

        public <T extends ImmutableCacheableObject<BasicCacheIdentifier>> T get(BasicCacheIdentifier cache, Object key) {
            return getContext(cache).get(cache, key);
        }

        public <T extends ImmutableCacheableObject<BasicCacheIdentifier>> Iterator<T> values(BasicCacheIdentifier cache) {
            return getContext(cache).values(cache);
        }

        public <T extends ImmutableCacheableObject<BasicCacheIdentifier>> T getByIndex(BasicOneToOneIndexIdentifier cache, Object leafKey) {
            return getContext(cache.getCacheType()).getByIndex(cache, leafKey);
        }

        public <T extends ImmutableCacheableObject<BasicCacheIdentifier>> Iterator<T> valuesByIndex(BasicOneToManyIndexIdentifier cache, Object leafKey) {
            return getContext(cache.getCacheType()).valuesByIndex(cache, leafKey);
        }

        public int getVersion() {
            throw new UnsupportedOperationException("Ambiguous");
        }
    }


    public Set<BasicCacheIdentifier> getCacheNames() {
        return cacheToServiceMap.keySet();
    }

    public Set<IndexIdentifierType<BasicCacheIdentifier>> getSupportedIndexes() {
        //todo:
        throw new UnsupportedOperationException();
    }

    public String getName() {
        return name;
    }

    public static class StorageGroup {
        private final String name;
        private final SingleCacheService[] cacheServices;

        public StorageGroup(String name, SingleCacheService[] cacheServices) {
            this.name = name;
            this.cacheServices = cacheServices.clone();
        }

        private SingleCacheService[] getCacheServices() {
            return cacheServices;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "StorageGroup " + name + ':' + Arrays.toString(cacheServices);
        }
    }
}
