package com.bezpredel.versioned.cache;


import com.bezpredel.collections.Pair;
import com.bezpredel.collections.Utils;
import com.bezpredel.versioned.datastore.AsyncCommand;
import com.bezpredel.versioned.datastore.Keyed;
import com.bezpredel.versioned.datastore.StorageSystem;
import com.google.common.base.Function;

import javax.annotation.Nullable;
import java.util.*;

public class CacheService {
    private final String name;
    private final StorageSystem<OneToOneID, OneToManyID> baseStorageSystem;
    private final StorageSystem<OneToOneID, OneToManyID> txStorageSystem;
    private final Map<OneToOneID, CacheImpl> caches;
    private final boolean unlockAsynchronously;
    private final Set<BasicCacheIdentifier> cacheNames;

    private final ArrayList<DataListener> listeners = new ArrayList<DataListener>();

    public CacheService(CacheServiceInitializer cacheServiceInitializer) {
        name = cacheServiceInitializer.getName();
        baseStorageSystem = cacheServiceInitializer.initializeStorageSystem();
        txStorageSystem = cacheServiceInitializer.initializeVirtualWriter(baseStorageSystem);
        caches = cacheServiceInitializer.initializeCaches();
        unlockAsynchronously = cacheServiceInitializer.isUnlockAsynchronously();

        cacheNames = Utils.map(caches.keySet(), new Function<OneToOneID, BasicCacheIdentifier>() {
            public BasicCacheIdentifier apply(OneToOneID input) {
                return input.getCacheIdentifier();
            }
        });
    }

    public void executeWrite(WriteCommand command) {
        ProxyWriteCommand proxyCommand = new ProxyWriteCommand(command);
        txStorageSystem.executeWrite(proxyCommand, proxyCommand);
    }

    public <T> T executeRead(ReadCommand<T> command) {
        return txStorageSystem.executeRead(new ProxyReadCommand<T>(command), unlockAsynchronously);
    }

    public <T> AsyncCommand<T> startAsyncRead(ReadCommand<T> command) {
        return txStorageSystem.startAsyncRead(new ProxyReadCommand<T>(command), unlockAsynchronously);
    }

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

        CacheService getCacheService();
    }

    public interface WriteCommand {
        void execute(WriteContext context);
    }

    public interface ReadCommand<R> {
        R execute(ReadContext context);
    }

    private class ProxyWriteCommand implements StorageSystem.WriteCommand<OneToOneID, OneToManyID>, StorageSystem.DataChangedCallback<OneToOneID, OneToManyID> {
        private final WriteCommand command;
        private List<Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>>> txRecords;
        private int version;

        private ProxyWriteCommand(WriteCommand command) {
            this.command = command;
        }

        public void execute(StorageSystem.WriteContext<OneToOneID, OneToManyID> context) {
            WriteContextImpl outerContext = new WriteContextImpl(context);
            version = context.getVersion();

            command.execute(outerContext);
        }

        private List<Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>>> getRecords() {
            if(txRecords == null) {
                txRecords = new ArrayList<Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>>>();
            }
            return txRecords;
        }

        public void replaced(OneToOneID cacheName, Keyed before, Keyed after) {
            if (before != null || after != null) {
                if(!cacheName.isIndex()) {
                    getRecords().add(new Pair(before, after));
                }
            }
        }

        public void finished(StorageSystem<OneToOneID, OneToManyID> source) {
            fireOnDataChanged(version, txRecords);
        }
    }

    private class ProxyReadCommand<T> implements StorageSystem.ReadCommand<T, OneToOneID, OneToManyID> {
        private final ReadCommand<T> command;

        private ProxyReadCommand(ReadCommand<T> command) {
            this.command = command;
        }

        public T execute(StorageSystem.ReadContext<OneToOneID, OneToManyID> context) {
            ReadContextImpl outerContext = new ReadContextImpl(context);

            return command.execute(outerContext);
        }
    }


    private static class ReadContextImpl implements ReadContext {
        private final StorageSystem.ReadContext<OneToOneID, OneToManyID> readContext;

        private ReadContextImpl(StorageSystem.ReadContext<OneToOneID, OneToManyID> readContext) {
            this.readContext = readContext;
        }

        public <T extends ImmutableCacheableObject<BasicCacheIdentifier>> T get(BasicCacheIdentifier cache, Object key) {
            return readContext.<T>get(cache.getDataStoreId(), key);
        }

        public <T extends ImmutableCacheableObject<BasicCacheIdentifier>> Iterator<T> values(BasicCacheIdentifier cache) {
            return readContext.<T>values(cache.getDataStoreId());
        }

        public <T extends ImmutableCacheableObject<BasicCacheIdentifier>> T getByIndex(BasicOneToOneIndexIdentifier cache, Object key) {
            KeyDecorator keyDecorator = readContext.get(cache.getDataStoreId(), KeyDecorator.wrap(key));
            return keyDecorator == null ? null : (T) keyDecorator.getValue();
        }

        public <T extends ImmutableCacheableObject<BasicCacheIdentifier>> Iterator<T> valuesByIndex(BasicOneToManyIndexIdentifier cache, Object key) {
            return readContext.<T>valuesByIndex(cache.getDataStoreId(), KeyDecorator.wrap(key));
        }

        public int getVersion() {
            return readContext.getVersion();
        }
    }

    private class WriteContextImpl extends ReadContextImpl implements WriteContext {
        private final StorageSystem.WriteContext<OneToOneID, OneToManyID> writeContext;

        private WriteContextImpl(StorageSystem.WriteContext<OneToOneID, OneToManyID> writeContext) {
            super(writeContext);
            this.writeContext = writeContext;
        }

        public <T extends ImmutableCacheableObject<BasicCacheIdentifier>> T put(T object) {
            BasicCacheIdentifier type = object.getCacheType();
            CacheImpl cache = caches.get(type.getDataStoreId());
            return (T)cache.put(writeContext, object);
        }

        public <T extends ImmutableCacheableObject<BasicCacheIdentifier>> T remove(T object) {
            assert object != null;
            BasicCacheIdentifier type = object.getCacheType();
            CacheImpl cache = caches.get(type.getDataStoreId());
            ImmutableCacheableObject<BasicCacheIdentifier> previousValue = cache.remove(writeContext, object.getKey());
            if (previousValue != null && previousValue != object) {
                throw new IllegalArgumentException("Object being removed is not the latest version");
            }
            return (T)previousValue;
        }

        public <T extends ImmutableCacheableObject<BasicCacheIdentifier>> T remove(BasicCacheIdentifier type, Object key) {
            CacheImpl cache = caches.get(type.getDataStoreId());
            return (T)cache.remove(writeContext, key);
        }

        public int clear(BasicCacheIdentifier type) {
            int cnt = 0;
            HashSet<Object> keys = new HashSet<Object>();
            Iterator<ImmutableCacheableObject<BasicCacheIdentifier>> iter = values(type);
            while(iter.hasNext()) {
                keys.add(iter.next().getCacheType());
            }

            CacheImpl cache = caches.get(type.getDataStoreId());

            for(Object key : keys) {
                cache.remove(writeContext, key);
                cnt++;
            }

            return cnt;
        }

        public CacheService getCacheService() {
            return CacheService.this;
        }
    }

    protected StorageSystem<OneToOneID, OneToManyID> getBaseStorageSystem() {
        return baseStorageSystem;
    }

    protected StorageSystem<OneToOneID, OneToManyID> getTxStorageSystem() {
        return txStorageSystem;
    }

    protected void fireOnDataChanged(int version, List<Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>>> txRecords) {
        for(int i=0, c=listeners.size(); i<c; i++) {
            listeners.get(i).onDataChanged(this, version, txRecords);
        }
    }

    public void addDataListener(DataListener listener) {
        synchronized (listeners) {
            if(!listeners.contains(listener)) listeners.add(listener);
        }
    }

    public void removeDataListener(DataListener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }

    public Set<BasicCacheIdentifier> getCacheNames() {
        return cacheNames;
    }

    public String getName() {
        return name;
    }

    public String toString() {
        return "CacheService#" + name;
    }

    public interface DataListener {
        void onDataChanged(CacheService source, int version, List<Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>>> changes);
    }

}
