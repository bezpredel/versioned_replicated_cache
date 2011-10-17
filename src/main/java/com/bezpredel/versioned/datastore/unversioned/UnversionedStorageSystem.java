package com.bezpredel.versioned.datastore.unversioned;

import com.bezpredel.collections.MapFactory;
import com.bezpredel.versioned.datastore.AsyncCommand;
import com.bezpredel.versioned.datastore.Keyed;
import com.bezpredel.versioned.datastore.StorageSystem;

import java.util.*;

/**
 * Date: 10/17/11
 * Time: 2:32 PM
 */
public class UnversionedStorageSystem<DATA, INDX> implements StorageSystem<DATA, INDX> {
    private final Map<DATA, Map<Object, Keyed>> dataMap;
    private final Map<INDX, Map<Object, Map<Object, Keyed>>> indexMap;

    private final Object writeLock;

    private int currentVersion = 0;

    public UnversionedStorageSystem(Class<DATA> dataIdentifierClass, Class<INDX> indexIdentifierClass, MapFactory mapFactory, Set<DATA> caches, Set<INDX> indices, Object writeLock) {
        this.writeLock = writeLock;

        this.dataMap = mapFactory.createMap(dataIdentifierClass);
        this.indexMap = mapFactory.createMap(indexIdentifierClass);

        for(DATA key : caches) {
            dataMap.put(key, new HashMap<Object, Keyed>());
        }

        for(INDX key : indices) {
            indexMap.put(key, new HashMap<Object, Map<Object, Keyed>>());
        }
    }

    public void executeWrite(WriteCommand<DATA, INDX> writeCommand, DataChangedCallback<DATA, INDX> callback) {
        synchronized (writeLock) {
            currentVersion++;

            writeCommand.execute(new WriteContextImpl(callback));

            if (callback != null) {
                callback.finished(this);
            }
        }
    }

    public <R> R executeRead(ReadCommand<R, DATA, INDX> command, boolean unlockAsynchronously) {
        throw new UnsupportedOperationException("Write-only cache");
    }

    public <R> AsyncCommand<R> startAsyncRead(ReadCommand<R, DATA, INDX> command, boolean unlockAsynchronously) {
        throw new UnsupportedOperationException("Write-only cache");
    }

    public ReadContext<DATA, INDX> dirtyReadContext() {
        return new WriteContextImpl(null);
    }

    public Collection<INDX> getSupportedIndexTypes() {
        return indexMap.keySet();
    }

    public Collection<DATA> getSupportedDataTypes() {
        return dataMap.keySet();
    }

    protected <T extends Keyed> Map<Object, T> getDataStore(DATA name) {
        return (Map<Object, T>) dataMap.get(name);
    }

    protected <T extends Keyed> Map<Object, Map<Object, T>> getIndexStore(INDX name) {
        return ( Map ) indexMap.get(name);
    }

    private class WriteContextImpl implements WriteContext<DATA, INDX> {
        private final DataChangedCallback callback;

        private WriteContextImpl(DataChangedCallback callback){
            this.callback = callback;
        }

        public int getVersion() {
            return currentVersion;
        }

        public <T extends Keyed> T put(DATA name, T value) {
            T previousValue = (T) UnversionedStorageSystem.this.getDataStore(name).put(value.getKey(), value);

            if(callback!=null) callback.replaced(name, previousValue, value);

            return previousValue;
        }

        public <T extends Keyed> T remove(DATA name, Object key) {
            T previousValue = (T)UnversionedStorageSystem.this.getDataStore(name).remove(key);

            if(callback!=null) callback.replaced(name, previousValue, null);

            return previousValue;
        }

        public void removeFromIndex(INDX name, Object leafKey, Object objectKey) {
            Map<Object, Map<Object, Keyed>> indx = UnversionedStorageSystem.this.getIndexStore(name);
            Map<Object, Keyed> leaf = indx.get(leafKey);
            if (leaf != null) {
                leaf.remove(objectKey);
                if (leaf.isEmpty()) {
                    indx.remove(leafKey);
                }
            }
        }

        public <T extends Keyed> void addToIndex(INDX name, Object leafKey, T value) {
            Map<Object, Map<Object, Keyed>> indx = UnversionedStorageSystem.this.getIndexStore(name);
            Map<Object, Keyed> leaf = indx.get(leafKey);
            if(leaf==null) {
                leaf = new HashMap<Object, Keyed>();
                indx.put(leafKey, leaf);
            }
            leaf.put(value.getKey(), value);
        }

        public <T extends Keyed> T get(DATA name, Object key) {
            return (T)UnversionedStorageSystem.this.getDataStore(name).get(key);
        }

        public <T extends Keyed> Iterator<T> values(DATA name) {
            return (Iterator<T>) UnversionedStorageSystem.this.getDataStore(name).values();
        }

        public <T extends Keyed> Iterator<T> valuesByIndex(INDX name, Object leafKey) {
            Map<Object, Map<Object, Keyed>> indx = UnversionedStorageSystem.this.getIndexStore(name);
            Map<Object, Keyed> leaf = indx.get(leafKey);
            return leaf!=null ? (Iterator)leaf.values().iterator() : EMPTY_ITER;
        }
    }

    private static final Iterator<Keyed> EMPTY_ITER = Collections.<Keyed>emptyList().iterator();
}
