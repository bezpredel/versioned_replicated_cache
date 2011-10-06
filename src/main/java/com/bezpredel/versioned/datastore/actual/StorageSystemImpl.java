package com.bezpredel.versioned.datastore.actual;


import com.bezpredel.collections.MapFactory;
import com.bezpredel.versioned.datastore.AsyncCommand;
import com.bezpredel.versioned.datastore.Keyed;
import com.bezpredel.versioned.datastore.StorageSystem;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

public class StorageSystemImpl<DATA, INDX> implements StorageSystem<DATA, INDX>, Sync {
    private final Map<DATA, VersionedDataStorage<Keyed>> dataMap;
    private final Map<INDX, OneToManyIndexStorage<Keyed>> indexMap;

    private final LockRecordStore<Keyed> lockRecordStore;

    private final Object writeLock;

    private final Object localReadLock = new Object();
    private final Executor unlockExecutor;


    private int currentVersion = 0;


    public StorageSystemImpl(Class<DATA> dataIdentifierClass, Class<INDX> indexIdentifierClass, MapFactory mapFactory, Set<DATA> caches, Set<INDX> indices, Object writeLock, Executor unlockExecutor) {
        this.writeLock = writeLock;
        this.unlockExecutor = unlockExecutor;
        this.dataMap = mapFactory.createMap(dataIdentifierClass);
        this.indexMap = mapFactory.createMap(indexIdentifierClass);
        this.lockRecordStore = new LockRecordStore<Keyed>(this);

        for(DATA key : caches) {
            VersionedDataStorageImpl<Keyed> vds = new VersionedDataStorageImpl<Keyed>(lockRecordStore);
            dataMap.put(key, vds);
        }

        for(INDX key : indices) {
            OneToManyIndexStorageImpl<Keyed> vds = new OneToManyIndexStorageImpl<Keyed>(lockRecordStore);
            indexMap.put(key, vds);
        }
    }


    protected <T extends Keyed> VersionedDataStorage<T> getDataStore(DATA name) {
        return (VersionedDataStorage<T>) dataMap.get(name);
    }

    protected <T extends Keyed> OneToManyIndexStorage<T> getIndexStore(INDX name) {
        return (OneToManyIndexStorage<T>) indexMap.get(name);
    }

    public ReadContext<DATA, INDX> dirtyReadContext() {
        return new DirtyReadContextImpl(currentVersion);
    }

    private class DirtyReadContextImpl implements ReadContext<DATA, INDX> {
        private final int version;

        public DirtyReadContextImpl(int version) {
            this.version = version;
        }

        public int getVersion() {
            return version;
        }

        public <T extends Keyed> T get(DATA name, Object key) {
            return (T)StorageSystemImpl.this.getDataStore(name).getTop(key);
        }

        public <T extends Keyed> Iterator<T> values(DATA name) {
            return (Iterator<T>) StorageSystemImpl.this.getDataStore(name).values(getVersion(), false);
        }

        public <T extends Keyed> Iterator<T> valuesByIndex(INDX name, Object leafKey) {
            return (Iterator<T>) StorageSystemImpl.this.getIndexStore(name).get(leafKey, getVersion(), false);
        }
    }

    private class ReadContextImpl implements ReadContext<DATA, INDX> {
        private final int version;

        private ReadContextImpl(int version) {
            this.version = version;
        }

        public int getVersion() {
            return version;
        }

        public <T extends Keyed> T get(DATA name, Object key) {
            return (T)StorageSystemImpl.this.getDataStore(name).get(key, getVersion());
        }

        public <T extends Keyed> Iterator<T> values(DATA name) {
            return (Iterator<T>) StorageSystemImpl.this.getDataStore(name).values(getVersion(), false);
        }

        public <T extends Keyed> Iterator<T> valuesByIndex(INDX name, Object leafKey) {
            return (Iterator<T>) StorageSystemImpl.this.getIndexStore(name).get(leafKey, getVersion(), false);
        }
    }

    private class WriteContextImpl implements WriteContext<DATA, INDX>{
        private final DataChangedCallback callback;

        private WriteContextImpl(DataChangedCallback callback){
            this.callback = callback;
        }

        public int getVersion() {
            return currentVersion;
        }

        public <T extends Keyed> VersionedDataStorage<T> get(DATA name) {
            return StorageSystemImpl.this.getDataStore(name);
        }

        public <T extends Keyed> T put(DATA name, T value) {
            T previousValue = (T) StorageSystemImpl.this.getDataStore(name).put(value, getVersion());

            if(callback!=null) callback.replaced(name, previousValue, value);

            return previousValue;
        }

        public <T extends Keyed> T remove(DATA name, Object key) {
            T previousValue = (T)StorageSystemImpl.this.getDataStore(name).remove(key, getVersion());

            if(callback!=null) callback.replaced(name, previousValue, null);

            return previousValue;
        }

        public void removeFromIndex(INDX name, Object leafKey, Object objectKey) {
            StorageSystemImpl.this.getIndexStore(name).remove(leafKey, objectKey, getVersion());
        }

        public <T extends Keyed> void addToIndex(INDX name, Object leafKey, T value) {
            StorageSystemImpl.this.getIndexStore(name).add(leafKey, value, getVersion());
        }

        public <T extends Keyed> T get(DATA name, Object key) {
            return (T)StorageSystemImpl.this.getDataStore(name).get(key, getVersion());
        }

        public <T extends Keyed> Iterator<T> values(DATA name) {
            return (Iterator<T>) StorageSystemImpl.this.getDataStore(name).values(getVersion(), true);
        }

        public <T extends Keyed> Iterator<T> valuesByIndex(INDX name, Object leafKey) {
            return (Iterator<T>) StorageSystemImpl.this.getIndexStore(name).get(leafKey, getVersion(), true);
        }
    }


    public void executeWrite(WriteCommand<DATA, INDX> writeCommand, DataChangedCallback<DATA, INDX> callback) {
        synchronized (writeLock) {
            synchronized (localReadLock) {
                currentVersion++;

                writeCommand.execute(new WriteContextImpl(callback));

                if (callback != null) {
                    callback.finished(this);
                }
            }
        }
    }

    public <R> R executeRead(ReadCommand<R, DATA, INDX> command, boolean unlockAsynchronously) {
        VersionLock<Keyed> versionLockObj = lock();

        try {
            ReadContext<DATA, INDX> context = new ReadContextImpl(versionLockObj.getVersion());
            return command.execute(context);
        } finally {
            unlock(versionLockObj, unlockAsynchronously);
        }
    }

    public <R> AsyncCommand<R> startAsyncRead(ReadCommand<R, DATA, INDX> command, boolean unlockAsynchronously) {
        VersionLock<Keyed> versionLockObj = lock();

        return new AsyncCommandImpl(command, versionLockObj, unlockAsynchronously);
    }

    public int getCurrentVersion() {
        synchronized (localReadLock) {
            return currentVersion;
        }
    }

    private void unlock(VersionLock<Keyed> versionLockObj, boolean unlockAsynchronously) {
        boolean unlocked = lockRecordStore.unlockStep1(versionLockObj);
        if(unlocked) {
            if(unlockAsynchronously) {
                unlockExecutor.execute( new UnlockTask( versionLockObj ) );
            } else {
                unlockInner(versionLockObj);
            }
        }
    }

    private VersionLock<Keyed> lock() {
        synchronized (localReadLock) {
            return lockRecordStore.lockVersion(currentVersion);
        }
    }


    private void unlockInner(VersionLock<Keyed> versionLock) {
        synchronized (localReadLock) {
            lockRecordStore.unlockStep2(versionLock);
        }
    }

    private class UnlockTask implements Runnable {
        private final VersionLock<Keyed> versionLock;

        public UnlockTask(VersionLock<Keyed> versionLock) {
            this.versionLock = versionLock;
        }

        public void run() {
            unlockInner(versionLock);
        }
    }

    public boolean assertInSharedLock() {
        return Thread.holdsLock(localReadLock);
    }

    public boolean assertInExclusiveWriteLock() {
        return Thread.holdsLock(writeLock);
    }

    public boolean isInWriteLock() {
        return Thread.holdsLock(writeLock);
    }

    /**
     * for testing only
     */
    public void __explicitLock() {
        lockRecordStore.lockVersion(currentVersion);
    }

    public Collection<INDX> getSupportedIndexTypes() {
        return indexMap.keySet();
    }

    public Collection<DATA> getSupportedDataTypes() {
        return dataMap.keySet();
    }

    protected class AsyncCommandImpl<R> implements AsyncCommand<R> {
        private final VersionLock<Keyed> versionLockObj;
        private final boolean unlockAsynchronously;
        private final ReadCommand<R, DATA, INDX> command;
        private boolean done = false;

        public AsyncCommandImpl(ReadCommand<R, DATA, INDX> command, VersionLock<Keyed> versionLockObj, boolean unlockAsynchronously) {
            this.versionLockObj = versionLockObj;
            this.unlockAsynchronously = unlockAsynchronously;
            this.command = command;
        }

        public void cancel() {
            if(!done) {
                done = true;
                unlock(versionLockObj, unlockAsynchronously);
            }
        }

        public void run() {
            execute();
        }

        public R execute() {
            if(done) throw new IllegalStateException("Already done");

            try {
                ReadContext<DATA, INDX> context = new ReadContextImpl(versionLockObj.getVersion());
                return command.execute(context);
            } finally {
                done = true;
                unlock(versionLockObj, unlockAsynchronously);
            }
        }
    }
}
