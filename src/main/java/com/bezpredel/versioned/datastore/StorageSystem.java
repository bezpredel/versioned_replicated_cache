package com.bezpredel.versioned.datastore;

import com.bezpredel.collections.PseudoEnum;

import java.util.Collection;
import java.util.Iterator;

public interface StorageSystem<DATA, INDX> {
    void executeWrite(WriteCommand<DATA, INDX> writeCommand, DataChangedCallback<DATA, INDX> callback);

    <R> R executeRead(ReadCommand<R, DATA, INDX> command, boolean unlockAsynchronously);

    <R> AsyncCommand<R> startAsyncRead(ReadCommand<R, DATA, INDX> command, boolean unlockAsynchronously);

    ReadContext<DATA, INDX> dirtyReadContext();

    Collection<DATA> getSupportedDataTypes();

    Collection<INDX> getSupportedIndexTypes();

    SSID getStorageSystemID();

    public interface DataChangedCallback<DATA, INDX> {
        void replaced(DATA name, Keyed before, Keyed after);
        void finished(StorageSystem<DATA, INDX> source);
    }

    public interface ReadCommand<R, D, I> {
        R execute(ReadContext<D, I> context);
    }

    public interface WriteCommand<D, I> {
        void execute(WriteContext<D, I> context);
    }

    public interface ReadContext<D, I> {
        int getVersion();

        <T extends Keyed> T get(D name, Object key);

        <T extends Keyed> Iterator<T> values(D name);

        <T extends Keyed> Iterator<T> valuesByIndex(I name, Object leafKey);
    }

    public interface WriteContext<D, I> extends ReadContext<D, I> {
        <T extends Keyed> T put(D name, T value);

        <T extends Keyed> T remove(D name, Object key);

        <T extends Keyed> void addToIndex(I name, Object leafKey, T value);

        void removeFromIndex(I name, Object leafKey, Object objectKey);
    }

    public static final class SSID extends PseudoEnum {
        private final StorageSystem storageSystem;

        public SSID(StorageSystem storageSystem) {
            this.storageSystem = storageSystem;
        }

        public <DATA, INDX> StorageSystem<DATA, INDX> getStorageSystem() {
            return storageSystem;
        }
    }
}
