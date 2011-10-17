package com.bezpredel.versioned.datastore;

public abstract class AbstractStorageSystem<DATA, INDX> implements StorageSystem<DATA, INDX> {
    private final SSID ssid = new SSID(this);
    protected final Object writeLock;

    public AbstractStorageSystem(Object writeLock) {
        this.writeLock = writeLock;
    }

    public SSID getStorageSystemID() {
        return ssid;
    }
}
