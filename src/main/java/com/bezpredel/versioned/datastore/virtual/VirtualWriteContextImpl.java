package com.bezpredel.versioned.datastore.virtual;

import com.bezpredel.versioned.datastore.Keyed;
import com.bezpredel.versioned.datastore.StorageSystem;

import java.util.Iterator;
import java.util.Map;

class VirtualWriteContextImpl<D, I> extends VirtualData<D, I> implements StorageSystem.WriteContext<D, I> {
    private final StorageSystem.ReadContext<D, I> readContext; // dirty read context

    VirtualWriteContextImpl(StorageSystem.ReadContext<D, I> readContext) {
        this.readContext = readContext;
    }

    public <T extends Keyed> T put(D name, T value) {
        Keyed prevValue = super._put(name, value);
        if(prevValue == null) {
            return readContext.<T>get(name, value.getKey());
        } else if(prevValue == NULL) {
            return null;
        } else {
            return (T) prevValue;
        }
    }

    public <T extends Keyed> T remove(D name, Object key) {
        Keyed prevValue = super._remove(name, key);
        if(prevValue==NULL) {
            return null;
        } else if(prevValue==null) {
            return readContext.<T>get(name, key);
        } else {
            return (T)prevValue;
        }
    }

    public <T extends Keyed> void addToIndex(I name, Object leafKey, T value) {
        super._putIntoIndex(name, leafKey, value);
    }

    public void removeFromIndex(I name, Object leafKey, Object objectKey) {
        super._removeFromIndex(name, leafKey, objectKey);
    }

    public int getVersion() {
        return readContext.getVersion() + 1;
    }

    public <T extends Keyed> T get(D name, Object key) {
        Keyed writtenValue = super._getValue(name, key);
        if(writtenValue==NULL) {
            return null;
        } else if(writtenValue==null) {
            return readContext.<T>get(name, key);
        } else {
            return (T)writtenValue;
        }
    }

    public <T extends Keyed> Iterator<T> values(D name) {
        Iterator<Keyed> fromParent = readContext.values(name);
        Map<Object, Keyed> fromHere = super._getValues(name);

        return new TwoSourceIterator<T>(fromParent, fromHere);
    }

    public <T extends Keyed> Iterator<T> valuesByIndex(I name, Object leafKey) {
        Iterator<Keyed> fromParent = readContext.valuesByIndex(name, leafKey);
        Map<Object, Keyed> fromHere = super._getValuesByIndex(name, leafKey);

        return new TwoSourceIterator<T>(fromParent, fromHere);
    }

}
