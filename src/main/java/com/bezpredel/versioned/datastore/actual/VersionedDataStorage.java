package com.bezpredel.versioned.datastore.actual;


import com.bezpredel.versioned.datastore.Keyed;

import java.util.Iterator;

public interface VersionedDataStorage<T extends Keyed> {
    T put(T value, int currentVersion);

    T remove(Object key, int currentVersion);

    T get(Object key, int version);

    /**
     * For calling within a write lock only
     */
    T getTop(Object key);

    Iterator<T> values(int version, boolean isInWriteLock);

    void removeDeadMember(VersionedValue<T> value);
}
