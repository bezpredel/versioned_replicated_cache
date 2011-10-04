package com.bezpredel.versioned.datastore.actual;

import com.bezpredel.versioned.datastore.Keyed;

import java.util.Iterator;

public interface OneToManyIndexStorage<T extends Keyed> {
    void add(Object leafKey, T value, int currentVersion);

    void remove(Object leafKey, Object objectKey, int currentVersion);

    Iterator<T> get(Object leafKey, int version, boolean isInWriteLock);
}
