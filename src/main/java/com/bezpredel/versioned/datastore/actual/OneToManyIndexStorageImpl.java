package com.bezpredel.versioned.datastore.actual;

import com.bezpredel.collections.AlmostHashMap;
import com.bezpredel.versioned.datastore.Keyed;

import java.util.Collections;
import java.util.Iterator;

public class OneToManyIndexStorageImpl<T extends Keyed> implements OneToManyIndexStorage<T> {
    private static final int INITIAL_CAPACITY = 16;
    private static final double LOAD_FACTOR = 0.5;
    private static final double SHRINK_RATIO = 0.5;

    private final LockRecordStore<T> lockRecordStore;
    private final AlmostHashMap<Leaf<T>> map;

    public OneToManyIndexStorageImpl(LockRecordStore<T> lockRecordStore) {
        this.lockRecordStore = lockRecordStore;
        this.map = new AlmostHashMap<Leaf<T>>(INITIAL_CAPACITY, LOAD_FACTOR, SHRINK_RATIO);
    }

    public void add(Object leafKey, T value, int currentVersion) {
        Leaf<T> leaf = map.get(leafKey);
        if(leaf==null) {
            leaf = new Leaf<T>(lockRecordStore, leafKey, this);
            map.put(leaf);
        }

        leaf.put(value, currentVersion);
    }

    public void remove(Object leafKey, Object objectKey, int currentVersion) {
        Leaf<T> leaf = map.get(leafKey);
        if(leaf!=null) {
            leaf.remove(objectKey, currentVersion);
            if(leaf.isEmpty()) {
                map.removeKey(leafKey);
            }
        } else {
            // why would we care?
        }
    }

    protected void removeDeadMember(Leaf<T> leaf) {
        // assert leaf not null and dead
        map.removeObject(leaf);
    }

    public Iterator<T> get(Object leafKey, int version, boolean isInWriteLock) {
        Leaf<T> leaf = map.get(leafKey);
        if(leaf==null) {
            return EMPTY.values(version, isInWriteLock);
        } else {
            return leaf.values(version, isInWriteLock);
        }
    }

    private static class Leaf<T extends Keyed> extends VersionedDataStorageImpl<T> implements Keyed {
        private final Object leafKey;
        private final OneToManyIndexStorageImpl<T> parent;

        public Leaf(LockRecordStore<T> lockRecordStore, Object leafKey, OneToManyIndexStorageImpl<T> parent) {
            super(lockRecordStore);
            this.leafKey = leafKey;
            this.parent = parent;
        }

        public Object getKey() {
            return leafKey;
        }

        @Override
        public void removeDeadMember(VersionedValue<T> tVersionedValue) {
            super.removeDeadMember(tVersionedValue);
            if(isEmpty()) {
                parent.removeDeadMember(this);
            }
        }
    }

    private static final VersionedDataStorage EMPTY = new VersionedDataStorage<Keyed>() {
        private final Iterator<Keyed> iter = Collections.<Keyed>emptyList().iterator();

        public Keyed put(Keyed value, int currentVersion) {
            throw new UnsupportedOperationException();
        }

        public Keyed remove(Object key, int currentVersion) {
            throw new UnsupportedOperationException();
        }

        public Keyed get(Object key, int version) {
            return null;
        }

        public Keyed getTop(Object key) {
            return null;
        }

        public Iterator<Keyed> values(int version, boolean isInWriteLock) {
            return iter;
        }

        public void removeDeadMember(VersionedValue<Keyed> keyedVersionedValue) {
        }
    };
}
