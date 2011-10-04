package com.bezpredel.versioned.datastore.actual;

import com.bezpredel.collections.AbstractDelegatingIterator;
import com.bezpredel.collections.AlmostHashMap;
import com.bezpredel.versioned.datastore.Keyed;

import java.util.Collections;
import java.util.Iterator;

public class VersionedDataStorageImpl<T extends Keyed> implements VersionedDataStorage<T> {
    private static final int INITIAL_CAPACITY = 16;
    private static final double LOAD_FACTOR = 0.5;
    private static final double SHRINK_RATIO = 0.5;

    protected final LockRecordStore<T> lockRecordStore;
    private final AlmostHashMap<VersionedValue<T>> map;

    public VersionedDataStorageImpl(LockRecordStore<T> lockRecordStore) {
        this.lockRecordStore = lockRecordStore;
        this.map = new AlmostHashMap<VersionedValue<T>>(INITIAL_CAPACITY, LOAD_FACTOR, SHRINK_RATIO);
    }

    public boolean isEmpty() {
        return map==null || map.isEmpty();
    }

    public T put(T value, int currentVersion) {
        if (value == null) throw new NullPointerException("put(null)");

        Object key = value.getKey();

        VersionedValue<T> vv = map.get(key);

        if (vv == null) {
            vv = new VersionedValueImpl<T>(value, currentVersion, this);
            map.put(vv);
            return null;
        } else {
            lockRecordStore.lockIfNeeded(vv);
            T retVal = vv.addNewValue(value, currentVersion);

            if(lockRecordStore.getEarliestVersionLocked() > currentVersion) {
                vv.collapse(lockRecordStore.getEarliestVersionLocked(), lockRecordStore.getLatestVersionLocked());
            }

            return retVal;
        }
    }


    public T remove(Object key, int currentVersion) {
        VersionedValue<T> vv = map.get(key);

        if (vv == null) {
            return null;
        } else {
            lockRecordStore.lockIfNeeded(vv);

            T retVal = vv.addNewValue(null, currentVersion);

            if(vv.isDead(lockRecordStore.getEarliestVersionLocked())) {
                map.removeObject(vv);
            } else if(lockRecordStore.getEarliestVersionLocked() > currentVersion) {
                vv.collapse(lockRecordStore.getEarliestVersionLocked(), lockRecordStore.getLatestVersionLocked());
            }

            return retVal;
        }
    }

    public T get(Object key, int version) {
        AlmostHashMap<VersionedValue<T>> map = this.map;
        if(map==null) return null; // since reads are not synchronized, this object might end up being read before it is initialized. In this case we do not care for it anyhow because it is expected to contain the data of the wrong version
        VersionedValue<T> vv = map.get(key);
        return vv!=null ? vv.getValue(version) : null;
    }

    /**
     * To only be called from within a write lock
     * @param key
     * @return
     */
    public T getTop(Object key) {
        VersionedValue<T> vv = map.get(key);
        return vv!=null ? vv.getTopValue() : null;
    }

    public Iterator<T> values(int version, boolean isInWriteLock) {
        if(isInWriteLock) {
            return new TopIter<T>(map.valueIterator());
        } else {
            if(map==null) {
                // since reads are not synchronized, this object might end up being read before it is initialized. In this case we do not care for it anyhow because it is expected to contain the data of the wrong version
                return Collections.<T>emptyList().iterator();
            } else {
                return new Iter<T>(map.valueIterator(), version);
            }
        }
    }

    // only called from within the lock
    public void removeDeadMember(VersionedValue<T> versionedValue) {
        //assert leaf is not null, is dead and belongs to me
        map.removeObject(versionedValue);
    }

    private static class Iter<T extends Keyed> extends AbstractDelegatingIterator<VersionedValue<T>, T> {
        private final int version;

        private Iter(Iterator<VersionedValue<T>> delegate, int version) {
            super(delegate);
            this.version = version;
        }

        @Override
        protected T getValue(VersionedValue<T> vv) {
            return vv.getValue(version);
        }
    }

    private static class TopIter<T extends Keyed> extends AbstractDelegatingIterator<VersionedValue<T>, T> {

        private TopIter(Iterator<VersionedValue<T>> delegate) {
            super(delegate);
        }

        @Override
        protected T getValue(VersionedValue<T> vv) {
            return vv.getTopValue();
        }
    }

}
