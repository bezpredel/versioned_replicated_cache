package com.bezpredel.versioned.datastore.actual;

import com.bezpredel.versioned.datastore.Keyed;

public class VersionedDataObjectCounter {
    private final LockRecordStore<Keyed> lockRecordStore;
    private final VersionedValue<VersionedCollectionSize> size;

    public VersionedDataObjectCounter(LockRecordStore<Keyed> lockRecordStore) {
        this.lockRecordStore = lockRecordStore;
        this.size  = new VersionedValueImpl<VersionedCollectionSize>(new VersionedCollectionSize(), -1, null);
    }

    public void countObjectAdded(int currentVersion) {
        if(size.getLatestVersion()==currentVersion) {
            size.getTopValue().inc();
        } else {
            lockRecordStore.lockIfNeeded((VersionedValue)size);
            size.addNewValue(new VersionedCollectionSize(size.getTopValue(), +1), currentVersion);

            if(lockRecordStore.getEarliestVersionLocked() > currentVersion) {
                size.collapse(lockRecordStore.getEarliestVersionLocked(), lockRecordStore.getLatestVersionLocked());
            }
        }
    }

    public void countObjectRemoved(int currentVersion) {
        if(size.getLatestVersion()==currentVersion) {
            size.getTopValue().dec();
        } else {
            lockRecordStore.lockIfNeeded((VersionedValue)size);
            size.addNewValue(new VersionedCollectionSize(size.getTopValue(), - 1), currentVersion);

            if(lockRecordStore.getEarliestVersionLocked() > currentVersion) {
                size.collapse(lockRecordStore.getEarliestVersionLocked(), lockRecordStore.getLatestVersionLocked());
            }
        }
    }

    public int getSize(int currentVersion) {
        return size.getValue(currentVersion).getSize();
    }

    private static class VersionedCollectionSize implements Keyed {
        private int size;

        private VersionedCollectionSize() {
        }

        private VersionedCollectionSize(VersionedCollectionSize prev, int delta) {
            this.size = prev.getSize() + delta;
        }

        public void inc() {
            size++;
        }

        public void dec() {
            size--;
        }

        public int getSize() {
            return size;
        }

        public Object getKey() {
            return "size";
        }
    }
}
