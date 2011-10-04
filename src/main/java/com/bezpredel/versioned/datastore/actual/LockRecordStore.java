package com.bezpredel.versioned.datastore.actual;


import com.bezpredel.versioned.datastore.Keyed;

import java.util.ArrayDeque;
import java.util.Collection;

class LockRecordStore<T extends Keyed>  {
    private final Sync sync;
    private int earliestVersionLocked = Integer.MAX_VALUE;
    private int latestVersionLocked = Integer.MIN_VALUE;


    private final ArrayDeque<VersionLock<T>> records = new ArrayDeque<VersionLock<T>>();

    public LockRecordStore(Sync sync) {
        this.sync = sync;
    }

    public boolean isAnythingLocked() {
        return !records.isEmpty();
    }

    public void lockIfNeeded(VersionedValue<T> entry) {
        if(isToBeLocked(entry)) {
            records.getLast().lockValue(entry);
        }
    }

    private boolean isToBeLocked(VersionedValue<T> entry) {
        return (entry.getLatestVersion() <= latestVersionLocked);
    }

    public VersionLock<T> lockVersion(int version) {
        assert sync.assertInSharedLock();

        assert latestVersionLocked <= version;

        if(latestVersionLocked==version) {
            VersionLock<T> lockRecord = records.getLast();
            assert lockRecord.getVersion()==version;
            lockRecord.incLockCount();


            return lockRecord;
        } else {
            latestVersionLocked = version;
            if(earliestVersionLocked == Integer.MAX_VALUE) {
                earliestVersionLocked = version;
            } else {
                assert earliestVersionLocked <= latestVersionLocked;
            }
            VersionLock<T> lockRecord = new VersionLock<T>(version);
            records.addLast(lockRecord);


            return lockRecord;
        }
    }


    public boolean unlockStep1(VersionLock<T> versionLock) {
        return versionLock.decLockCount();
    }

    public void unlockStep2(VersionLock<T> versionLock) {
        assert sync.assertInSharedLock();

        if(earliestVersionLocked==versionLock.getVersion()) {
            assert records.getFirst()==versionLock;
            doRemovalProcedure();
        }
    }

    private void doRemovalProcedure() {

        { // identify the earliest record that is still locked.
            VersionLock<T> earliestLockedRecord = null;
            for(VersionLock<T> lockRecord : records) {
                if(lockRecord.isLocked()) {
                    earliestLockedRecord = lockRecord;
                    earliestVersionLocked = lockRecord.getVersion();
                    break;
                }
            }

            if(earliestLockedRecord==null) {
                earliestVersionLocked = Integer.MAX_VALUE;
                latestVersionLocked = Integer.MIN_VALUE;
            }
        }

        VersionLock<T> currRecord;

        while (true) {
            currRecord = records.peekFirst();
            // while this loop is running, a record might get unlocked. This may happen because of the two-step unlock.
            // In this case, let us pretend that record is still locked, and unlock it in the next removal
            // Since records never get re-locked, from (currRecord.getVersion() < earliestVersionLocked) follows (currRecord.isLocked()===false) guaranteed.
            if(currRecord==null || currRecord.getVersion() >= earliestVersionLocked) {
                break;
            } else {
                assert !currRecord.isLocked();
                records.pollFirst();
            }

            Collection<VersionedValue<T>> lockedValues = currRecord.getLockedValues();


            if (lockedValues != null) {
                for(VersionedValue<T> value : lockedValues) {
                    int retVal = value.collapse(earliestVersionLocked, latestVersionLocked);
                    assert retVal <= earliestVersionLocked;

                    if(value.isDead(earliestVersionLocked)) {
                        value.removeSelfFromParent();
                    }
                }
            }
        }
    }

    public int getEarliestVersionLocked() {
        assert sync.assertInSharedLock();

        return earliestVersionLocked;
    }

    public int getLatestVersionLocked() {
        assert sync.assertInSharedLock();

        return latestVersionLocked;
    }
}
