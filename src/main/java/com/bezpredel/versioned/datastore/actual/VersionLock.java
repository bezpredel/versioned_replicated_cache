package com.bezpredel.versioned.datastore.actual;

import com.bezpredel.versioned.datastore.Keyed;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class VersionLock<T extends Keyed> {
    private final int version;
    private AtomicInteger lockCount = new AtomicInteger(1);
    private Set<VersionedValue<T>> lockedValues;

    protected VersionLock(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    protected boolean incLockCount() {
        lockCount.incrementAndGet();
        return true;
    }

    protected boolean decLockCount() {
        return lockCount.decrementAndGet() == 0;
    }

    protected void lockValue(VersionedValue<T> value) {
        if (lockedValues == null) lockedValues = new HashSet<VersionedValue<T>>();
        lockedValues.add(value);
    }

    public boolean isLocked() {
        return lockCount.get() != 0;
    }

    protected Collection<VersionedValue<T>> getLockedValues() {
        return lockedValues;
    }
}
