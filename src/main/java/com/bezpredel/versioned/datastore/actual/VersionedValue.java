package com.bezpredel.versioned.datastore.actual;

import com.bezpredel.versioned.datastore.Keyed;

public interface VersionedValue<T extends Keyed> extends Keyed {
    /**
     * @return null if doesn't exist
     */
    T getValue(int version);

    /**
     * To be called from write lock only!!
     * @return
     */
    T getTopValue();

    /**
     * @return previousValue
     */
    T addNewValue(T t, int version);

    // this should be done in a lock that is mutually exclusive with moving latestLocked
    int collapse(int earliestLocked, int latestLocked);

    // Should be consistent with addNewValue() && should happen-after some earliestLocked update
    boolean isDead(int earliestLocked);

    // should be called from a write lock only
    int getLatestVersion();

    void removeSelfFromParent();
}
