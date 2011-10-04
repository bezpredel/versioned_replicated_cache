package com.bezpredel.versioned.datastore.actual;

import com.bezpredel.versioned.datastore.Keyed;

/**
 * NOTE: Entry.next points to a newer version
 *
 * Single object overhead: 80b
 * Each extra version stored: +40b
 *
 * @param <T>
 */
public class VersionedValueImpl<T extends Keyed> implements VersionedValue<T> {
    private final VersionedDataStorage<T> parent; // yeah i know this sux. 4 more bytes per value.

    public VersionedValueImpl(T first, int version, VersionedDataStorage<T> parent) {
        this.parent = parent;
        top = bottom = Entry.createExistingEntry(version, first);
    }

    public void removeSelfFromParent() {
        if(getParent()!=null) {
            getParent().removeDeadMember(this);
        }
    }

    public VersionedDataStorage<T> getParent() {
        return parent;
    }

    // -----------------------------------------------------------------------
    // READING ASPECT
    // -----------------------------------------------------------------------

    private Entry<T> bottom; //for read only

    public T getValue(int version) {
        // get should never read the top.
        Entry<T> e = bottom;

        // e can be null: since reads are not synchronized, this object might end up being read before it is initialized. In this case we do not care for it anyhow because it is expected to contain the data of the wrong version
        if(e == null || e.getVersion() > version) {
            return null;
        } else {
            Entry<T> n = e.getNext();
            while (n != null && n.getVersion() <= version) {
                e = n;
                n = n.getNext();
            }

            assert e.getVersion() <= version;

            return e.getValue();
        }
    }

    // this should be done in a lock that is mutually exclusive with moving latestLocked

    /**
     * @return earliest version left
     */
    public int collapse(int earliestLocked, int latestLocked) {
        // when nothing is locked, expect earliestLocked = MAX and latestLocked = MIN
        if(this.bottom.getVersion() > latestLocked ) {
            this.bottom = this.top;
        } else {
            Entry<T> e = this.bottom;
            Entry<T> top = this.top;

            while (e != top && (/* assert e.getNext()!=null 'cause this means e==top */ e.getNext().getVersion() <= earliestLocked) ) {
                e = e.getNext();
            }
            bottom = e;

            // e now equals to earliest locked node
            // we might be already at the top, if not let's fund the latest locked node
            if(e != top) {
                Entry<T> topLocked = null;
                while(e != top && e.getVersion() <= latestLocked) {
                    topLocked = e;
                    e = e.getNext();
                }

                if(e != top && e.getVersion() > latestLocked ) {
                    assert topLocked!=null;
                    // this means that we found a non-locked entry that is not at the top. All the following entries up to the top can be skipped.
                    topLocked.setNext(top);
                    // NOTE: notice how it is "top", not "this.top".
                    //       "this.top" could have already moved forward while we are running, I do not want to shoot a moving
                    //       target. Let's take care of it
                }
            }
        }

        return bottom.getVersion();
    }

    // -----------------------------------------------------------------------
    // WRITING ASPECT
    // -----------------------------------------------------------------------
    private Entry<T> top; //for write only

    /**
     * To be called from a write lock only!!!
     * @return
     */
    public T getTopValue() {
        return top.getValue();
    }

    public T addNewValue(T t, int version) {
        assert version > getLatestVersion();
        T retVal = top.getValue();
        Entry<T> value =
                t==null
                        ? Entry.<T>createRemovedEntry(version, getKey())
                        : Entry.<T>createExistingEntry(version, t);

        top.setNext(value);
        top = value;
        return retVal;
    }

    // Should be consistent with addNewValue() && should happen-after some earliestLocked update
    public boolean isDead(int earliestLocked) {
        // when nothing is locked, expect earliestLocked = MAX
        return (earliestLocked >= top.getVersion() && top.isADeleteRecord());
    }

    // should be called from a write lock only
    public int getLatestVersion() {
        return top.getVersion();
    }

    public Object getKey() {
        if(top==null) {
            // If we end up here while reading, there is no synchronization around the "top".
            // The reading thread might not see this field set yet. In this case, the reader should not
            // be interested in this version anyhow, so let's return a key that won't match with anything.
            // Hopefully, we don't hit this case ever.
            return new Object();
        } else {
            return top.getKey();
        }
    }

    protected Entry<T> getBottom() {
        return bottom;
    }

    protected Entry<T> getTop() {
        return top;
    }
}
