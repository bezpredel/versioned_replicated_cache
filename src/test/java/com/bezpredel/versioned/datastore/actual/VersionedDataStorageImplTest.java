package com.bezpredel.versioned.datastore.actual;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.bezpredel.versioned.datastore.Keyed;

import java.util.Iterator;

public class VersionedDataStorageImplTest {
    private CV currentVersion;
    private LockRecordStore<D> lockRecordStore;
    private VersionedDataStorage<D> vsd;

    @Before
    public void setup() throws Exception {
        currentVersion = new CV();
        Sync sync = new Sync(){
            public boolean assertInSharedLock() {
                return false;
            }

            public boolean assertInExclusiveWriteLock() {
                return false;
            }

            public boolean isInWriteLock() {
                return false;
            }
        };

        lockRecordStore = new LockRecordStore<D>(sync);

        vsd = new VersionedDataStorageImpl<D>(
                lockRecordStore
        );

    }

    private VersionLock<D> lockCurrentVersion() {
        return lockRecordStore.lockVersion(currentVersion.get());
    }

    @Test
    public void testIterator() throws Exception {

        D d1v1 = new D(1, 1);
        D d2v1 = new D(2, 1);
        D d3v1 = new D(3, 1);
        D d4v1 = new D(4, 1);
        D d1v2 = new D(1, 2);
        D d2v2 = new D(2, 2);
        D d4v2 = new D(4, 2);

        currentVersion.incVersion();
        vsd.put(d1v1, currentVersion.get());
        vsd.put(d2v1, currentVersion.get());
        vsd.put(d3v1, currentVersion.get());
        vsd.put(d4v1, currentVersion.get());

        VersionLock<D> lockRec1 = lockCurrentVersion();

        Iterator<D> iter = vsd.values(lockRec1.getVersion(), false);
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d1v1, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d2v1, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d3v1, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d4v1, iter.next());
        Assert.assertFalse(iter.hasNext());

        currentVersion.incVersion();
        vsd.put(d1v2, currentVersion.get());
        vsd.put(d2v2, currentVersion.get());
        vsd.remove(3, currentVersion.get());
        vsd.put(d4v2, currentVersion.get());

        iter = vsd.values(lockRec1.getVersion(), false);
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d1v1, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d2v1, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d3v1, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d4v1, iter.next());
        Assert.assertFalse(iter.hasNext());

        iter = vsd.values(currentVersion.get(), false);
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d1v2, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d2v2, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d4v2, iter.next());
        Assert.assertFalse(iter.hasNext());

        if(lockRecordStore.unlockStep1(lockRec1)) {
            lockRecordStore.unlockStep2(lockRec1);
        }

        iter = vsd.values(currentVersion.get(), false);
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d1v2, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d2v2, iter.next());
        Assert.assertTrue(iter.hasNext());
        Assert.assertSame(d4v2, iter.next());
        Assert.assertFalse(iter.hasNext());

    }

    @Test
    public void testBasic() throws Exception {

        D d1v1 = new D(1, 1);
        D d2v1 = new D(2, 1);
        D d3v1 = new D(3, 1);
        D d1v2 = new D(1, 2);
        D d2v2 = new D(2, 2);
        D d1v3 = new D(1, 3);
        D d2v3 = new D(2, 3);

        currentVersion.incVersion();
        vsd.put(d1v1, currentVersion.get());
        vsd.put(d2v1, currentVersion.get());

        VersionLock<D> lockRec1 = lockCurrentVersion();

        Assert.assertSame(d1v1, vsd.get(1, lockRec1.getVersion()));
        Assert.assertSame(d2v1, vsd.get(2, lockRec1.getVersion()));
        Assert.assertNull(vsd.get(3, lockRec1.getVersion()));

        currentVersion.incVersion();
        vsd.put(d1v2, currentVersion.get());
        vsd.put(d2v2, currentVersion.get());
        vsd.put(d3v1, currentVersion.get());

        Assert.assertSame(d1v1, vsd.get(1, lockRec1.getVersion()));
        Assert.assertSame(d2v1, vsd.get(2, lockRec1.getVersion()));
        Assert.assertNull(vsd.get(3, lockRec1.getVersion()));

        VersionLock<D> lockRec2 = lockCurrentVersion();

        Assert.assertSame(d1v1, vsd.get(1, lockRec1.getVersion()));
        Assert.assertSame(d2v1, vsd.get(2, lockRec1.getVersion()));
        Assert.assertNull(vsd.get(3, lockRec1.getVersion()));

        Assert.assertSame(d1v2, vsd.get(1, lockRec2.getVersion()));
        Assert.assertSame(d2v2, vsd.get(2, lockRec2.getVersion()));
        Assert.assertSame(d3v1, vsd.get(3, lockRec2.getVersion()));

        currentVersion.incVersion();
        vsd.remove(1, currentVersion.get());
        vsd.remove(2, currentVersion.get());

        Assert.assertSame(d1v1, vsd.get(1, lockRec1.getVersion()));
        Assert.assertSame(d2v1, vsd.get(2, lockRec1.getVersion()));
        Assert.assertNull(vsd.get(3, lockRec1.getVersion()));

        Assert.assertSame(d1v2, vsd.get(1, lockRec2.getVersion()));
        Assert.assertSame(d2v2, vsd.get(2, lockRec2.getVersion()));
        Assert.assertSame(d3v1, vsd.get(3, lockRec2.getVersion()));

        Assert.assertNull(vsd.get(1, currentVersion.get()));
        Assert.assertNull(vsd.get(2, currentVersion.get()));
        Assert.assertSame(d3v1, vsd.get(3, currentVersion.get()));

        VersionLock<D> lockRec3 = lockCurrentVersion();

        Assert.assertSame(d1v1, vsd.get(1, lockRec1.getVersion()));
        Assert.assertSame(d2v1, vsd.get(2, lockRec1.getVersion()));
        Assert.assertNull(vsd.get(3, lockRec1.getVersion()));

        Assert.assertSame(d1v2, vsd.get(1, lockRec2.getVersion()));
        Assert.assertSame(d2v2, vsd.get(2, lockRec2.getVersion()));
        Assert.assertSame(d3v1, vsd.get(3, lockRec2.getVersion()));

        Assert.assertNull(vsd.get(1, lockRec3.getVersion()));
        Assert.assertNull(vsd.get(2, lockRec3.getVersion()));
        Assert.assertSame(d3v1, vsd.get(3, lockRec3.getVersion()));

        currentVersion.incVersion();
        vsd.put(d1v3, currentVersion.get());

        Assert.assertSame(d1v1, vsd.get(1, lockRec1.getVersion()));
        Assert.assertSame(d1v2, vsd.get(1, lockRec2.getVersion()));
        Assert.assertNull(vsd.get(1, lockRec3.getVersion()));
        Assert.assertSame(d1v3, vsd.get(1, currentVersion.get()));

        if(lockRecordStore.unlockStep1(lockRec1)) {
            lockRecordStore.unlockStep2(lockRec1);
        }
        Assert.assertNull(vsd.get(1, lockRec1.getVersion()));

        if(lockRecordStore.unlockStep1(lockRec3)) {
            lockRecordStore.unlockStep2(lockRec3);
        }
        Assert.assertSame(d1v2, vsd.get(1, lockRec2.getVersion()));

        if(lockRecordStore.unlockStep1(lockRec2)) {
            lockRecordStore.unlockStep2(lockRec2);
        }


    }


    private static class CV {
        private int version = 0;

        public void incVersion() {
            version++;
        }
        public int get() {
            return version;
        }
    }

    private static class D implements Keyed {
        private final Integer key;
        private final int version;

        private D(Integer key, int version) {
            this.key = key;
            this.version = version;
        }

        public Object getKey() {
            return key;
        }

        public String toString() {
            return key + "v" + version;
        }
    }
}
