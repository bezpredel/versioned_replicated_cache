package com.bezpredel.versioned.datastore.actual;

import com.bezpredel.TestUtils;
import org.junit.Before;
import org.junit.Test;
import com.bezpredel.versioned.datastore.Keyed;

public class OneToManyIndexStorageImplTest {
    private CV currentVersion;
    private LockRecordStore<D> lockRecordStore;
    private OneToManyIndexStorage<D> indexStorage;

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

        indexStorage = new OneToManyIndexStorageImpl<D>(lockRecordStore);

    }

    private VersionLock<D> lockCurrentVersion() {
        return lockRecordStore.lockVersion(currentVersion.get());
    }

    @Test
    public void test() throws Exception {
        D x1_a_0 = new D(1, 'a', 0);
        D x2_a_0 = new D(2, 'a', 0);
        D x3_a_0 = new D(3, 'a', 0);
        D x3_a_1 = new D(3, 'a', 1);
        D x4_a_1 = new D(4, 'a', 1);

        D x1_a_2 = new D(1, 'a', 2);

        D x4_b_0 = new D(4, 'b', 0);
        D x5_b_0 = new D(5, 'b', 0);
        D x6_b_0 = new D(6, 'b', 0);

        D x1_b_1 = new D(1, 'b', 1);

        addToStorage(x1_a_0);
        validateLeafContents('a', x1_a_0);
        addToStorage(x2_a_0);
        validateLeafContents('a', x1_a_0, x2_a_0);
        addToStorage(x4_b_0);
        validateLeafContents('a', x1_a_0, x2_a_0);
        validateLeafContents('b', x4_b_0);
        addToStorage(x3_a_0);
        validateLeafContents('a', x1_a_0, x2_a_0, x3_a_0);
        validateLeafContents('b', x4_b_0);
        addToStorage(x5_b_0);
        validateLeafContents('a', x1_a_0, x2_a_0, x3_a_0);
        validateLeafContents('b', x4_b_0, x5_b_0);
        addToStorage(x6_b_0);
        validateLeafContents('a', x1_a_0, x2_a_0, x3_a_0);
        validateLeafContents('b', x4_b_0, x5_b_0, x6_b_0);

        VersionLock<D> lockRecord = lockCurrentVersion();
        validateLeafContents(lockRecord.getVersion(), 'a', x1_a_0, x2_a_0, x3_a_0);
        validateLeafContents(lockRecord.getVersion(), 'b', x4_b_0, x5_b_0, x6_b_0);

        if(lockRecordStore.unlockStep1(lockRecord)) {
            lockRecordStore.unlockStep2(lockRecord);
        }
        lockRecord = lockCurrentVersion();

        validateLeafContents(lockRecord.getVersion(), 'a', x1_a_0, x2_a_0, x3_a_0);
        validateLeafContents(lockRecord.getVersion(), 'b', x4_b_0, x5_b_0, x6_b_0);

        currentVersion.incVersion();

        validateLeafContents(lockRecord.getVersion(), 'a', x1_a_0, x2_a_0, x3_a_0);
        validateLeafContents(lockRecord.getVersion(), 'b', x4_b_0, x5_b_0, x6_b_0);

        removeFromStorage(x1_a_0);

        validateLeafContents(lockRecord.getVersion(), 'a', x1_a_0, x2_a_0, x3_a_0);
        validateLeafContents(lockRecord.getVersion(), 'b', x4_b_0, x5_b_0, x6_b_0);

        validateLeafContents(currentVersion.get(), 'a', x2_a_0, x3_a_0);
        validateLeafContents(currentVersion.get(), 'b', x4_b_0, x5_b_0, x6_b_0);

        addToStorage(x3_a_1);
        addToStorage(x4_a_1);

        validateLeafContents(lockRecord.getVersion(), 'a', x1_a_0, x2_a_0, x3_a_0);
        validateLeafContents(lockRecord.getVersion(), 'b', x4_b_0, x5_b_0, x6_b_0);

        validateLeafContents(currentVersion.get(), 'a', x2_a_0, x3_a_1, x4_a_1);
        validateLeafContents(currentVersion.get(), 'b', x4_b_0, x5_b_0, x6_b_0);

        addToStorage(x1_b_1);
        validateLeafContents(lockRecord.getVersion(), 'a', x1_a_0, x2_a_0, x3_a_0);
        validateLeafContents(lockRecord.getVersion(), 'b', x4_b_0, x5_b_0, x6_b_0);

        validateLeafContents(currentVersion.get(), 'a', x2_a_0, x3_a_1, x4_a_1);
        validateLeafContents(currentVersion.get(), 'b', x4_b_0, x5_b_0, x6_b_0, x1_b_1);
        currentVersion.incVersion();

        removeFromStorage(x2_a_0);
        removeFromStorage(x3_a_1);
        removeFromStorage(x4_a_1);
        removeFromStorage(x4_b_0);
        removeFromStorage(x5_b_0);
        removeFromStorage(x6_b_0);
        removeFromStorage(x1_b_1);

        validateLeafContents(lockRecord.getVersion(), 'a', x1_a_0, x2_a_0, x3_a_0);
        validateLeafContents(lockRecord.getVersion(), 'b', x4_b_0, x5_b_0, x6_b_0);

        validateLeafContents(currentVersion.get(), 'a');
        validateLeafContents(currentVersion.get(), 'b');

        VersionLock<D> lockRecord2 = lockCurrentVersion();
        currentVersion.incVersion();
        if(lockRecordStore.unlockStep1(lockRecord)) {
            lockRecordStore.unlockStep2(lockRecord);
        }


        validateLeafContents(lockRecord2.getVersion(), 'a');
        validateLeafContents(lockRecord2.getVersion(), 'b');

        addToStorage(x1_a_2);

        validateLeafContents(lockRecord2.getVersion(), 'a');
        validateLeafContents(lockRecord2.getVersion(), 'b');

        validateLeafContents(currentVersion.get(), 'a', x1_a_2);
        validateLeafContents(currentVersion.get(), 'b');

        if(lockRecordStore.unlockStep1(lockRecord2)) {
            lockRecordStore.unlockStep2(lockRecord2);
        }

        validateLeafContents(currentVersion.get(), 'a', x1_a_2);
        validateLeafContents(currentVersion.get(), 'b');
    }

    private void addToStorage(D a) {
        indexStorage.add(a.leafKey, a, currentVersion.get());
    }

    private void removeFromStorage(D a) {
        indexStorage.remove(a.leafKey, a.key, currentVersion.get());
    }

    private void validateLeafContents(Character leafKey, D ... expected) throws Exception {
        validateLeafContents(currentVersion.get(), leafKey, expected);
    }

    private void validateLeafContents(int version, Character leafKey, D ... expected) throws Exception {
        TestUtils.<D>validateCollectionContents(
                indexStorage.get(leafKey, version, false),
                expected
        );
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
        private final Character leafKey;
        private final int version;

        private D(Integer key, Character leafKey, int version) {
            this.key = key;
            this.version = version;
            this.leafKey = leafKey;
        }

        public Object getKey() {
            return key;
        }

        public String toString() {
            return key + "v" + version;
        }
    }
}
