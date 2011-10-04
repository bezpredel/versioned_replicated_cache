package com.bezpredel.versioned.datastore.actual;

import com.bezpredel.versioned.datastore.Keyed;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class VersionedValueImplTest {
    private static final K o0 = new K(0);
    private static final K o1 = new K(1);
    private static final K o2 = new K(2);
    private static final K o3 = new K(3);
    private static final K o4 = new K(4);
    private static final K o5 = new K(5);
    private static final K o6 = new K(6);
    private static final K o7 = new K(7);
    private static final K o8 = new K(8);
    private static final K o9 = new K(9);

    private VersionedValueImpl<K> vv;

    @Before
    public void setUp() throws Exception {
        vv = new VersionedValueImpl<K>(o0, 0, null);
        vv.addNewValue(o1, 2);
        vv.addNewValue(o2, 4);
        vv.addNewValue(o3, 6);
        vv.addNewValue(o4, 8);
        vv.addNewValue(o5, 10);
        vv.addNewValue(o6, 12);
        vv.addNewValue(o7, 14);
        vv.addNewValue(o8, 16);
        vv.addNewValue(o9, 18);
    }

    @Test
    public void testBasic() throws Exception {
        VersionedValueImpl<K> vv = new VersionedValueImpl<K>(o0, 1, null);
        assertSame(o0, vv.getBottom().getValue());
        assertSame(o0, vv.getTop().getValue());
        assertSame(o0, vv.getTopValue());
        assertSame(1, vv.getLatestVersion());
        assertNull(vv.getValue(0));
        assertSame(o0, vv.getValue(1));
        assertSame(o0, vv.getValue(10));
    }

    @Test
    public void testCollapseWithRedundancy1() throws Exception {
        VersionedValueImpl<K> vv = new VersionedValueImpl<K>(o0, 0, null);
        vv.addNewValue(o1, 2);
        vv.addNewValue(o2, 4);
        vv.addNewValue(o3, 4);
        verifyHasOnlyVersions(vv, o0, o1, o2, o3);
        vv.collapse(0, 2);

        verifyHasOnlyVersions(vv, o0, o1, o3);
    }

    @Test
    public void testCollapseWithRedundancy2() throws Exception {
        VersionedValueImpl<K> vv = new VersionedValueImpl<K>(o0, 0, null);
        vv.addNewValue(o1, 2);
        vv.addNewValue(o2, 2);
        vv.addNewValue(o3, 2);
        vv.addNewValue(o4, 4);
        verifyHasOnlyVersions(vv, o0, o1, o2, o3, o4);
        vv.collapse(2, 2);

        verifyHasOnlyVersions(vv, o3, o4);
    }

    @Test
    public void testCollapseWithRedundancy3() throws Exception {
        VersionedValueImpl<K> vv = new VersionedValueImpl<K>(o0, 0, null);
        vv.addNewValue(o1, 2);
        vv.addNewValue(o2, 2);
        vv.addNewValue(o3, 2);
        vv.addNewValue(o4, 4);
        vv.addNewValue(o5, 6);
        vv.addNewValue(o6, 6);
        vv.addNewValue(o7, 6);
        verifyHasOnlyVersions(vv, o0, o1, o2, o3, o4, o5, o6, o7);
        vv.collapse(2, 4);

        verifyHasOnlyVersions(vv, o3, o4, o7);
    }

    @Test
    public void testCollapse1() throws Exception {
        assertSame(o0, vv.getBottom().getValue());
        assertSame(o9, vv.getTop().getValue());
        assertSame(o9, vv.getTopValue());
        assertSame(18, vv.getLatestVersion());

        assertSame(o0, vv.getValue(0));
        assertSame(o5, vv.getValue(10));
        assertSame(o9, vv.getValue(20));

        vv.collapse(0, 20);

        assertSame(o0, vv.getBottom().getValue());
        assertSame(o9, vv.getTop().getValue());

        vv.collapse(1, 20);
        assertSame(o0, vv.getBottom().getValue());
        assertSame(o9, vv.getTop().getValue());

        vv.collapse(2, 20);
        assertSame(o1, vv.getBottom().getValue());
        assertSame(o9, vv.getTop().getValue());

        vv.collapse(6, 20);
        assertSame(o3, vv.getBottom().getValue());
        assertSame(o9, vv.getTop().getValue());

        // we do not collapse the top version
        vv.collapse(6, 16);
        assertSame(o3, vv.getBottom().getValue());
        assertSame(o9, vv.getTop().getValue());

        verifyHasOnlyVersions(vv, o3, o4, o5, o6, o7, o8, o9);

        vv.collapse(6, 15);
        assertSame(o3, vv.getBottom().getValue());
        assertSame(o9, vv.getTop().getValue());

        verifyHasOnlyVersions(vv, o3, o4, o5, o6, o7, o9);

        vv.collapse(11, 11);
        verifyHasOnlyVersions(vv, o5, o9);

        vv.collapse(Integer.MAX_VALUE, Integer.MIN_VALUE);
        verifyHasOnlyVersions(vv, o9);
    }

    @Test
    public void testCollapseAllAtOnce() throws Exception {
        vv.collapse(Integer.MAX_VALUE, Integer.MIN_VALUE);
        verifyHasOnlyVersions(vv, o9);
    }

    @Test
    public void testCollapse2() throws Exception {
        vv.collapse(18, 18);
        verifyHasOnlyVersions(vv, o9);
    }

    @Test
    public void testCollapse3() throws Exception {
        vv.collapse(18, 50);
        verifyHasOnlyVersions(vv, o9);
    }

    @Test
    public void testCollapse4() throws Exception {
        vv.collapse(25, 50);
        verifyHasOnlyVersions(vv, o9);
    }

    @Test
    public void testCollapse5() throws Exception {
        vv.collapse(4, 8);
        verifyHasOnlyVersions(vv, o2, o3, o4, o9);
    }

    @Test
    public void testCollapse6() throws Exception {
        vv.collapse(16, 16);
        verifyHasOnlyVersions(vv, o8, o9);
    }


    @Test
    public void testCollapse7() throws Exception {
        vv.collapse(14, 14);
        verifyHasOnlyVersions(vv, o7, o9);
    }

    private void verifyHasOnlyVersions(VersionedValueImpl<K> vv, K... versions) {
        Entry<K> curr = vv.getBottom();
        for(K ver : versions) {
            assertNotNull(curr);
            assertSame(ver, curr.getValue());
            curr = curr.getNext();
        }
        assertNull(curr);
    }

    @Test
    public void testIsDead() throws Exception {
        //todo
    }

    private static class K implements Keyed {
        private final Integer key;
        private final int id;

        public K(int id) {
            this.key = 0;
            this.id = id;
        }

        public Integer getKey() {
            return key;
        }

        public String toString() {
            return "K#" + id;
        }
    }


}
