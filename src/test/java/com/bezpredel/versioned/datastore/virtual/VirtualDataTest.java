package com.bezpredel.versioned.datastore.virtual;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import com.bezpredel.versioned.datastore.Keyed;


import java.util.Map;

public class VirtualDataTest {
    private VirtualData<Object, Object> vd;
    private final Object DATA_K1 = new Object();
    private final Object DATA_K2 = new Object();

    private final Object INDEX_K1 = new Object();
    private final Object INDEX_K2 = new Object();

    private final Object indexLeafK1 = "leaf key #1";
    private final Object indexLeafK2 = "leaf key #2";
    private final Object indexLeafK3 = "leaf key #2";

    private final Integer key1 = 1;
    private final Integer key2 = 2;
    private final Integer key3 = 3;
    private final Integer key4 = 4;
    private final Integer key5 = 5;

    private final K k1 = new K(key1);
    private final K k1_1 = new K(key1);
    private final K k2 = new K(key2);
    private final K k3 = new K(key3);
    private final K k4 = new K(key4);
    private final K k5 = new K(key5);


    @Before
    public void setUp() throws Exception {
        vd = new VirtualData<Object, Object>();
    }

    @Test
    public void testNoExceptionOnEmptyData() throws Exception {
        assertFalse(vd._containsKey(DATA_K1, key1));
        assertNull(vd._getValue(DATA_K1, key1));
        Map<Object, Keyed> vals = vd._getValues(DATA_K1);
        assertNotNull(vals);
        assertTrue(vals.isEmpty());
        vals = vd._getValuesByIndex(INDEX_K1, indexLeafK1);
        assertNotNull(vals);
        assertTrue(vals.isEmpty());
    }

    @Test
    public void testPutRemove() throws Exception {
        Keyed prev = vd._put(DATA_K1, k1);
        assertNull(prev);
        
        prev = vd._put(DATA_K1, k2);
        assertNull(prev);
        
        prev = vd._put(DATA_K1, k1_1);
        assertSame(k1, prev);
        
        vd._put(DATA_K2, k3);
        vd._put(DATA_K2, k4);
        
        assertTrue(vd._containsKey(DATA_K1, key1));
        assertTrue(vd._containsKey(DATA_K1, key2));
        assertFalse(vd._containsKey(DATA_K1, key3));
        assertFalse(vd._containsKey(DATA_K1, key4));
        assertFalse(vd._containsKey(DATA_K1, key5));
        
        assertFalse(vd._containsKey(DATA_K2, key1));
        assertFalse(vd._containsKey(DATA_K2, key2));
        assertTrue(vd._containsKey(DATA_K2, key3));
        assertTrue(vd._containsKey(DATA_K2, key4));
        assertFalse(vd._containsKey(DATA_K2, key5));

        prev = vd._remove(DATA_K1, key1);
        assertSame(k1_1, prev);
        assertTrue(vd._containsKey(DATA_K1, key1)); // YES, TRUE AFTER REMOVE

        assertSame(VirtualData.NULL, vd._getValue(DATA_K1, key1));
        prev = vd._put(DATA_K1, k1);
        assertSame(VirtualData.NULL, prev);
        assertTrue(vd._containsKey(DATA_K1, key1));
        assertSame(k1, vd._getValue(DATA_K1, key1));
    }

    @Test
    public void testIndexAddRemove() throws Exception {
        vd._putIntoIndex(INDEX_K1, indexLeafK1, k1);
        vd._putIntoIndex(INDEX_K1, indexLeafK1, k2);
        vd._putIntoIndex(INDEX_K1, indexLeafK2, k3);
        vd._putIntoIndex(INDEX_K1, indexLeafK2, k4);
        vd._putIntoIndex(INDEX_K1, indexLeafK2, k5);

        Map<Object, Keyed> leaf1 = vd._getValuesByIndex(INDEX_K1, indexLeafK1);
        assertEquals(2, leaf1.size());
        assertSame(k1, leaf1.get(key1));
        assertSame(k2, leaf1.get(key2));

        Map<Object, Keyed> leaf2 = vd._getValuesByIndex(INDEX_K1, indexLeafK2);
        assertEquals(3, leaf2.size());
        assertSame(k3, leaf2.get(key3));
        assertSame(k4, leaf2.get(key4));
        assertSame(k5, leaf2.get(key5));

        vd._removeFromIndex(INDEX_K1, indexLeafK2, key3);
        assertEquals(3, leaf2.size());
        assertSame(VirtualData.NULL, leaf2.get(key3));
        assertSame(k4, leaf2.get(key4));
        assertSame(k5, leaf2.get(key5));
        vd._putIntoIndex(INDEX_K1, indexLeafK2, k3);
        assertSame(k3, leaf2.get(key3));

    }

    private static class K implements Keyed {
        private final Integer key;

        public K(Integer key) {
            this.key = key;
        }

        public Integer getKey() {
            return key;
        }

        public String toString() {
            return "K#"+key;
        }
    }

}
