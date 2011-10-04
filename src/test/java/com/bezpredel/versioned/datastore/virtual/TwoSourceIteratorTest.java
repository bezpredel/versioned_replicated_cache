package com.bezpredel.versioned.datastore.virtual;

import com.bezpredel.TestUtils;
import com.bezpredel.versioned.datastore.Keyed;
import org.junit.Before;
import org.junit.Test;
import static com.bezpredel.versioned.datastore.virtual.VirtualData.NULL;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TwoSourceIteratorTest {
    private final T t0_0 = new T(0);
    private final T t0_1 = new T(0);
    private final T t1 = new T(1);
    private final T t2 = new T(2);
    private final T t3 = new T(3);
    private final T t4 = new T(4);
    private final T t5 = new T(5);
    private final T t6 = new T(6);
    private final T t7 = new T(7);
    private final T t8 = new T(8);
    private final T t9 = new T(9);

    private Set<Keyed> underlying;

    @Before
    public void setUp() throws Exception {
        underlying = new HashSet<Keyed>();
        underlying.add(t0_0);
        underlying.add(t1);
        underlying.add(t2);
        underlying.add(t3);
        underlying.add(t4);
    }

    @Test
    public void testOne() throws Exception {
        Map<Object, Keyed> map = new HashMap<Object, Keyed>();
        TwoSourceIterator<T> iter = new TwoSourceIterator<T>(underlying.iterator(), map);

        TestUtils.validateCollectionContents(
            iter,
            t0_0, t1, t2, t3, t4
        );
    }

    @Test
    public void testTwo() throws Exception {
        Map<Object, Keyed> map = new HashMap<Object, Keyed>();
        map.put(0, NULL);
        map.put(1, NULL);
        map.put(2, NULL);
        map.put(3, NULL);
        map.put(4, NULL);

        TwoSourceIterator<T> iter = new TwoSourceIterator<T>(underlying.iterator(), map);

        TestUtils.validateCollectionContents(
            iter
        );
    }

    @Test
    public void testThree() throws Exception {
        Map<Object, Keyed> map = new HashMap<Object, Keyed>();
        map.put(0, t0_1);
        map.put(1, NULL);
        map.put(2, NULL);
        map.put(3, NULL);
        map.put(5, t5);
        map.put(6, t6);
        map.put(7, t7);


        TwoSourceIterator<T> iter = new TwoSourceIterator<T>(underlying.iterator(), map);

        TestUtils.validateCollectionContents(
            iter,
            t0_1, t4, t5, t6, t7
        );
    }

    private static class T implements Keyed {
        private final Integer key;

        public T(Integer key) {
            this.key = key;
        }

        public Integer getKey() {
            return key;
        }
    }
}
