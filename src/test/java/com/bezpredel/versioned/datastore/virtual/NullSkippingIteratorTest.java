package com.bezpredel.versioned.datastore.virtual;

import com.bezpredel.versioned.datastore.Keyed;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import static com.bezpredel.versioned.datastore.virtual.VirtualData.NULL;

public class NullSkippingIteratorTest {
    @Test
    public void testAll() throws Exception {
        List<Keyed> list = Arrays.asList(
                NULL,
                new T(1),
                new T(2),
                NULL,
                NULL,
                new T(3),
                new T(4),
                NULL,
                new T(5),
                NULL,
                NULL,
                NULL
        );

        NullSkippingIterator iter = new NullSkippingIterator(list.iterator());

        assertTrue(iter.hasNext());
        assertEquals(1, iter.next().getKey());
        assertTrue(iter.hasNext());
        assertEquals(2, iter.next().getKey());
        assertTrue(iter.hasNext());
        assertEquals(3, iter.next().getKey());
        assertTrue(iter.hasNext());
        assertEquals(4, iter.next().getKey());
        assertTrue(iter.hasNext());
        assertEquals(5, iter.next().getKey());
        assertFalse(iter.hasNext());
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
