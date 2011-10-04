package com.bezpredel;

import org.junit.Assert;
import com.bezpredel.versioned.datastore.Keyed;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

public class TestUtils {
    public static <D extends Keyed> void validateCollectionContents(Iterator<D> iter, D... expected) {
        HashSet<D> expectedSet = new HashSet<D>(Arrays.asList(expected));
        while(iter.hasNext()) {
            D d = iter.next();
            boolean res = expectedSet.remove(d);
            Assert.assertTrue(d.toString() + " is not expected in the set", res);
        }

        Assert.assertTrue("Values expected but not found: " + expectedSet, expectedSet.isEmpty());
    }
}
