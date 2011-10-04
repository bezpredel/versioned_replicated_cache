package com.bezpredel.collections;

import junit.framework.Assert;
import org.junit.Test;
import com.bezpredel.versioned.datastore.Keyed;

import java.util.*;

public class AlmostHashMapTest {
    @Test
    public void testPut() throws Exception {
        AlmostHashMap<Keyed> m = new AlmostHashMap<Keyed>(4, 1, 1);

        K k1 = new K(0);
        K k2 = new K(1);
        K k3 = new K(2);

        K k1_1 = new K(0);
        K k2_1 = new K(1);
        K k3_1 = new K(2);

        K k1_2 = new K(0);
        K k2_2 = new K(1);
        K k3_2 = new K(2);

        K_NULL null_1 = new K_NULL();
        K_NULL null_2 = new K_NULL();

        Assert.assertFalse(m.contains(k1));
        Assert.assertFalse(m.contains(k2));
        Assert.assertFalse(m.contains(k3));
        Assert.assertFalse(m.contains(k1_1));
        Assert.assertFalse(m.contains(k2_1));
        Assert.assertFalse(m.contains(k3_1));
        Assert.assertFalse(m.contains(k1_2));
        Assert.assertFalse(m.contains(k2_2));
        Assert.assertFalse(m.contains(k3_2));
        Assert.assertFalse(m.contains(null_1));
        Assert.assertFalse(m.contains(null_2));


        m.put(k1);

        Assert.assertTrue(m.contains(k1));
        Assert.assertFalse(m.contains(k2));
        Assert.assertFalse(m.contains(k3));
        Assert.assertFalse(m.contains(k1_1));
        Assert.assertFalse(m.contains(k2_1));
        Assert.assertFalse(m.contains(k3_1));
        Assert.assertFalse(m.contains(k1_2));
        Assert.assertFalse(m.contains(k2_2));
        Assert.assertFalse(m.contains(k3_2));
        Assert.assertFalse(m.contains(null_1));
        Assert.assertFalse(m.contains(null_2));

        m.put(k1_1);

        Assert.assertTrue(m.contains(k1));
        Assert.assertFalse(m.contains(k2));
        Assert.assertFalse(m.contains(k3));
        Assert.assertTrue(m.contains(k1_1));
        Assert.assertFalse(m.contains(k2_1));
        Assert.assertFalse(m.contains(k3_1));
        Assert.assertFalse(m.contains(k1_2));
        Assert.assertFalse(m.contains(k2_2));
        Assert.assertFalse(m.contains(k3_2));
        Assert.assertFalse(m.contains(null_1));
        Assert.assertFalse(m.contains(null_2));

        m.put(k2);
        m.put(k3);

        Assert.assertTrue(m.contains(k1));
        Assert.assertTrue(m.contains(k2));
        Assert.assertTrue(m.contains(k3));
        Assert.assertTrue(m.contains(k1_1));
        Assert.assertFalse(m.contains(k2_1));
        Assert.assertFalse(m.contains(k3_1));
        Assert.assertFalse(m.contains(k1_2));
        Assert.assertFalse(m.contains(k2_2));
        Assert.assertFalse(m.contains(k3_2));
        Assert.assertFalse(m.contains(null_1));
        Assert.assertFalse(m.contains(null_2));

        //verify double puts noop
        m.put(k2);
        m.put(k3);
        m.put(k1_1);

        Assert.assertTrue(m.contains(k1));
        Assert.assertTrue(m.contains(k2));
        Assert.assertTrue(m.contains(k3));
        Assert.assertTrue(m.contains(k1_1));
        Assert.assertFalse(m.contains(k2_1));
        Assert.assertFalse(m.contains(k3_1));
        Assert.assertFalse(m.contains(k1_2));
        Assert.assertFalse(m.contains(k2_2));
        Assert.assertFalse(m.contains(k3_2));
        Assert.assertFalse(m.contains(null_1));
        Assert.assertFalse(m.contains(null_2));

        m.put(k2_1);
        m.put(k3_1);
        m.put(k1_2);
        m.put(k2_2);
        m.put(k3_2);


        Assert.assertTrue(m.contains(k1));
        Assert.assertTrue(m.contains(k2));
        Assert.assertTrue(m.contains(k3));
        Assert.assertTrue(m.contains(k1_1));
        Assert.assertTrue(m.contains(k2_1));
        Assert.assertTrue(m.contains(k3_1));
        Assert.assertTrue(m.contains(k1_2));
        Assert.assertTrue(m.contains(k2_2));
        Assert.assertTrue(m.contains(k3_2));
        Assert.assertFalse(m.contains(null_1));
        Assert.assertFalse(m.contains(null_2));

        m.put(null_1);
        Assert.assertTrue(m.contains(null_1));
        Assert.assertTrue(m.contains(null_2));
    }


    @Test
    public void testRemove() throws Exception {
        AlmostHashMap<Keyed> m = new AlmostHashMap<Keyed>(4, 1, 0.5);

        HashSet<Keyed> in = new HashSet<Keyed>();
        HashSet<Keyed> out = new HashSet<Keyed>();

        K k1 = new K(0);
        K k2 = new K(1);
        K k3 = new K(2);
        K k4 = new K(3);
        K k5 = new K(4);
        K k6 = new K(5);
        K k7 = new K(6);
        K k8 = new K(7);
        K k9 = new K(8);

        K k1_1 = new K(0);
        K k2_1 = new K(1);
        K k3_1 = new K(2);

        K k1_2 = new K(0);
        K k2_2 = new K(1);
        K k3_2 = new K(2);

        K_NULL null_1 = new K_NULL();

        m.put(k1); in.add(k1);
        m.put(k2); in.add(k2);
        m.put(k3); in.add(k3);
        m.put(k4); in.add(k4);
        m.put(k5); in.add(k5);
        m.put(k6); in.add(k6);
        m.put(k7); in.add(k7);
        m.put(k8); in.add(k8);
        m.put(k9); in.add(k9);
        m.put(k1_1); in.add(k1_1);
        m.put(k2_1); in.add(k2_1);
        m.put(k3_1); in.add(k3_1);
        m.put(k1_2); in.add(k1_2);
        m.put(k2_2); in.add(k2_2);
        m.put(k3_2); in.add(k3_2);
        m.put(null_1); in.add(null_1);

        for(Keyed o : in) {
            Assert.assertTrue(m.contains(o));
        }

        for(Keyed o : out) {
            Assert.assertFalse(m.contains(o));
        }

        boolean result = m.removeObject(k1); in.remove(k1); out.add(k1);

        Assert.assertTrue(result);

        for(Keyed o : in) {
            Assert.assertTrue(m.contains(o));
        }

        for(Keyed o : out) {
            Assert.assertFalse(m.contains(o));
        }

        result = m.removeObject(k1);

        Assert.assertFalse(result);

        while(!in.isEmpty()) {
            Keyed next = in.iterator().next();

            result = m.removeObject(next); in.remove(next); out.add(next);

            Assert.assertTrue(result);

            for(Keyed o : in) {
                Assert.assertTrue(m.contains(o));
            }

            for(Keyed o : out) {
                Assert.assertFalse(m.contains(o));
            }

            result = m.removeObject(next);

            Assert.assertFalse(result);
        }

        // random test

        List<Keyed> l = new ArrayList<Keyed>(in);
        l.addAll(out);
        Random r = new Random();

        long t0 = System.currentTimeMillis();
        final int len = 100000;
        for(int i=0; i<len; i++) {
            int ind = r.nextInt(l.size());
            Keyed next = l.get(ind);
            if(in.contains(next)) {
                result = m.removeObject(next); in.remove(next); out.add(next);
                Assert.assertTrue(result);
            } else {
                m.put(next);out.remove(next);in.add(next);
            }

            for(Keyed o : in) {
                Assert.assertTrue(m.contains(o));
            }

            for(Keyed o : out) {
                Assert.assertFalse(m.contains(o));
            }
        }
        long t1 = System.currentTimeMillis();
        System.out.println("Random test for " + len + " took " + (t1-t0));
    }



    private static class K implements Keyed {
        private final int hash;

        public K(int hash) {
            this.hash = hash;
        }

        public Object getKey() {
            return this;
        }

        public int hashCode() {
            return hash;
        }

        public boolean equals(Object o) {
            return super.equals(o);
        }
    }

    private static class K_NULL implements Keyed {
        public Object getKey() {
            return null;
        }
    }
}
