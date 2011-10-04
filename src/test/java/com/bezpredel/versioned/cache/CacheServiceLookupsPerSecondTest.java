package com.bezpredel.versioned.cache;

import com.bezpredel.collections.FieldGetterFunction;
import com.bezpredel.versioned.cache.def.CacheSpec;
import com.bezpredel.versioned.cache.def.IndexSpec;
import com.bezpredel.versioned.datastore.Keyed;
import com.bezpredel.versioned.datastore.actual.StorageSystemImpl;
import org.junit.Before;
import org.junit.Test;

import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import static com.bezpredel.TestUtils.validateCollectionContents;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class CacheServiceLookupsPerSecondTest {
    private final long timeToRun = 5000;

    private CacheService cacheService;
    private Object writeLock;
    private Integer[] keys;
    private Integer[] moreKeys;
    private StorageSystemImpl storageSystem;


    @Test
    public void testCheapestMissLookups() throws Exception {
        cacheService.executeWrite(
                new CacheService.WriteCommand() {
                    public void execute(CacheService.WriteContext context) {
                        for(Integer key : keys) {
                            context.put(new D(key, 0));
                        }
                    }
                }
        );

        testMissLookups(timeToRun, "testCheapestMissLookups()");
    }

    @Test
    public void testCheapestHitLookups() throws Exception {
        cacheService.executeWrite(
                new CacheService.WriteCommand() {
                    public void execute(CacheService.WriteContext context) {
                        for(Integer key : keys) {
                            context.put(new D(key, 0));
                        }
                    }
                }
        );


        testHitLookups(timeToRun, "testCheapestHitLookups()");
    }

    @Test
    public void test2VHitLookups() throws Exception {
        cacheService.executeWrite(
                new CacheService.WriteCommand() {
                    public void execute(CacheService.WriteContext context) {
                        for(Integer key : keys) {
                            context.put(new D(key, 0));
                        }
                    }
                }
        );
        storageSystem.__explicitLock();
        cacheService.executeWrite(
                new CacheService.WriteCommand() {
                    public void execute(CacheService.WriteContext context) {
                        for(Integer key : keys) {
                            context.put(new D(key, 1));
                        }
                    }
                }
        );

        testHitLookups(timeToRun, "test2VHitLookups()");
    }

    @Test
    public void test50VHitLookups() throws Exception {
        for(int i=0; i<50; i++) {
            final int j = i;
            cacheService.executeWrite(
                    new CacheService.WriteCommand() {
                        public void execute(CacheService.WriteContext context) {
                            for(Integer key : keys) {
                                context.put(new D(key, j));
                            }
                        }
                    }
            );
            storageSystem.__explicitLock();
        }

        testHitLookups(timeToRun, "test50VHitLookups()");
    }

    @Test
    public void test3VHitLookups() throws Exception {
        cacheService.executeWrite(
                new CacheService.WriteCommand() {
                    public void execute(CacheService.WriteContext context) {
                        for(Integer key : keys) {
                            context.put(new D(key, 0));
                        }
                    }
                }
        );
        storageSystem.__explicitLock();

        cacheService.executeWrite(
                new CacheService.WriteCommand() {
                    public void execute(CacheService.WriteContext context) {
                        for(Integer key : keys) {
                            context.put(new D(key, 1));
                        }
                    }
                }
        );
        storageSystem.__explicitLock();

        cacheService.executeWrite(
                new CacheService.WriteCommand() {
                    public void execute(CacheService.WriteContext context) {
                        for(Integer key : keys) {
                            context.put(new D(key, 2));
                        }
                    }
                }
        );

        testHitLookups(timeToRun, "test3VHitLookups()");
    }

    @Test
    public void test3VMissLookups() throws Exception {
        cacheService.executeWrite(
                new CacheService.WriteCommand() {
                    public void execute(CacheService.WriteContext context) {
                        for(Integer key : keys) {
                            context.put(new D(key, 0));
                        }
                    }
                }
        );

        storageSystem.__explicitLock();
        cacheService.executeWrite(
                new CacheService.WriteCommand() {
                    public void execute(CacheService.WriteContext context) {
                        for(Integer key : keys) {
                            context.put(new D(key, 1));
                        }
                    }
                }
        );
        storageSystem.__explicitLock();

        cacheService.executeWrite(
                new CacheService.WriteCommand() {
                    public void execute(CacheService.WriteContext context) {
                        for(Integer key : keys) {
                            context.put(new D(key, 2));
                        }
                    }
                }
        );
        storageSystem.__explicitLock();

        cacheService.executeWrite(
                new CacheService.WriteCommand() {
                    public void execute(CacheService.WriteContext context) {
                        for(Integer key : keys) {
                            context.remove(D.CACHE_ID, key);
                        }
                    }
                }
        );

        testHitLookups(timeToRun, "test3VMissLookups()");
    }

    @Test
    public void baselineHitLookups() {
        HashMap<Integer, D> map = new HashMap<Integer, D>();
        for(Integer key : keys) {
            map.put(key, new D(key, 1));
        }

        int i = 0;
        final int len = keys.length;
        final long runUntil = System.currentTimeMillis() + timeToRun;


        while (System.currentTimeMillis() < runUntil) {
            map.get(keys[i % len]);
            i++;
        }

        System.err.println("baselineHitLookups for " + timeToRun + "ms: " + NumberFormat.getInstance().format(i) + " lookups");
    }

    @Test
    public void baselineMissLookups() {
        HashMap<Integer, D> map = new HashMap<Integer, D>();
        for(Integer key : keys) {
            map.put(key, new D(key, 1));
        }

        int i = 0;
        final int len = moreKeys.length;
        final long runUntil = System.currentTimeMillis() + timeToRun;

        while (System.currentTimeMillis() < runUntil) {
            map.get(moreKeys[i % len]);
            i++;
        }

        System.err.println("baselineMissLookups for " + timeToRun + "ms: " + NumberFormat.getInstance().format(i) + " lookups");
    }

    private void testHitLookups(final long timeToRun, final String name) {
        cacheService.executeRead(
                new CacheService.ReadCommand<Object>() {
                    public Object execute(CacheService.ReadContext context) {

                        int i = 0;
                        final int len = keys.length;
                        final long runUntil = System.currentTimeMillis() + timeToRun;

                        while (System.currentTimeMillis() < runUntil) {
                            context.get(D.CACHE_ID, keys[i % len]);
                            i++;
                        }

                        System.err.println(name + " for " + timeToRun + "ms: " + NumberFormat.getInstance().format(i) + " lookups");

                        return null;
                    }
                }
        );
    }

    private void testMissLookups(final long timeToRun, final String name) {
        cacheService.executeRead(
                new CacheService.ReadCommand<Object>() {
                    public Object execute(CacheService.ReadContext context) {

                        int i = 0;
                        final int len = moreKeys.length;
                        final long runUntil = System.currentTimeMillis() + timeToRun;

                        while (System.currentTimeMillis() < runUntil) {
                            context.get(D.CACHE_ID, moreKeys[i % len]);
                            i++;
                        }

                        System.err.println(name + " for " + timeToRun + "ms: " + NumberFormat.getInstance().format(i) + " lookups");

                        return null;
                    }
                }
        );
    }



    @Before
    public void setUp() throws Exception {
        writeLock = new Object();
        CacheServiceInitializer csi = new CacheServiceInitializer();
        csi.setWriteLock(writeLock);
        csi.setUnlockExecutor(Executors.newSingleThreadExecutor());
        csi.setUnlockAsynchronously(false);
        csi.setCacheSpecs(new HashSet<CacheSpec>(Arrays.asList(
            new CacheSpec(
                D.CACHE_ID, Collections.<IndexSpec>emptySet()
            )
        )));

        cacheService = new CacheService(csi);
        storageSystem = ((StorageSystemImpl) cacheService.getBaseStorageSystem());

        keys = new Integer[1000];
        for(int i=0; i<keys.length; i++) keys[i] = new Integer(i);
        moreKeys = new Integer[1000];
        for(int i=0; i<keys.length; i++) keys[i] = new Integer(i + 10000);
    }


    private static class D implements ImmutableCacheableObject<BasicCacheIdentifier> {
        public static final BasicCacheIdentifier CACHE_ID = new BasicCacheIdentifier("D", D.class);

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

        public BasicCacheIdentifier getCacheType() {
            return CACHE_ID;
        }
        public void stored() {
        }
    }
}
