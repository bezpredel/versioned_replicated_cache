package com.bezpredel.versioned.datastore.virtual;

import com.bezpredel.TestUtils;
import com.bezpredel.versioned.datastore.StorageSystem;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import com.bezpredel.versioned.datastore.Keyed;
import com.bezpredel.versioned.datastore.actual.StorageSystemImpl;
import com.bezpredel.collections.MapFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class VirtualWriteStorageSystemTest {
    static final Object CACHE1 = new Object();
    static final Object INDEX1 = new Object();

    private static final K obj0_0 = new K(0, 0);
    private static final K obj0_1 = new K(0, 1);
    private static final K obj0_2 = new K(0, 2);
    private static final K obj1_0 = new K(1, 0);
    private static final K obj2_0 = new K(2, 0);
    private static final K obj3_0 = new K(3, 0);
    private static final K obj4_0 = new K(4, 0);
    private static final K obj5_0 = new K(5, 0);

    private static final Object key0 = 0;
    private static final Object key1 = 1;
    private static final Object key2 = 2;
    private static final Object key3 = 3;
    private static final Object key4 = 4;
    private static final Object key5 = 5;

    private static final Object leafKey1 = "leaf1";
    private static final Object leafKey2 = "leaf2";



    private Object globalWriteLock;
    private Executor sharedUnlockExecutor;
    private StorageSystemImpl<Object, Object> storageSystem;
    private VirtualWriteStorageSystem<Object, Object> virtualWriteStorageSystem;

    @Before
    public void setUp() throws Exception {
        globalWriteLock = new Object();
        sharedUnlockExecutor = Executors.newSingleThreadExecutor();

        storageSystem = new StorageSystemImpl<Object, Object>(
                Object.class, Object.class,
                new MapFactory() {
                    public <K, V> Map<K, V> createMap(Class<K> keyClass) {
                        return new HashMap<K, V>();
                    }
                },
                Collections.singleton(CACHE1),
                Collections.singleton(INDEX1),
                globalWriteLock,
                sharedUnlockExecutor
        );

        virtualWriteStorageSystem = new VirtualWriteStorageSystem<Object, Object>(storageSystem, globalWriteLock);
    }

    @Test
    public void testVirtualDataStateDuringWrite() throws Exception {
        virtualWriteStorageSystem.executeWrite(
            new WriteCommand() {
                public void execute(StorageSystem.WriteContext<Object, Object> context) {
                    context.put(CACHE1, obj0_0);
                    context.put(CACHE1, obj1_0);
                    context.put(CACHE1, obj2_0);
                }

            },
                null);


        virtualWriteStorageSystem.executeWrite(
                new WriteCommand() {
                    public void execute(StorageSystem.WriteContext<Object, Object> context) {
                        assertSame(obj0_0, context.get(CACHE1, key0));
                        assertSame(obj1_0, context.get(CACHE1, key1));
                        assertSame(obj2_0, context.get(CACHE1, key2));

                        K prev = context.put(CACHE1, obj0_1);

                        assertSame(obj0_0, prev);
                        assertSame(obj0_1, context.get(CACHE1, key0));
                        assertSame(obj1_0, context.get(CACHE1, key1));
                        assertSame(obj2_0, context.get(CACHE1, key2));

                        prev = context.remove(CACHE1, key0);

                        assertSame(obj0_1, prev);
                        assertNull(context.get(CACHE1, key0));
                        assertSame(obj1_0, context.get(CACHE1, key1));
                        assertSame(obj2_0, context.get(CACHE1, key2));

                        prev = context.put(CACHE1, obj0_2);

                        assertNull(prev);

                        prev = context.put(CACHE1, obj3_0);

                        assertNull(prev);

                        assertSame(obj0_2, context.get(CACHE1, key0));
                        assertSame(obj1_0, context.get(CACHE1, key1));
                        assertSame(obj2_0, context.get(CACHE1, key2));
                        assertSame(obj3_0, context.get(CACHE1, key3));

                        context.remove(CACHE1, key0);
                        context.remove(CACHE1, key1);

                        Iterator<Keyed> values = context.values(CACHE1);
                        TestUtils.validateCollectionContents(
                                values,
                                obj2_0, obj3_0
                        );

                    }
                },
                null);


        virtualWriteStorageSystem.executeRead(
                new StorageSystem.ReadCommand<Object, Object, Object>() {
                    public Object execute(StorageSystem.ReadContext<Object, Object> context) {
                        Iterator<Keyed> values = context.values(CACHE1);
                        TestUtils.validateCollectionContents(
                                values,
                                obj2_0, obj3_0
                        );
                        return null;
                    }
                }, false
        );
    }

    @Test
    public void testRegularOperations() throws Exception {
        virtualWriteStorageSystem.executeWrite(
            new WriteCommand() {
                public void execute(StorageSystem.WriteContext<Object, Object> context) {
                    context.put(CACHE1, obj0_0);
                    context.put(CACHE1, obj1_0);
                    context.put(CACHE1, obj2_0);
                }
            },
                null);


        virtualWriteStorageSystem.executeRead(
                new StorageSystem.ReadCommand<Object, Object, Object>() {
                    public Object execute(StorageSystem.ReadContext<Object, Object> context) {
                        assertSame(obj0_0, context.get(CACHE1, key0));
                        assertSame(obj1_0, context.get(CACHE1, key1));
                        assertSame(obj2_0, context.get(CACHE1, key2));
                        assertNull(context.get(CACHE1, key3));
                        assertNull(context.get(CACHE1, key4));
                        return null;
                    }
                }, false
        );

        try {
            virtualWriteStorageSystem.executeWrite(
                    new WriteCommand() {
                        public void execute(StorageSystem.WriteContext<Object, Object> context) {
                            context.put(CACHE1, obj3_0);
                            context.put(CACHE1, obj4_0);
                            context.remove(CACHE1, key0);
                            throw new RuntimeException();
                        }
                    },
                    null);

            assertFalse(true);
        } catch (Exception e) {

        }

        virtualWriteStorageSystem.executeRead(
                new StorageSystem.ReadCommand<Object, Object, Object>() {
                    public Object execute(StorageSystem.ReadContext<Object, Object> context) {
                        assertSame(obj0_0, context.get(CACHE1, key0));
                        assertSame(obj1_0, context.get(CACHE1, key1));
                        assertSame(obj2_0, context.get(CACHE1, key2));
                        assertNull(context.get(CACHE1, key3));
                        assertNull(context.get(CACHE1, key4));
                        return null;
                    }
                }, false
        );

        virtualWriteStorageSystem.executeWrite(
                new WriteCommand() {
                    public void execute(StorageSystem.WriteContext<Object, Object> context) {
                        context.put(CACHE1, obj3_0);
                        context.put(CACHE1, obj4_0);
                        context.remove(CACHE1, key0);
                    }
                },
                null);


        virtualWriteStorageSystem.executeRead(
                new StorageSystem.ReadCommand<Object, Object, Object>() {
                    public Object execute(StorageSystem.ReadContext<Object, Object> context) {
                        assertNull(context.get(CACHE1, key0));
                        assertSame(obj1_0, context.get(CACHE1, key1));
                        assertSame(obj2_0, context.get(CACHE1, key2));
                        assertSame(obj3_0, context.get(CACHE1, key3));
                        assertSame(obj4_0, context.get(CACHE1, key4));
                        return null;
                    }
                }, false
        );
    }

    @Test
    public void testIndexOperations() throws Exception {
        //TODO: implement
    }

    @Test
    public void testVirtualIndexStateDuringWrite() throws Exception {
        virtualWriteStorageSystem.executeWrite(
            new WriteCommand() {
                public void execute(StorageSystem.WriteContext<Object, Object> context) {
                    context.addToIndex(INDEX1, leafKey1, obj0_0);
                    context.addToIndex(INDEX1, leafKey1, obj1_0);
                    context.addToIndex(INDEX1, leafKey1, obj2_0);
                    context.addToIndex(INDEX1, leafKey2, obj3_0);
                    context.addToIndex(INDEX1, leafKey2, obj4_0);
                }
            },
                null);


        virtualWriteStorageSystem.executeWrite(
                new WriteCommand() {
                    public void execute(StorageSystem.WriteContext<Object, Object> context) {
                        TestUtils.validateCollectionContents(
                            context.valuesByIndex(INDEX1, leafKey1),
                            obj0_0, obj1_0, obj2_0
                        );

                        TestUtils.validateCollectionContents(
                            context.valuesByIndex(INDEX1, leafKey2),
                            obj3_0, obj4_0
                        );

                        context.addToIndex(INDEX1, leafKey1, obj0_1);

                        TestUtils.validateCollectionContents(
                                context.valuesByIndex(INDEX1, leafKey1),
                                obj0_1, obj1_0, obj2_0
                        );

                        context.removeFromIndex(INDEX1, leafKey1, key1);

                        TestUtils.validateCollectionContents(
                                context.valuesByIndex(INDEX1, leafKey1),
                                obj0_1, obj2_0
                        );

                        context.removeFromIndex(INDEX1, leafKey1, key0);

                        TestUtils.validateCollectionContents(
                                context.valuesByIndex(INDEX1, leafKey1),
                                obj2_0
                        );

                        context.addToIndex(INDEX1, leafKey1, obj0_2);

                        TestUtils.validateCollectionContents(
                                context.valuesByIndex(INDEX1, leafKey1),
                                obj0_2, obj2_0
                        );

                        context.addToIndex(INDEX1, leafKey1, obj5_0);

                        TestUtils.validateCollectionContents(
                                context.valuesByIndex(INDEX1, leafKey1),
                                obj0_2, obj2_0, obj5_0
                        );

                        TestUtils.validateCollectionContents(
                            context.valuesByIndex(INDEX1, leafKey2),
                            obj3_0, obj4_0
                        );
                        context.removeFromIndex(INDEX1, leafKey2, key3);
                        context.removeFromIndex(INDEX1, leafKey2, key4);

                        TestUtils.validateCollectionContents(
                                context.valuesByIndex(INDEX1, leafKey2)
                        );

                    }
                },
                null);


        virtualWriteStorageSystem.executeRead(
                new StorageSystem.ReadCommand<Object, Object, Object>() {
                    public Object execute(StorageSystem.ReadContext<Object, Object> context) {
                        TestUtils.validateCollectionContents(
                                context.valuesByIndex(INDEX1, leafKey1),
                                obj0_2, obj2_0, obj5_0
                        );

                        TestUtils.validateCollectionContents(
                            context.valuesByIndex(INDEX1, leafKey2)
                        );

                        return null;
                    }
                }, false
        );
    }

    private abstract static class WriteCommand implements StorageSystem.WriteCommand<Object, Object> {
    }

    private static class K implements Keyed {
        private final Integer key;
        private final int version;

        public K(Integer key, int version) {
            this.key = key;
            this.version = version;
        }

        public Integer getKey() {
            return key;
        }

        public String toString() {
            return "K#"+key + "x" + version;
        }
    }
}
