package com.bezpredel.versioned.datastore.actual;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.bezpredel.collections.MapFactory;
import com.bezpredel.versioned.datastore.Keyed;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class StorageSystemMultiThreadedTest {
    static final Object TYPE = new Object();
    private StorageSystemImpl<Object, Object> storageSystem;
    private Object globalWriteLock;
    private Executor sharedUnlockExecutor;

    @Before
    public void init() {
        globalWriteLock = new Object();
        sharedUnlockExecutor = Executors.newSingleThreadExecutor();

        storageSystem = new StorageSystemImpl<Object, Object>(
                Object.class, Object.class,
                new MapFactory() {
                    public <K, V> Map<K, V> createMap(Class<K> keyClass) {
                        return new HashMap<K, V>();
                    }
                },
                Collections.singleton(TYPE),
                Collections.emptySet(),
                globalWriteLock,
                sharedUnlockExecutor
        );
    }

    @Test
    public void testDataStorage1() {
        prepareDataStorage();
        long duration = 10000;
        long finishTime = System.currentTimeMillis() + duration;
        HashSet<Thread> threads = new HashSet<Thread>();

        for(int i=0; i<2; i++) {
            Thread thread = new WriterThread(finishTime, i);
            threads.add(thread);
            thread.start();
        }

        for(int i=0; i<10; i++) {
            Thread thread = new ReaderThread(finishTime, i, i % 2==0);
            threads.add(thread);
            thread.start();
        }

        for(Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testDataStorage2() throws Exception{
        prepareDataStorage();
        long duration = 10000;
        long finishTime = System.currentTimeMillis() + duration;
        HashSet<Thread> threads = new HashSet<Thread>();

        {
            Thread thread = new WriterThread(finishTime, 0);
            threads.add(thread);
            thread.start();
        }

        for(int i=0; i<3; i++) {
            Thread thread = new ReaderThread(finishTime, i, false);
            threads.add(thread);
            thread.start();
        }

        for(Thread t : threads) {
            try {
                t.join();
                System.out.println(t);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

//        System.out.println("Sleeping");
//        Thread.sleep(20000);
    }

    @Test
    public void testIndexStorage1() throws Exception {
        //todo
    }

    private void prepareDataStorage() {
        storageSystem.executeWrite(
                new StorageSystemImpl.WriteCommand() {
                    public void execute(StorageSystemImpl.WriteContext context) {
                        int ver = context.getVersion();

                        for(int i = ver; i < ver + 15; i++) {
                            context.put(TYPE, new Item(i, ver));
                        }
                    }
                },
                null);
    }


    private class WriterThread extends Thread {
        private final long finishTime;
        private final int id;
        private int cnt = 0;

        private WriterThread(long finishTime, int id) {
            this.finishTime = finishTime;
            this.id = id;
        }

        @Override
        public void run() {
            while(System.currentTimeMillis() < finishTime) {
                storageSystem.executeWrite(
                    new StorageSystemImpl.WriteCommand() {
                        public void execute(StorageSystemImpl.WriteContext context) {
                            int ver = context.getVersion();
                            context.remove(TYPE, ver - 1);
                            for (int i = ver; i < ver + 15; i++) {
                                context.put(TYPE, new Item(i, ver));
                            }

//                            System.out.println("Writer# "+id+" OK " + ver);
                        }
                    },
                        null);
                cnt++;
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public String toString() {
            return "Writer#" + id +": " + cnt;
        }
    }

    private class ReaderThread extends Thread {
        private final long finishTime;
        private final int id;
        private final boolean sleeper;
        private int cnt = 0;

        private ReaderThread(long finishTime, int id, boolean sleeper) {
            this.finishTime = finishTime;
            this.id = id;
            this.sleeper = sleeper;
        }

        @Override
        public void run() {
            final Random r = new Random();
            while(System.currentTimeMillis() < finishTime) {
                storageSystem.executeRead(
                    new StorageSystemImpl.ReadCommand<Object, Object, Object>() {
                        public Object execute(StorageSystemImpl.ReadContext context) {
                            verifyExpectations(context);
                            if(sleeper) {
                                try {
                                    Thread.sleep(r.nextInt(10));
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
//                            System.out.println("Reader# "+id + (sleeper ? "S" : "")+" OK " + context.getVersion());
                            return null;
                        }
                    }, false
                );
                cnt++;
            }
        }

        @Override
        public String toString() {
            return "Reader#" + id +": " + cnt;
        }

    }


    private void verifyExpectations(StorageSystemImpl.ReadContext<Object, Object> context) {
        int ver = context.getVersion();

        Assert.assertNull(context.get(TYPE, ver - 1));

        for(int i = ver; i < ver + 15; i++) {
            Item item = context.get(TYPE, i);
            Assert.assertNotNull(item);
            Assert.assertEquals(i, item.getKey());
            Assert.assertTrue(item.getVersion() <= ver);
        }

        Assert.assertNull(context.get(TYPE, ver + 15));

        Iterator<Item> iter = context.values(TYPE);
        for(int i = ver; i < ver + 15; i++) {
            Assert.assertTrue(iter.hasNext());
            Item item = iter.next();
            Assert.assertNotNull(item);
            Assert.assertEquals(i, item.getKey());
            Assert.assertTrue(item.getVersion() == ver);
        }
        Assert.assertFalse(iter.hasNext());
    }

    static class Item implements Keyed {
        final int version;
        final Integer key;

        Item(Integer key, int version) {
            this.version = version;
            this.key = key;
        }

        public Object getKey() {
            return key;
        }

        public int getVersion() {
            return version;
        }
    }
}
