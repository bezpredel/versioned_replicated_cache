package com.bezpredel.versioned.cache;

import com.bezpredel.collections.FieldGetterFunction;
import com.bezpredel.versioned.cache.def.CacheSpec;
import com.bezpredel.versioned.cache.def.IndexSpec;
import com.bezpredel.versioned.datastore.actual.StorageSystemImpl;
import org.apache.commons.lang.mutable.MutableInt;
import org.junit.Before;
import org.junit.Test;

import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import static com.bezpredel.TestUtils.validateCollectionContents;
import static org.junit.Assert.*;

public class CacheServiceLoadTest {
    private CacheService cacheService;
    private Object writeLock;

    private final Box[] boxes_bedroom = new Box[] {
            new Box(0, Color.a, Shape.square, Room.bedroom),
            new Box(1, Color.b, Shape.square, Room.bedroom),
            new Box(2, Color.c, Shape.square, Room.bedroom),
            new Box(3, Color.d, Shape.square, Room.bedroom),
            new Box(4, Color.e, Shape.square, Room.bedroom),
            new Box(5, Color.f, Shape.square, Room.bedroom),
            new Box(6, Color.g, Shape.square, Room.bedroom),
            new Box(7, Color.h, Shape.square, Room.bedroom),
            new Box(8, Color.i, Shape.square, Room.bedroom),
            new Box(9, Color.j, Shape.square, Room.bedroom),
            new Box(10, Color.k, Shape.square, Room.bedroom),
            new Box(11, Color.l, Shape.square, Room.bedroom),
            new Box(12, Color.m, Shape.square, Room.bedroom),
            new Box(13, Color.n, Shape.square, Room.bedroom),
            new Box(14, Color.o, Shape.square, Room.bedroom),
            new Box(15, Color.p, Shape.square, Room.bedroom)
    };
    
    private final Box[] boxes_livingroom = new Box[] {
            new Box(0, Color.a, Shape.square, Room.livingroom),
            new Box(1, Color.b, Shape.square, Room.livingroom),
            new Box(2, Color.c, Shape.square, Room.livingroom),
            new Box(3, Color.d, Shape.square, Room.livingroom),
            new Box(4, Color.e, Shape.square, Room.livingroom),
            new Box(5, Color.f, Shape.square, Room.livingroom),
            new Box(6, Color.g, Shape.square, Room.livingroom),
            new Box(7, Color.h, Shape.square, Room.livingroom),
            new Box(8, Color.i, Shape.square, Room.livingroom),
            new Box(9, Color.j, Shape.square, Room.livingroom),
            new Box(10, Color.k, Shape.square, Room.livingroom),
            new Box(11, Color.l, Shape.square, Room.livingroom),
            new Box(12, Color.m, Shape.square, Room.livingroom),
            new Box(13, Color.n, Shape.square, Room.livingroom),
            new Box(14, Color.o, Shape.square, Room.livingroom),
            new Box(15, Color.p, Shape.square, Room.livingroom)
    };
    
    private final Box[] boxes_kitchen = new Box[] {
            new Box(0, Color.a, Shape.square, Room.kitchen),
            new Box(1, Color.b, Shape.square, Room.kitchen),
            new Box(2, Color.c, Shape.square, Room.kitchen),
            new Box(3, Color.d, Shape.square, Room.kitchen),
            new Box(4, Color.e, Shape.square, Room.kitchen),
            new Box(5, Color.f, Shape.square, Room.kitchen),
            new Box(6, Color.g, Shape.square, Room.kitchen),
            new Box(7, Color.h, Shape.square, Room.kitchen),
            new Box(8, Color.i, Shape.square, Room.kitchen),
            new Box(9, Color.j, Shape.square, Room.kitchen),
            new Box(10, Color.k, Shape.square, Room.kitchen),
            new Box(11, Color.l, Shape.square, Room.kitchen),
            new Box(12, Color.m, Shape.square, Room.kitchen),
            new Box(13, Color.n, Shape.square, Room.kitchen),
            new Box(14, Color.o, Shape.square, Room.kitchen),
            new Box(15, Color.p, Shape.square, Room.kitchen)
    };

    private final Box[][] boxes = new Box[][]{boxes_bedroom, boxes_kitchen, boxes_livingroom};

    private final Marble[][] marbles;

    private final int marblesMask;


    public CacheServiceLoadTest() {
        marbles = new Marble[5][];
        Color[] colors = new Color[]{Color.a, Color.b, Color.c, Color.d, Color.e};
        Random r = new Random();
        for(int i=0; i<5; i++) {
            marbles[i] = new Marble[16];
            for(int j=0; j<16; j++) {
                marbles[i][j] = new Marble(j, colors[i], r.nextInt(16));
            }
        }

        marblesMask = r.nextInt();
    }

    @Test
    public void testLoadWithSlowReaders() throws Exception {
        Set<Thread> threads = new HashSet<Thread>();
        long runUntil = System.currentTimeMillis() + 10000;
        threads.add(new WriterThread(runUntil, 0));
        for(int i=0; i<8; i++) {
            threads.add(new FastReaderThread(runUntil));
        }
        for(int i=0; i<6; i++) {
            threads.add(new SlowReaderThread(runUntil));
        }

        startTestAndWaitToFinish(threads);
    }

    @Test
    public void testLoadWithSlowReadersAndSleeperWriter() throws Exception {
        Set<Thread> threads = new HashSet<Thread>();
        long runUntil = System.currentTimeMillis() + 10000;
        threads.add(new WriterThread(runUntil, 10));
        for(int i=0; i<8; i++) {
            threads.add(new FastReaderThread(runUntil));
        }
        for(int i=0; i<6; i++) {
            threads.add(new SlowReaderThread(runUntil));
        }

        startTestAndWaitToFinish(threads);
    }

    @Test
    public void testLoadLotsFastReaders() throws Exception {
        Set<Thread> threads = new HashSet<Thread>();
        long runUntil = System.currentTimeMillis() + 10000;
        threads.add(new WriterThread(runUntil, 0));
        for(int i=0; i<16; i++) {
            threads.add(new FastReaderThread(runUntil));
        }

        startTestAndWaitToFinish(threads);
    }

    @Test
    public void testLoadFewFastReaders() throws Exception {
        Set<Thread> threads = new HashSet<Thread>();
        long runUntil = System.currentTimeMillis() + 10000;
        threads.add(new WriterThread(runUntil, 0));
        for(int i=0; i<3; i++) {
            threads.add(new FastReaderThread(runUntil));
        }

        startTestAndWaitToFinish(threads);
    }


    @Test
    public void testLoadTonneFastReaders() throws Exception {
        Set<Thread> threads = new HashSet<Thread>();
        long runUntil = System.currentTimeMillis() + 10000;
        threads.add(new WriterThread(runUntil, 0));
        for(int i=0; i<50; i++) {
            threads.add(new FastReaderThread(runUntil));
        }

        startTestAndWaitToFinish(threads);
    }
    @Test
    public void testLoadLotsFastReadersSleepyWriter() throws Exception {
        Set<Thread> threads = new HashSet<Thread>();
        long runUntil = System.currentTimeMillis() + 15000;
        threads.add(new WriterThread(runUntil, 5));
        for(int i=0; i<16; i++) {
            threads.add(new FastReaderThread(runUntil));
        }

        startTestAndWaitToFinish(threads);
    }

    private void startTestAndWaitToFinish(Set<Thread> threads) throws InterruptedException {
        for(Thread t : threads) {
            t.start();
        }

        for(Thread t : threads) {
            t.join();
        }
        System.out.println("-------------------------------------------------------------");
    }

    class WriterThread extends Thread {
        final long runUntil;
        private int delay;

        public WriterThread(long runUntil, int delay) {
            this.runUntil = runUntil;
            this.delay = delay;
        }

        @Override
        public void run() {
            int cnt = 0;
            try {
                while(System.currentTimeMillis() < runUntil) {
                    cacheService.executeWrite(
                            new CacheService.WriteCommand() {
                                public void execute(CacheService.WriteContext context) {
                                    updateVersion(context);
                                    verifyVersion(context);
                                }
                            }
                    );
                    cnt++;
                    if(delay > 0) {
                        Thread.sleep(delay);
                    }
                }
            } catch (Exception e) {
            }
            System.out.println("WRITE COUNT: " + cnt);
        }
    }

    class FastReaderThread extends Thread {
        final long runUntil;

        public FastReaderThread(long runUntil) {
            this.runUntil = runUntil;
        }

        @Override
        public void run() {
            int cnt = 0;
            while(System.currentTimeMillis() < runUntil) {
                cacheService.executeRead(
                        new CacheService.ReadCommand() {
                            public Object execute(CacheService.ReadContext context) {
                                verifyVersion(context);
                                return null;
                            }
                        }
                );
                cnt++;
            }

            System.out.println("FAST READ COUNT: " + cnt);
        }
    }

    class SlowReaderThread extends Thread {
        final long runUntil;


        public SlowReaderThread(long runUntil) {
            this.runUntil = runUntil;
        }

        @Override
        public void run() {
            final Random r = new Random();
            try {
                while(System.currentTimeMillis() < runUntil) {
                    cacheService.executeRead(
                            new CacheService.ReadCommand() {
                                public Object execute(CacheService.ReadContext context) {
                                    for(int i=0; i < 5 + r.nextInt(10); i++) {
                                        verifyVersion(context);
                                        try {
                                            Thread.sleep(2 + r.nextInt(10));
                                        } catch (InterruptedException e) {
                                        }
                                    }
                                    verifyVersion(context);

                                    return null;
                                }
                            }
                    );

                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
            }
        }
    }

    private boolean shouldBoxBePresent(int version, int index) {
        return (version & (1 << index))!=0;
    }

    private boolean shouldMarbleBePresent(int version, int index) {
        return ((version ^ marblesMask) & (1 << index))!=0;
    }

    private Box getBox(int version, int index) {
        return boxes[version % 3][index];
    }

    private Marble getMarble(int version, int index) {
        return marbles[version % 5][index];
    }

    private void updateVersion(CacheService.WriteContext context) {
        int version = context.getVersion();

        for (int i = 0; i < 16; i++) {
            Box b = context.get(Box.CACHE_ID, i);
            if (shouldBoxBePresent(version, i)) {
                Box newVal = getBox(version, i);
                if (newVal != b) {
                    context.put(newVal);
                }
            } else if (b != null) {
                context.remove(b);
            }

            Marble m = context.get(Marble.CACHE_ID, i);
            if (shouldMarbleBePresent(version, i)) {
                Marble newVal = getMarble(version, i);
                if (newVal != m) {
                    context.put(newVal);
                }
            } else if (m != null) {
                context.remove(m);
            }
        }
    }

    private void verifyVersion(CacheService.ReadContext context) {
        int version = context.getVersion();
        if(version==0) return;

        Map<Room, Set<Box>> rooms = new EnumMap<Room, Set<Box>>(Room.class);
        Set<Marble> marbles = new HashSet<Marble>();

        for(Room r : Room.values) {
            rooms.put(r, new HashSet<Box>());
        }

        for(int i=0; i<16; i++) {
            Box b = context.get(Box.CACHE_ID, i);
            if(shouldBoxBePresent(version, i)) {
                assertSame(getBox(version, i), b);
                rooms.get(b.location).add(b);
            } else {
                assertNull(b);
            }

            Marble m = context.get(Marble.CACHE_ID, i);
            if(shouldMarbleBePresent(version, i)) {
                assertSame(getMarble(version, i), m);
                marbles.add(m);
            } else {
                assertNull(m);
            }
        }

        for(Room room : rooms.keySet()) {
            validateCollectionContents(context.<Box>valuesByIndex(Box.INDEX_LOCATION, room), rooms.get(room).toArray(new Box[rooms.get(room).size()]));
        }

        Color mc = Color.values[version % 5];
        validateCollectionContents(context.<Marble>valuesByIndex(Marble.INDEX_COLOR, mc), marbles.toArray(new Marble[marbles.size()]));
    }



    private void putAll(CacheService.WriteContext context, ImmutableCacheableObject<BasicCacheIdentifier> ... objects) {
        for( ImmutableCacheableObject<BasicCacheIdentifier> o : objects) {
            context.put(o);
        }
    }

    private void validateEachItemsPresence (CacheService.ReadContext context, ImmutableCacheableObject<BasicCacheIdentifier> ... objects) {
        for( ImmutableCacheableObject<BasicCacheIdentifier> o : objects) {
            assertSame(o, context.get(o.getCacheType(), o.getKey()));
        }
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
                Box.CACHE_ID,
                new HashSet<IndexSpec>(Arrays.asList(
                    new IndexSpec(
                        Box.INDEX_COLOR,
                        new FieldGetterFunction<ImmutableCacheableObject, Object>(Box.class, "color"),
                        false,
                        true
                    ),
                    new IndexSpec(
                        Box.INDEX_SHAPE,
                        new FieldGetterFunction<ImmutableCacheableObject, Object>(Box.class, "shape"),
                        false,
                        true
                    ),
                    new IndexSpec(
                        Box.INDEX_LOCATION,
                        new FieldGetterFunction<ImmutableCacheableObject, Object>(Box.class, "location"),
                        true,
                        false
                    )
                ))
            ),
            new CacheSpec(
                Marble.CACHE_ID,
                new HashSet<IndexSpec>(Arrays.asList(
                    new IndexSpec(
                        Marble.INDEX_COLOR,
                        new FieldGetterFunction<ImmutableCacheableObject, Object>(Marble.class, "color"),
                        false,
                        false
                    ),
                    new IndexSpec(
                        Marble.INDEX_BOX,
                        new FieldGetterFunction<ImmutableCacheableObject, Object>(Marble.class, "boxId"),
                        true,
                        false
                    )
                ))
            )
        )));

        cacheService = new CacheService(csi);
    }

    private static class Box implements ImmutableCacheableObject<BasicCacheIdentifier> {
        public static final BasicCacheIdentifier CACHE_ID = new BasicCacheIdentifier("Box", Box.class);

        public static final BasicOneToOneIndexIdentifier INDEX_COLOR = new BasicOneToOneIndexIdentifier(CACHE_ID, "color");
        public static final BasicOneToManyIndexIdentifier INDEX_SHAPE = new BasicOneToManyIndexIdentifier(CACHE_ID, "shape");
        public static final BasicOneToManyIndexIdentifier INDEX_LOCATION = new BasicOneToManyIndexIdentifier(CACHE_ID, "location");

        private final Color color;
        private final Integer id;
        private final Shape shape;
        private final Room location;

        public Box(Integer id, Color color, Shape shape, Room location) {
            this.color = color;
            this.shape = shape;
            this.location = location;
            this.id = id;
        }

        public BasicCacheIdentifier getCacheType() {
            return CACHE_ID;
        }

        public Object getKey() {
            return id;
        }

        public String toString() {
            return id + ": " + color + " " + shape + " BOX " + (location==null ? "nowhere" : location);
        }
        public void stored() {
        }

    }

    private static class Marble implements ImmutableCacheableObject<BasicCacheIdentifier> {
        public static final BasicCacheIdentifier CACHE_ID = new BasicCacheIdentifier("Marble", Marble.class);
        public static final BasicOneToManyIndexIdentifier INDEX_COLOR = new BasicOneToManyIndexIdentifier(CACHE_ID, "color");
        public static final BasicOneToManyIndexIdentifier INDEX_BOX = new BasicOneToManyIndexIdentifier(CACHE_ID, "box");

        private final Integer id;
        private final Color color;
        private final Object boxId;

        private Marble(Integer id, Color color, Object boxId) {
            this.id = id;
            this.color = color;
            this.boxId = boxId;
        }

        public BasicCacheIdentifier getCacheType() {
            return CACHE_ID;
        }

        public Object getKey() {
            return id;
        }

        public String toString() {
            return color + " marble #" + id + " in box " + boxId;
        }

        public void stored() {
        }
    }


    private static enum Color {
        a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p;

        static Color[] values = values();
    }

    private static enum Shape {
        square, round, triangular
    }

    private static enum Room {
        bedroom, livingroom, kitchen;
        static Room[] values = values();
    }



}
