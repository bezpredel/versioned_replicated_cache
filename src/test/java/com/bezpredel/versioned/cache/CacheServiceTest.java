package com.bezpredel.versioned.cache;

import com.bezpredel.collections.FieldGetterFunction;
import com.bezpredel.versioned.cache.def.CacheSpec;
import com.bezpredel.versioned.cache.def.IndexSpec;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;
import static com.bezpredel.TestUtils.validateCollectionContents;

public class CacheServiceTest {
    private static final Box redSquare_nowhere = new Box(1, Color.red, Shape.square, null);
    private static final Box greenSquare_nowhere = new Box(2, Color.green, Shape.square, null);
    private static final Box blueSquare_nowhere = new Box(3, Color.blue, Shape.square, null);
    private static final Box orangeCircle_nowhere = new Box(4, Color.orange, Shape.round, null);
    private static final Box pinkCircle_nowhere = new Box(5, Color.pink, Shape.round, null);
    private static final Box whiteCircle_nowhere = new Box(6, Color.white, Shape.round, null);
    private static final Box blackTriangle_nowhere = new Box(7, Color.black, Shape.triangular, null);
    private static final Box yellowTriangle_nowhere = new Box(8, Color.yellow, Shape.triangular, null);

    private static final Box redSquare_kitchen = new Box(1, Color.red, Shape.square, Room.kitchen);
    private static final Box greenSquare_kitchen = new Box(2, Color.green, Shape.square, Room.kitchen);

    private static final Box redSquare_bedroom = new Box(1, Color.red, Shape.square, Room.bedroom);
    private static final Box greenSquare_bedroom = new Box(2, Color.green, Shape.square, Room.bedroom);


    private static final Marble mRed1_nowhere = new Marble(1, Color.red, null);
    private static final Marble mRed2_nowhere = new Marble(2, Color.red, null);
    private static final Marble mRed3_nowhere = new Marble(3, Color.red, null);
    private static final Marble mRed4_nowhere = new Marble(4, Color.red, null);
    private static final Marble mRed5_nowhere = new Marble(5, Color.red, null);
    private static final Marble mRed6_nowhere = new Marble(6, Color.red, null);
    private static final Marble mGreen1_nowhere = new Marble(7, Color.green, null);
    private static final Marble mGreen2_nowhere = new Marble(8, Color.green, null);
    private static final Marble mGreen3_nowhere = new Marble(9, Color.green, null);
    private static final Marble mGreen4_nowhere = new Marble(10, Color.green, null);


    private CacheService cacheService;
    private Object writeLock;

    @Test
    public void testBasic() throws Exception {
        // initial insert
        cacheService.executeWrite(
            new CacheService.WriteCommand() {
                public void execute(CacheService.WriteContext context) {
                    putAll(context, redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere);
                    validateCollectionContents(context.values(Box.CACHE_ID), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere);
                    validateEachItemsPresence(context, redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.square), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.round), orangeCircle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.triangular));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, null), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.bedroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.livingroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.kitchen));
                    assertSame(redSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.red));
                    assertSame(greenSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.green));
                    assertSame(blueSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.blue));
                    assertSame(orangeCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.orange));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.pink));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.white));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.black));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.yellow));
                }
            }
        );

        cacheService.executeRead(
                new CacheService.ReadCommand<Object>() {
                    public Object execute(CacheService.ReadContext context) {
                        validateCollectionContents(context.values(Box.CACHE_ID), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere);
                        validateEachItemsPresence(context, redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere);

                        validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.square), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere);
                        validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.round), orangeCircle_nowhere);
                        validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.triangular));
                        validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, null), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere);
                        validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.bedroom));
                        validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.livingroom));
                        validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.kitchen));
                        assertSame(redSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.red));
                        assertSame(greenSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.green));
                        assertSame(blueSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.blue));
                        assertSame(orangeCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.orange));
                        assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.pink));
                        assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.white));
                        assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.black));
                        assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.yellow));

                        return null;
                    }
                }
        );

        try {
            cacheService.executeWrite(
                new CacheService.WriteCommand() {
                    public void execute(CacheService.WriteContext context) {
                        putAll(context, pinkCircle_nowhere, whiteCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                        validateCollectionContents(context.values(Box.CACHE_ID), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, whiteCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                        validateEachItemsPresence(context, redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, whiteCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);

                        validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.square), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere);
                        validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.round), orangeCircle_nowhere, pinkCircle_nowhere, whiteCircle_nowhere);
                        validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.triangular), blackTriangle_nowhere, yellowTriangle_nowhere);

                        validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, null), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, whiteCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                        validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.bedroom));
                        validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.livingroom));
                        validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.kitchen));
                        assertSame(redSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.red));
                        assertSame(greenSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.green));
                        assertSame(blueSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.blue));
                        assertSame(orangeCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.orange));
                        assertSame(pinkCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.pink));
                        assertSame(whiteCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.white));
                        assertSame(blackTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.black));
                        assertSame(yellowTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.yellow));

                        throw new RuntimeException(); //rollback
                    }
                }
            );
            assertTrue("Exception is expected to bubble", false);
        } catch (Exception e) {
        }

        cacheService.executeRead(
            new CacheService.ReadCommand<Object>() {
                public Object execute(CacheService.ReadContext context) {
                    validateCollectionContents(context.values(Box.CACHE_ID), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere);
                    validateEachItemsPresence(context, redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.square), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.round), orangeCircle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.triangular));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, null), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.bedroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.livingroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.kitchen));
                    assertSame(redSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.red));
                    assertSame(greenSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.green));
                    assertSame(blueSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.blue));
                    assertSame(orangeCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.orange));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.pink));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.white));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.black));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.yellow));

                    return null;
                }
            }
        );

        cacheService.executeWrite(
            new CacheService.WriteCommand() {
                public void execute(CacheService.WriteContext context) {
                    putAll(context, pinkCircle_nowhere, whiteCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.values(Box.CACHE_ID), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, whiteCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateEachItemsPresence(context, redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, whiteCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.square), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.round), orangeCircle_nowhere, pinkCircle_nowhere, whiteCircle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.triangular), blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, null), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, whiteCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.bedroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.livingroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.kitchen));
                    assertSame(redSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.red));
                    assertSame(greenSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.green));
                    assertSame(blueSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.blue));
                    assertSame(orangeCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.orange));
                    assertSame(pinkCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.pink));
                    assertSame(whiteCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.white));
                    assertSame(blackTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.black));
                    assertSame(yellowTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.yellow));
                }
            }
        );


        cacheService.executeRead(
            new CacheService.ReadCommand<Object>() {
                public Object execute(CacheService.ReadContext context) {
                    validateCollectionContents(context.values(Box.CACHE_ID), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, whiteCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateEachItemsPresence(context, redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, whiteCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.square), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.round), orangeCircle_nowhere, pinkCircle_nowhere, whiteCircle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.triangular), blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, null), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, whiteCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.bedroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.livingroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.kitchen));
                    assertSame(redSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.red));
                    assertSame(greenSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.green));
                    assertSame(blueSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.blue));
                    assertSame(orangeCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.orange));
                    assertSame(pinkCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.pink));
                    assertSame(whiteCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.white));
                    assertSame(blackTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.black));
                    assertSame(yellowTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.yellow));

                    return null;
                }
            }
        );

        cacheService.executeWrite(
            new CacheService.WriteCommand() {
                public void execute(CacheService.WriteContext context) {
                    context.remove(whiteCircle_nowhere);

                    validateCollectionContents(context.values(Box.CACHE_ID), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateEachItemsPresence(context, redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.square), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.round), orangeCircle_nowhere, pinkCircle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.triangular), blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, null), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.bedroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.livingroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.kitchen));
                    assertSame(redSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.red));
                    assertSame(greenSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.green));
                    assertSame(blueSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.blue));
                    assertSame(orangeCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.orange));
                    assertSame(pinkCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.pink));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.white));
                    assertSame(blackTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.black));
                    assertSame(yellowTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.yellow));
                }
            }
        );

        cacheService.executeRead(
            new CacheService.ReadCommand<Object>() {
                public Object execute(CacheService.ReadContext context) {
                    validateCollectionContents(context.values(Box.CACHE_ID), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateEachItemsPresence(context, redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.square), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.round), orangeCircle_nowhere, pinkCircle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.triangular), blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, null), redSquare_nowhere, greenSquare_nowhere, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.bedroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.livingroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.kitchen));
                    assertSame(redSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.red));
                    assertSame(greenSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.green));
                    assertSame(blueSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.blue));
                    assertSame(orangeCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.orange));
                    assertSame(pinkCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.pink));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.white));
                    assertSame(blackTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.black));
                    assertSame(yellowTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.yellow));

                    return null;
                }
            }
        );

        cacheService.executeWrite(
            new CacheService.WriteCommand() {
                public void execute(CacheService.WriteContext context) {
                    assertSame(greenSquare_nowhere, context.put(greenSquare_kitchen));
                    assertSame(redSquare_nowhere, context.put(redSquare_kitchen));

                    validateCollectionContents(context.values(Box.CACHE_ID), redSquare_kitchen, greenSquare_kitchen, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateEachItemsPresence(context, redSquare_kitchen, greenSquare_kitchen, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.square), redSquare_kitchen, greenSquare_kitchen, blueSquare_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.round), orangeCircle_nowhere, pinkCircle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.triangular), blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, null), blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.kitchen), redSquare_kitchen, greenSquare_kitchen);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.livingroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.bedroom));
                    assertSame(redSquare_kitchen, context.getByIndex(Box.INDEX_COLOR, Color.red));
                    assertSame(greenSquare_kitchen, context.getByIndex(Box.INDEX_COLOR, Color.green));
                    assertSame(blueSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.blue));
                    assertSame(orangeCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.orange));
                    assertSame(pinkCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.pink));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.white));
                    assertSame(blackTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.black));
                    assertSame(yellowTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.yellow));

                    assertSame(greenSquare_kitchen, context.put(greenSquare_bedroom));
                    assertSame(redSquare_kitchen, context.put(redSquare_bedroom));

                    validateCollectionContents(context.values(Box.CACHE_ID), redSquare_bedroom, greenSquare_bedroom, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateEachItemsPresence(context, redSquare_bedroom, greenSquare_bedroom, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.square), redSquare_bedroom, greenSquare_bedroom, blueSquare_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.round), orangeCircle_nowhere, pinkCircle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.triangular), blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, null), blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.bedroom), redSquare_bedroom, greenSquare_bedroom);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.livingroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.kitchen));
                    assertSame(redSquare_bedroom, context.getByIndex(Box.INDEX_COLOR, Color.red));
                    assertSame(greenSquare_bedroom, context.getByIndex(Box.INDEX_COLOR, Color.green));
                    assertSame(blueSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.blue));
                    assertSame(orangeCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.orange));
                    assertSame(pinkCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.pink));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.white));
                    assertSame(blackTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.black));
                    assertSame(yellowTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.yellow));

                }
            }
        );

        cacheService.executeRead(
            new CacheService.ReadCommand<Object>() {
                public Object execute(CacheService.ReadContext context) {
                    validateCollectionContents(context.values(Box.CACHE_ID), redSquare_bedroom, greenSquare_bedroom, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateEachItemsPresence(context, redSquare_bedroom, greenSquare_bedroom, blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.square), redSquare_bedroom, greenSquare_bedroom, blueSquare_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.round), orangeCircle_nowhere, pinkCircle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_SHAPE, Shape.triangular), blackTriangle_nowhere, yellowTriangle_nowhere);

                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, null), blueSquare_nowhere, orangeCircle_nowhere, pinkCircle_nowhere, blackTriangle_nowhere, yellowTriangle_nowhere);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.bedroom), redSquare_bedroom, greenSquare_bedroom);
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.livingroom));
                    validateCollectionContents(context.valuesByIndex(Box.INDEX_LOCATION, Room.kitchen));
                    assertSame(redSquare_bedroom, context.getByIndex(Box.INDEX_COLOR, Color.red));
                    assertSame(greenSquare_bedroom, context.getByIndex(Box.INDEX_COLOR, Color.green));
                    assertSame(blueSquare_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.blue));
                    assertSame(orangeCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.orange));
                    assertSame(pinkCircle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.pink));
                    assertSame(null, context.getByIndex(Box.INDEX_COLOR, Color.white));
                    assertSame(blackTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.black));
                    assertSame(yellowTriangle_nowhere, context.getByIndex(Box.INDEX_COLOR, Color.yellow));
                    return null;
                }
            }
        );

        cacheService.executeWrite(
            new CacheService.WriteCommand() {
                public void execute(CacheService.WriteContext context) {
                    context.remove(redSquare_bedroom);
                    context.remove(greenSquare_bedroom);
                }
            }
        );


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
                        true
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
        public void stored() {
        }

    }


    private static enum Color {
        red, green, blue, orange, pink, white, black, yellow
    }

    private static enum Shape {
        square, round, triangular
    }

    private static enum Room {
        bedroom, livingroom, kitchen
    }

}
