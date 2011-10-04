package com.bezpredel.collections;

import org.junit.Assert;
import org.junit.Test;


public class CircularLinkedListNodeTest {
    @Test
    public void testOneNode() throws Exception {
        CircularLinkedListNode node1 = new CircularLinkedListNode();
        Assert.assertSame(node1, node1.getNext());
        Assert.assertSame(node1, node1.getPrevious());
        Assert.assertEquals(true, node1.isTheOnlyOne());
        Assert.assertEquals(1, node1.getSize());

        Assert.assertEquals(false, node1.isDestroyed());
    }

    @Test
    public void testDestroy1() throws Exception {
        CircularLinkedListNode node1 = new CircularLinkedListNode();

        node1.destroy();

        Assert.assertEquals(node1.getSize(), 0);

        Assert.assertSame(null, node1.getNext());
        Assert.assertSame(null, node1.getPrevious());

        Assert.assertEquals(false, node1.isTheOnlyOne());

        Assert.assertEquals(true, node1.isDestroyed());
    }

    @Test(expected = IllegalStateException.class)
    public void testDestroy2() throws Exception {
        CircularLinkedListNode node1 = new CircularLinkedListNode();

        node1.destroy();

        node1.remove();
    }

    @Test(expected = IllegalStateException.class)
    public void testDestroy3() throws Exception {
        CircularLinkedListNode node1 = new CircularLinkedListNode();
        CircularLinkedListNode node2 = new CircularLinkedListNode();

        node1.destroy();

        node1.addBefore(node2);
    }


    @Test(expected = IllegalStateException.class)
    public void testDestroy4() throws Exception {
        CircularLinkedListNode node1 = new CircularLinkedListNode();
        CircularLinkedListNode node2 = new CircularLinkedListNode();

        node1.destroy();

        node2.addBefore(node1);
    }

    @Test
    public void testInsert1() throws Exception {
        CircularLinkedListNode node1 = new CircularLinkedListNodeX(1);
        CircularLinkedListNode node2 = new CircularLinkedListNodeX(2);
        CircularLinkedListNode node3 = new CircularLinkedListNodeX(3);
        CircularLinkedListNode node4 = new CircularLinkedListNodeX(4);

        node1.addBefore(node2);
        node2.addBefore(node3);
        node3.addBefore(node4);

        Assert.assertSame(node2, node1.getNext());
        Assert.assertSame(node4, node1.getPrevious());

        Assert.assertSame(node3, node2.getNext());
        Assert.assertSame(node1, node2.getPrevious());

        Assert.assertSame(node4, node3.getNext());
        Assert.assertSame(node2, node3.getPrevious());

        Assert.assertSame(node1, node4.getNext());
        Assert.assertSame(node3, node4.getPrevious());
    }

    @Test
    public void testInsert2() throws Exception {
        CircularLinkedListNode node1 = new CircularLinkedListNodeX(1);
        CircularLinkedListNode node2 = new CircularLinkedListNodeX(2);
        CircularLinkedListNode node3 = new CircularLinkedListNodeX(3);
        CircularLinkedListNode node4 = new CircularLinkedListNodeX(4);

        node1.addBefore(node2);

        node3.addBefore(node4);

        node2.addBefore(node3);

        Assert.assertSame(node2, node1.getNext());
        Assert.assertSame(node4, node1.getPrevious());

        Assert.assertSame(node3, node2.getNext());
        Assert.assertSame(node1, node2.getPrevious());

        Assert.assertSame(node4, node3.getNext());
        Assert.assertSame(node2, node3.getPrevious());

        Assert.assertSame(node1, node4.getNext());
        Assert.assertSame(node3, node4.getPrevious());
    }

    @Test
    public void testRemove1() throws Exception {
        CircularLinkedListNode node1 = new CircularLinkedListNodeX(1);
        CircularLinkedListNode node2 = new CircularLinkedListNodeX(2);
        CircularLinkedListNode node3 = new CircularLinkedListNodeX(3);
        CircularLinkedListNode node4 = new CircularLinkedListNodeX(4);

        node1.addBefore(node2);
        node2.addBefore(node3);
        node3.addBefore(node4);

        node1.remove();


        Assert.assertSame(node3, node2.getNext());
        Assert.assertSame(node4, node2.getPrevious());

        Assert.assertSame(node4, node3.getNext());
        Assert.assertSame(node2, node3.getPrevious());

        Assert.assertSame(node2, node4.getNext());
        Assert.assertSame(node3, node4.getPrevious());

        node2.remove();


        Assert.assertSame(node4, node3.getNext());
        Assert.assertSame(node4, node3.getPrevious());

        Assert.assertSame(node3, node4.getNext());
        Assert.assertSame(node3, node4.getPrevious());

        node3.remove();


        Assert.assertSame(node4, node4.getNext());
        Assert.assertSame(node4, node4.getPrevious());

        Assert.assertEquals(true, node4.isTheOnlyOne());

        node4.remove();

    }

    private static class CircularLinkedListNodeX extends CircularLinkedListNode {
        private final int i;

        private CircularLinkedListNodeX(int i) {
            this.i = i;
        }

        @Override
        public String toString() {
            return "Node#" + i;
        }
    }
}
