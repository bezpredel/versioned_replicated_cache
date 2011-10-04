package com.bezpredel.collections;

public class CircularLinkedListHelper {
    private CircularLinkedListHelper (){}

    public static void destroy(LinkedListNode node) {
        node.setNext(null);
        node.setPrevious(null);
    }

    /**
     * @return next node
     */
    public static LinkedListNode remove(LinkedListNode node) {
        if(isDestroyed(node)) {
            // we are destroyed
            assert node.getPrevious()==null;
            throw new IllegalStateException("Cannot remove already removed node");
        } else if(node.getNext() == node) {
            return null;
        } else {
            node.getPrevious().setNext(node.getNext());
            node.getNext().setPrevious(node.getPrevious());

            return node.getNext();
        }
    }

    public static void addBefore(LinkedListNode nextNode, LinkedListNode nodeToAdd) {
        if(isDestroyed(nodeToAdd)) {
            // we are destroyed
            throw new IllegalStateException("Cannot add already removed node");
        } else if(isDestroyed(nextNode)) {
            // next node is destroyed
            throw new IllegalStateException("Cannot add after already removed node");
        } else if(nextNode==nodeToAdd) {
            throw new IllegalStateException("Cannot add to self");
        }

        //this is the tail of this group
        // nextNode is head of the other group
        LinkedListNode headOfThisGroup = nodeToAdd.getNext();
        LinkedListNode tailOfOtherGroup = nextNode.getPrevious();

        headOfThisGroup.setPrevious(tailOfOtherGroup);
        nodeToAdd.setNext(nextNode);
        tailOfOtherGroup.setNext(headOfThisGroup);
        nextNode.setPrevious(nodeToAdd);
    }

    public static boolean isTheOnlyOne(LinkedListNode node) {
        return node.getNext()==node;
    }

    public static boolean isDestroyed(LinkedListNode node) {
        return node.getNext()==null;
    }

    public static int getSize(LinkedListNode node) {
        if(node.getNext()==null) {
            return 0;
        } else {
            int i = 1;

            LinkedListNode n = node.getNext();
            while (n != node) {
                i++;
                n = n.getNext();
            }

            return i;
        }
    }


    public interface LinkedListNode {
        public void setNext(LinkedListNode n);
        public void setPrevious(LinkedListNode n);
        public LinkedListNode getNext();
        public LinkedListNode getPrevious();
    }
}

