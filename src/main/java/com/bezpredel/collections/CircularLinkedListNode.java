package com.bezpredel.collections;

public class CircularLinkedListNode {
    private CircularLinkedListNode next = this; //if this proves to interfere with gc, maybe it should be optimized to be nulls
    private CircularLinkedListNode previous = this;

    public CircularLinkedListNode getNext() {
        return next;
    }

    private void setNext(CircularLinkedListNode next) {
        this.next = next;
    }

    public CircularLinkedListNode getPrevious() {
        return previous;
    }

    private void setPrevious(CircularLinkedListNode previous) {
        this.previous = previous;
    }

    public void destroy() {
        next = null;
        previous = null;
    }

    /**
     * @return next node
     */
    public CircularLinkedListNode remove() {
        if(this.isDestroyed()) {
            // we are destroyed
            assert this.previous==null;
            throw new IllegalStateException("Cannot remove already removed node");
        } else if(this.getNext() == this) {
            return null;
        } else {
            this.getPrevious().setNext(this.getNext());
            this.getNext().setPrevious(this.getPrevious());

            return this.getNext();
        }
    }

    public void addBefore(CircularLinkedListNode nextNode) {
        if(this.isDestroyed()) {
            // we are destroyed
            throw new IllegalStateException("Cannot add already removed node");
        } else if(nextNode.isDestroyed()) {
            // next node is destroyed
            throw new IllegalStateException("Cannot add after already removed node");
        } else if(nextNode==this) {
            throw new IllegalStateException("Cannot add to self");
        }

        //this is the tail of this group
        // nextNode is head of the other group
        CircularLinkedListNode headOfThisGroup = this.getNext();
        CircularLinkedListNode tailOfOtherGroup = nextNode.getPrevious();

        headOfThisGroup.setPrevious(tailOfOtherGroup);
        this.setNext(nextNode);
        tailOfOtherGroup.setNext(headOfThisGroup);
        nextNode.setPrevious(this);
    }

    public boolean isTheOnlyOne() {
        return getNext()==this;
    }

    public boolean isDestroyed() {
        return getNext()==null;
    }

    public int getSize() {
        if(this.getNext()==null) {
            return 0;
        } else {
            int i = 1;

            CircularLinkedListNode n = this.getNext();
            while (n != this) {
                i++;
                n = n.getNext();
            }

            return i;
        }
    }
}

