package com.bezpredel.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class CompoundIterator<T> implements Iterator<T> {
    private int i;
    private final Iterator<? extends T>[] iters;

    public CompoundIterator(Collection<Iterator<? extends T>> delegates) {
        this(delegates.toArray(new Iterator[delegates.size()]));
    }

    public CompoundIterator(Iterator<? extends T> ... delegates) {
        if (delegates.length==0) {
            throw new IllegalArgumentException();
        } else {
            iters = delegates;
        }
    }


    private boolean findNext() {
        while (true) {
            if (iters[i].hasNext()) return true;

            i++;
            if (i == iters.length) {
                return false;
            }
        }
    }

    public boolean hasNext() {
        return findNext();
    }

    public T next() {
        if (findNext()) {
            return iters[i].next();
        } else {
            throw new NoSuchElementException();
        }
    }

    public void remove() {
        iters[i].remove();
    }
}
