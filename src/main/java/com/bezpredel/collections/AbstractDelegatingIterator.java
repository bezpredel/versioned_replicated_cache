package com.bezpredel.collections;


import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractDelegatingIterator<INTERMEDIATE, T> implements Iterator<T> {
    private final Iterator<INTERMEDIATE> delegate;
    protected T current;

    public AbstractDelegatingIterator(Iterator<INTERMEDIATE> delegate) {
        this.delegate = delegate;
    }

    protected void advance() {
        while(delegate.hasNext() && current==null) {
            INTERMEDIATE vv = delegate.next();
            this.current = getValue(vv);
        }
    }

    protected abstract T getValue(INTERMEDIATE vv);

    public boolean hasNext() {
        advance();

        return this.current!=null;
    }

    public T next() {
        advance();
        T retVal = current;
        if(retVal==null) {
            throw new NoSuchElementException();
        } else {
            clearCurrent();
            return retVal;
        }
    }

    protected void clearCurrent() {
        current = null;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}
