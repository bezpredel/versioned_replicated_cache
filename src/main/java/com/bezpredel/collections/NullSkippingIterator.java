package com.bezpredel.collections;

import java.util.Iterator;

public class NullSkippingIterator<T> extends AbstractDelegatingIterator<T, T> {
    @Override
    protected T getValue(T t) {
        return t;
    }

    public NullSkippingIterator(Iterator<T> delegate) {
        super(delegate);
    }
}
