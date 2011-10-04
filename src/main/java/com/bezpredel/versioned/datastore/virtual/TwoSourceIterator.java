package com.bezpredel.versioned.datastore.virtual;

import com.bezpredel.versioned.datastore.Keyed;
import com.bezpredel.collections.AbstractDelegatingIterator;

import java.util.Iterator;
import java.util.Map;

class TwoSourceIterator<T> extends AbstractDelegatingIterator<Keyed, T> {
    private final Map<Object, Keyed> fromHere;
    private NullSkippingIterator localIterator;

    TwoSourceIterator(Iterator<Keyed> fromParent, Map<Object, Keyed> fromHere) {
        super(fromParent);
        this.fromHere = fromHere;
    }

    @Override
    protected void advance() {
        if(localIterator==null) {
            super.advance();

            if(current==null && !fromHere.isEmpty()) {
                localIterator = new NullSkippingIterator(fromHere.values().iterator());
            }
        }

        if (localIterator != null && current==null) {
            current = localIterator.hasNext() ? (T) localIterator.next() : null;
        }
    }

    @Override
    protected T getValue(Keyed vv) {
        if(fromHere.containsKey(vv.getKey())) {
            return null;
        } else {
            return (T)vv;
        }
    }
}
