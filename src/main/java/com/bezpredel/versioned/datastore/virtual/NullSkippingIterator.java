package com.bezpredel.versioned.datastore.virtual;

import com.bezpredel.versioned.datastore.Keyed;
import com.bezpredel.collections.AbstractDelegatingIterator;

import java.util.Iterator;

class NullSkippingIterator extends AbstractDelegatingIterator<Keyed, Keyed> {
    NullSkippingIterator(Iterator<Keyed> delegate) {
        super(delegate);
    }

    @Override
    protected Keyed getValue(Keyed vv) {
        return vv == VirtualData.NULL ? null : vv;
    }
}
