package com.bezpredel.versioned.datastore.virtual;

import com.bezpredel.versioned.datastore.UpdateDescriptor;

public interface UpdateListener<D,I> {
    void onUpdate(int version, UpdateDescriptor<D,I> descriptor);
}
