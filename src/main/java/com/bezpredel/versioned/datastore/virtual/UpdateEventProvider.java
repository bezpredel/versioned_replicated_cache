package com.bezpredel.versioned.datastore.virtual;

public interface UpdateEventProvider<C,I> {
    void addUpdateListener(UpdateListener<C,I> listener);
    void removeUpdateListener(UpdateListener<C,I> listener);
}
