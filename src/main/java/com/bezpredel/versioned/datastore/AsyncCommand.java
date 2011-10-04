package com.bezpredel.versioned.datastore;

public interface AsyncCommand<T> extends Runnable {
    T execute();
    void cancel();
}
