package com.bezpredel.utils;

import java.util.Queue;

public class QueueDistributor<T> implements Distributor<T> {
    private final Queue<T> targetQueue;

    public QueueDistributor(Queue<T> targetQueue) {
        this.targetQueue = targetQueue;
    }

    public void distribute(T obj) {
        targetQueue.add(obj);
    }
}
