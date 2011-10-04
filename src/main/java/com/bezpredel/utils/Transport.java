package com.bezpredel.utils;

public interface Transport<T> {
    void send(T t);
    T receive(T t) throws InterruptedException;
}
