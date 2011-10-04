package com.bezpredel.utils;

import java.util.ArrayList;

public class MultiDistributor<T> implements Distributor<T> {
    private final ArrayList<Distributor<T>> distributors = new ArrayList<Distributor<T>>();

    public void addDistributor(Distributor<T> d) {
        if(!distributors.contains(d)) distributors.add(d);
    }

    public void removeDistributor(Distributor<T> d) {
        distributors.remove(d);
    }

    public void distribute(T obj) {
        for(int i=0, c=distributors.size(); i<c; i++) {
            distributors.get(i).distribute(obj);
        }
    }
}
