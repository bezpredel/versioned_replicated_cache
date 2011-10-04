package com.bezpredel.versioned.example.data;

import com.bezpredel.versioned.cache.AbstractImmutableCacheableObject;
import com.bezpredel.versioned.cache.BasicCacheIdentifier;

public class Market extends AbstractImmutableCacheableObject {
    public static final BasicCacheIdentifier CACHE = new BasicCacheIdentifier("Market", Market.class);
    private static final long serialVersionUID = -1124528450528928780L;

    private long openTime;
    private long closeTime;
    private String name;


    public Market(Object key) {
        super(key);
    }

    public long getOpenTime() {
        return openTime;
    }

    public long getCloseTime() {
        return closeTime;
    }

    public String getName() {
        return name;
    }

    public void setOpenTime(long openTime) {
        checkIfModificationIsAllowed();
        this.openTime = openTime;
    }

    public void setCloseTime(long closeTime) {
        checkIfModificationIsAllowed();
        this.closeTime = closeTime;
    }

    public void setName(String name) {
        checkIfModificationIsAllowed();
        this.name = name;
    }

    public BasicCacheIdentifier getCacheType() {
        return CACHE;
    }
}
