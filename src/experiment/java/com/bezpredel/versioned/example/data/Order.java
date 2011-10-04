package com.bezpredel.versioned.example.data;

import com.bezpredel.versioned.cache.AbstractImmutableCacheableObject;
import com.bezpredel.versioned.cache.BasicCacheIdentifier;
import com.bezpredel.versioned.cache.BasicOneToManyIndexIdentifier;

public class Order extends AbstractImmutableCacheableObject {
    public static final BasicCacheIdentifier CACHE = new BasicCacheIdentifier("Order", Order.class);

    public static final BasicOneToManyIndexIdentifier BIDS = new BasicOneToManyIndexIdentifier(CACHE, "bids");
    public static final BasicOneToManyIndexIdentifier OFFERS = new BasicOneToManyIndexIdentifier(CACHE, "offers");
    public static final BasicOneToManyIndexIdentifier BY_USER = new BasicOneToManyIndexIdentifier(CACHE, "owner");

    private static final long serialVersionUID = -934457179502871316L;

    private Object instrumentId;
    private Object userId;
    private Side side;

    public Order(Object key) {
        super(key);
    }

    public Object getInstrumentId() {
        return instrumentId;
    }

    public Object getUserId() {
        return userId;
    }

    public Side getSide() {
        return side;
    }

    public void setInstrumentId(Object instrumentId) {
        checkIfModificationIsAllowed();
        this.instrumentId = instrumentId;
    }

    public void setUserId(Object userId) {
        checkIfModificationIsAllowed();
        this.userId = userId;
    }

    public void setInstrument(Instrument instrument) {
        checkIfModificationIsAllowed();
        this.instrumentId = instrument==null ? null : instrument.getKey();
    }

    public void setUser(User user) {
        checkIfModificationIsAllowed();
        this.userId = user==null ? null : user.getKey();
    }

    public void setSide(Side side) {
        checkIfModificationIsAllowed();
        this.side = side;
    }

    public BasicCacheIdentifier getCacheType() {
        return CACHE;
    }
}
