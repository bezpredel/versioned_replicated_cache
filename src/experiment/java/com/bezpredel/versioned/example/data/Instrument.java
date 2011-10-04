package com.bezpredel.versioned.example.data;

import com.bezpredel.versioned.cache.AbstractImmutableCacheableObject;
import com.bezpredel.versioned.cache.BasicCacheIdentifier;
import com.bezpredel.versioned.cache.BasicOneToManyIndexIdentifier;
import com.bezpredel.versioned.cache.BasicOneToOneIndexIdentifier;

public class Instrument extends AbstractImmutableCacheableObject {
    public static final BasicCacheIdentifier CACHE = new BasicCacheIdentifier("Instrument", Instrument.class);
    public static final BasicOneToManyIndexIdentifier BY_MARKET = new BasicOneToManyIndexIdentifier(CACHE, "market");
    public static final BasicOneToOneIndexIdentifier BY_TICKER = new BasicOneToOneIndexIdentifier(CACHE, "ticker");

    private static final long serialVersionUID = 6130214596358457375L;

    private AssetType assetType;
    private Object marketId;
    private String ticker;
    private String name;
    private boolean active;

    public Instrument(Object key) {
        super(key);
    }

    public AssetType getAssetType() {
        return assetType;
    }

    public Object getMarketId() {
        return marketId;
    }

    public boolean isActive() {
        return active;
    }

    public String getTicker() {
        return ticker;
    }

    public String getName() {
        return name;
    }

    public void setAssetType(AssetType assetType) {
        checkIfModificationIsAllowed();

        this.assetType = assetType;
    }

    public void setActive(boolean active) {
        checkIfModificationIsAllowed();

        this.active = active;
    }

    public void setMarketId(Object marketId) {
        checkIfModificationIsAllowed();

        this.marketId = marketId;
    }

    public void setTicker(String ticker) {
        checkIfModificationIsAllowed();

        this.ticker = ticker;
    }

    public void setName(String name) {
        checkIfModificationIsAllowed();

        this.name = name;
    }

    public void setMarket(Market market) {
        checkIfModificationIsAllowed();

        if(market==null) {
            marketId = null;
        } else {
            marketId = market.getKey();
        }
    }

    public BasicCacheIdentifier getCacheType() {
        return CACHE;
    }


}
