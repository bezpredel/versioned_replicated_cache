package com.bezpredel.versioned.cache;

import com.bezpredel.collections.PseudoEnum;


public class OneToOneID extends PseudoEnum {
    private final BasicCacheIdentifier cacheIdentifier;
    private final BasicOneToOneIndexIdentifier indexIdentifier;

    public OneToOneID(BasicCacheIdentifier cacheIdentifier) {
        this.cacheIdentifier = cacheIdentifier;
        this.indexIdentifier = null;
    }

    public OneToOneID(BasicOneToOneIndexIdentifier indexIdentifier) {
        this.indexIdentifier = indexIdentifier;
        this.cacheIdentifier = null;
    }

    public boolean isIndex() {
        return indexIdentifier!=null;
    }

    public BasicCacheIdentifier getCacheIdentifier() {
        if(cacheIdentifier==null) throw new UnsupportedOperationException();
        return cacheIdentifier;
    }

    public BasicOneToOneIndexIdentifier getIndexIdentifier() {
        if(indexIdentifier==null) throw new UnsupportedOperationException();
        return indexIdentifier;
    }

    @Override
    public String toString() {
        return cacheIdentifier!=null ? cacheIdentifier.toString() : indexIdentifier.toString();
    }
}
