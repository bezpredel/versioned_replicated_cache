package com.bezpredel.versioned.cache;

public final class BasicManyToManyIndexIdentifier extends BasicOneToManyIndexIdentifier {
    private static final Integer classIdentity = 3;
    private static final long serialVersionUID = 2675319611855296058L;

    public BasicManyToManyIndexIdentifier(BasicCacheIdentifier cacheIdentifier, String name) {
        super(cacheIdentifier, name);
    }

    public boolean isOneToMany() {
        return true;
    }

    public String toString() {
        return getCacheType().getCacheName() + "*->*" + getName();
    }

    @Override
    protected Object getClassIdentity() {
        return classIdentity;
    }
}
