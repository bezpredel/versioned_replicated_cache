package com.bezpredel.versioned.cache;

public final class BasicManyToManyIndexIdentifier extends BasicOneToManyIndexIdentifier {
    private static final Integer classIdentity = 3;
    private static final long serialVersionUID = 2675319611855296058L;

    private final OneToManyID dataStoreId;

    public BasicManyToManyIndexIdentifier(BasicCacheIdentifier cacheIdentifier, String name) {
        super(cacheIdentifier, name);

        this.dataStoreId = new OneToManyID(this);
    }

    public boolean isOneToMany() {
        return true;
    }

    protected OneToManyID getDataStoreId() {
        return dataStoreId;
    }

    public String toString() {
        return getCacheType().getCacheName() + "*->*" + getName();
    }

    @Override
    protected Object getClassIdentity() {
        return classIdentity;
    }
}
