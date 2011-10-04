package com.bezpredel.versioned.cache;

public final class BasicOneToManyIndexIdentifier extends AbstractIndexIdentifier {
    private static final long serialVersionUID = -691996805037110311L;
    private static final Integer classIdentity = 0;

    private final OneToManyID dataStoreId;

    public BasicOneToManyIndexIdentifier(BasicCacheIdentifier cacheIdentifier, String name) {
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
        return getCacheType().getCacheName() + "->*" + getName();
    }

    @Override
    protected Object getClassIdentity() {
        return classIdentity;
    }
}
