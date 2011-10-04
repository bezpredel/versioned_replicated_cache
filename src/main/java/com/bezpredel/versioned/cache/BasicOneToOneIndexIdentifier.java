package com.bezpredel.versioned.cache;

public final class BasicOneToOneIndexIdentifier extends AbstractIndexIdentifier {
    private static final long serialVersionUID = -5965588549459018028L;
    private static final Integer classIdentity = 1;

    private final OneToOneID dataStoreId;

    public BasicOneToOneIndexIdentifier(BasicCacheIdentifier cacheIdentifier, String name) {
        super(cacheIdentifier, name);

        this.dataStoreId = new OneToOneID(this);
    }

    public boolean isOneToMany() {
        return false;
    }

    protected OneToOneID getDataStoreId() {
        return dataStoreId;
    }

    public String toString() {
        return getCacheType().getCacheName() + "->" + getName();
    }

    @Override
    protected Object getClassIdentity() {
        return classIdentity;
    }
}
