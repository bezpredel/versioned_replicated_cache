package com.bezpredel.versioned.cache;

import com.bezpredel.collections.SerializablePseudoEnum;

public abstract class AbstractIndexIdentifier extends SerializablePseudoEnum implements IndexIdentifierType<BasicCacheIdentifier> {
    private static final long serialVersionUID = 4290712625460552220L;


    private final BasicCacheIdentifier cacheIdentifier;
    private final String name;

    public AbstractIndexIdentifier(BasicCacheIdentifier cacheIdentifier, String name) {
        super(cacheIdentifier.getCacheName() + "." + name);

        this.cacheIdentifier = cacheIdentifier;
        this.name = name;
    }

    public BasicCacheIdentifier getCacheType() {
        return cacheIdentifier;
    }

    public String getName() {
        return name;
    }

    @Override
    protected abstract Object getClassIdentity();
}
