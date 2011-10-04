package com.bezpredel.versioned.example.data;

import com.bezpredel.versioned.cache.AbstractImmutableCacheableObject;
import com.bezpredel.versioned.cache.BasicCacheIdentifier;

public class Institution extends AbstractImmutableCacheableObject {
    public static final BasicCacheIdentifier CACHE = new BasicCacheIdentifier("Institution", Institution.class);
    private static final long serialVersionUID = -3697899337418738122L;

    private String name;

    public Institution(Object key) {
        super(key);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        checkIfModificationIsAllowed();

        this.name = name;
    }

    public BasicCacheIdentifier getCacheType() {
        return CACHE;
    }


}
