package com.bezpredel.versioned.cache.def;

import com.bezpredel.versioned.cache.BasicCacheIdentifier;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class CacheSpec {
    private BasicCacheIdentifier type;
    private Set<IndexSpec> indices = Collections.emptySet();

    public CacheSpec() {
    }

    public CacheSpec(BasicCacheIdentifier type, Collection<IndexSpec> indices) {
        this.type = type;
        this.indices = new HashSet<IndexSpec>(indices);
    }

    public BasicCacheIdentifier getType() {
        return type;
    }

    public void setType(BasicCacheIdentifier type) {
        this.type = type;
    }

    public Set<IndexSpec> getIndices() {
        return indices;
    }

    public void setIndices(Set<IndexSpec> indices) {
        this.indices = indices;
    }
}
