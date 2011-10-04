package com.bezpredel.versioned.cache.def;

import com.bezpredel.versioned.cache.*;
import com.google.common.base.Function;

public class IndexSpec {
    private IndexIdentifierType<BasicCacheIdentifier> identifierType;
    private Function<ImmutableCacheableObject, Object> mapFunction;
    private boolean supportNullKeys;
    private boolean isStaticMapping;

    public IndexSpec() {
    }

    public IndexSpec(IndexIdentifierType<BasicCacheIdentifier> identifierType, Function<ImmutableCacheableObject, Object> mapFunction, boolean supportNullKeys, boolean staticMapping) {
        this.identifierType = identifierType;
        this.mapFunction = mapFunction;
        this.supportNullKeys = supportNullKeys;
        isStaticMapping = staticMapping;
    }

    public Function<ImmutableCacheableObject, Object> getMapFunction() {
        return mapFunction;
    }

    public void setMapFunction(Function<ImmutableCacheableObject, Object> mapFunction) {
        this.mapFunction = mapFunction;
    }

    public void setIdentifierType(IndexIdentifierType<BasicCacheIdentifier> identifierType) {
        this.identifierType = identifierType;
    }

    public boolean isOneToMany() {
        return identifierType instanceof BasicOneToManyIndexIdentifier;
    }

    public boolean isSupportNullKeys() {
        return supportNullKeys;
    }

    public void setSupportNullKeys(boolean supportNullKeys) {
        this.supportNullKeys = supportNullKeys;
    }

    public boolean isStaticMapping() {
        return isStaticMapping;
    }

    public void setStaticMapping(boolean staticMapping) {
        isStaticMapping = staticMapping;
    }

    public BasicOneToManyIndexIdentifier getBasicOneToManyIndexIdentifier() {
        return (BasicOneToManyIndexIdentifier) identifierType;
    }

    public BasicOneToOneIndexIdentifier getBasicOneToOneIndexIdentifier() {
        return (BasicOneToOneIndexIdentifier) identifierType;
    }
}
