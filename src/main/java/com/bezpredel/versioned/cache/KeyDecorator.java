package com.bezpredel.versioned.cache;



public class KeyDecorator implements ImmutableCacheableObject {
    public static final Object NULL_KEY = new Object();

    private final Object key;
    private final ImmutableCacheableObject value;

    KeyDecorator(Object key, ImmutableCacheableObject value) {
        assert key != null;
        this.key = key;
        this.value = value;
    }

    public Object getKey() {
        return key;
    }

    public CacheIdentifierType getCacheType() {
        return value.getCacheType();
    }

    public ImmutableCacheableObject getValue() {
        return value;
    }

    public static Object wrap(Object key) {
        return key==null ? NULL_KEY : key;
    }

    public void stored() {
    }
}
