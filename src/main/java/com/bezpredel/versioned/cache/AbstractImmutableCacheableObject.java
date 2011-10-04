package com.bezpredel.versioned.cache;


public abstract class AbstractImmutableCacheableObject implements ImmutableCacheableObject<BasicCacheIdentifier>, Cloneable {
    private final Object key;
    private boolean locked;

    public AbstractImmutableCacheableObject(Object key) {
        this.key = key;
        this.locked = false;
    }

    public Object getKey() {
        return key;
    }

    public void stored() {
        locked = true;
    }

    protected void checkIfModificationIsAllowed() {
        if(locked) {
            throw new IllegalStateException("Not allowed to modify locked objects");
        }
    }

    public AbstractImmutableCacheableObject clone() {
        try {
            AbstractImmutableCacheableObject copy = (AbstractImmutableCacheableObject) super.clone();
            copy.locked = false;
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
