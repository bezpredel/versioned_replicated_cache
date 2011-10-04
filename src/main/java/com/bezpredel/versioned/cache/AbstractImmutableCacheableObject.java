package com.bezpredel.versioned.cache;


import java.io.Serializable;

public abstract class AbstractImmutableCacheableObject implements ImmutableCacheableObject<BasicCacheIdentifier>, Cloneable, Serializable {
    private static final long serialVersionUID = 8263108840747854862L;

    private final Object key;
    private transient volatile boolean locked;/*volatile?*/

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

    public String toString() {
        return getClass().getSimpleName() + "#" + key.toString() + (locked ?"" :"*");
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
