package com.bezpredel.versioned.datastore.actual;


interface Sync {

    public boolean assertInSharedLock();

    public boolean assertInExclusiveWriteLock();


    public boolean isInWriteLock();
}
