package com.bezpredel.versioned.cache;

import com.bezpredel.collections.PseudoEnum;


public class OneToManyID extends PseudoEnum {
    private final BasicOneToManyIndexIdentifier indexIdentifier;

    public OneToManyID(BasicOneToManyIndexIdentifier indexIdentifier) {
        this.indexIdentifier = indexIdentifier;
    }

    @Override
    public String toString() {
        return indexIdentifier.toString();
    }
}
