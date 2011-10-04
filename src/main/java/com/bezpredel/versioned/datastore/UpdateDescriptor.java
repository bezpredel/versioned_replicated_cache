package com.bezpredel.versioned.datastore;

import java.io.Serializable;

public interface UpdateDescriptor<DATA, INDX> extends Serializable {
    public void applyTo(StorageSystem.WriteContext<DATA, INDX> writeContext);
}
