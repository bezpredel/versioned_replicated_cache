package com.bezpredel.versioned.cache;

import java.io.Serializable;

public interface UpdateDescriptor extends Serializable {
    public Object getSessionIdentifier();
    public int getVersion();
    public void applyTo(CacheService.WriteContext writeContext);
}
