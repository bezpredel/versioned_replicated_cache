package com.bezpredel.versioned.datastore.virtual;

import com.bezpredel.versioned.datastore.StorageSystem;
import com.bezpredel.versioned.datastore.UpdateDescriptor;

public class UpdateDescriptorWriteCommand<D, I> implements StorageSystem.WriteCommand<D, I> {
    private final UpdateDescriptor<D, I> data;

    public UpdateDescriptorWriteCommand(UpdateDescriptor<D, I> data) {
        this.data = data;
    }

    public void execute(StorageSystem.WriteContext<D, I> context) {
        data.applyTo(context);
    }
}
