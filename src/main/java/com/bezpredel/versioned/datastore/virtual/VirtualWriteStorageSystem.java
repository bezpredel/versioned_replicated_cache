package com.bezpredel.versioned.datastore.virtual;

import com.bezpredel.versioned.datastore.AsyncCommand;
import com.bezpredel.versioned.datastore.StorageSystem;
import com.bezpredel.versioned.datastore.UpdateDescriptor;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class VirtualWriteStorageSystem<DATA, INDX> implements StorageSystem<DATA, INDX>, UpdateEventProvider<DATA, INDX> {
    private final static Logger logger = Logger.getLogger(VirtualWriteStorageSystem.class);
    private final StorageSystem<DATA, INDX> delegate;
    private final Object writeLock;
    private final List<UpdateListener<DATA, INDX>> listeners = new ArrayList<UpdateListener<DATA, INDX>>();

    public VirtualWriteStorageSystem(StorageSystem<DATA, INDX> delegate, Object writeLock) {
        this.delegate = delegate;
        this.writeLock = writeLock;
    }

    public void executeWrite(WriteCommand<DATA, INDX> writeCommand, DataChangedCallback<DATA, INDX> callback) {
        synchronized (writeLock) {
            VirtualWriteContextImpl<DATA, INDX> writeContext = new VirtualWriteContextImpl<DATA, INDX>(
                    delegate.dirtyReadContext()
            );

            writeCommand.execute(writeContext); // exceptions from here WILL be rethrown, and the changes will be discarded

            // ok, so we got here, so the data application worked fine. Let's replay things quickly.
            if(!writeContext.isEmpty()) {
                UpdateDescriptor<DATA, INDX> updateDescriptor = writeContext.produceUpdateDescriptor();
                // the following call will re-lock the writeLock, but that's OK
                delegate.executeWrite(new UpdateDescriptorWriteCommand<DATA, INDX>(updateDescriptor), callback);
                // at this point, the write has been committed

                fire(writeContext.getVersion(), updateDescriptor);
            } else {
                // do nothing, since nothing really happened
            }

        }
    }

    public <R> R executeRead(ReadCommand<R, DATA, INDX> command, boolean unlockAsynchronously) {
        return delegate.executeRead(command, unlockAsynchronously);
    }

    public <R> AsyncCommand<R> startAsyncRead(ReadCommand<R, DATA, INDX> command, boolean unlockAsynchronously) {
        return delegate.startAsyncRead(command, unlockAsynchronously);
    }

    public ReadContext<DATA, INDX> dirtyReadContext() {
        return delegate.dirtyReadContext();
    }

    public void addUpdateListener(UpdateListener<DATA, INDX> dataindxUpdateListener) {
        if(!listeners.contains(dataindxUpdateListener)) listeners.add(dataindxUpdateListener);
    }

    public void removeUpdateListener(UpdateListener<DATA, INDX> dataindxUpdateListener) {
        listeners.remove(dataindxUpdateListener);
    }

    protected void fire(int version, UpdateDescriptor<DATA, INDX> updateDescriptor) {
        for(UpdateListener<DATA, INDX> listener : listeners) {
            try {
                listener.onUpdate(version, updateDescriptor);
            } catch (Exception e) {
                logger.error("Exception while firing", e);
            }
        }
    }

    public Collection<DATA> getSupportedDataTypes() {
        return delegate.getSupportedDataTypes();
    }

    public Collection<INDX> getSupportedIndexTypes() {
        return delegate.getSupportedIndexTypes();
    }
}

