//package com.bezpredel.versioned.datastore.virtual;
//
//import com.bezpredel.collections.MapFactory;
//import com.bezpredel.versioned.datastore.StorageSystem;
//import org.apache.log4j.Logger;
//
//import java.util.*;
//
//public class VirtualWriteMultiStorageSystem<DATA, INDX> implements StorageSystem<DATA, INDX>, UpdateEventProvider<DATA, INDX> {
//    private final static Logger logger = Logger.getLogger(VirtualWriteMultiStorageSystem.class);
//    private final Map<DATA, DataStorageRec> dataToStorageSystemDelegateMap;
//    private final Map<INDX, DataStorageRec> indexToStorageSystemDelegateMap;
//
//    private final Collection<DATA> supportedDataTypes;
//    private final Collection<INDX> supportedIndexTypes;
//
//    private final List<UpdateListener<DATA, INDX>> listeners = new ArrayList<UpdateListener<DATA, INDX>>();
//
//    public VirtualWriteMultiStorageSystem(StorageSystem<DATA, INDX>[] delegates, Object[] writeLocks, Class<DATA> dataIdentifierClass, Class<INDX> indexIdentifierClass, MapFactory mapFactory) {
//        assert delegates.length == writeLocks.length;
//        dataToStorageSystemDelegateMap = mapFactory.createMap(dataIdentifierClass);
//        indexToStorageSystemDelegateMap = mapFactory.createMap(indexIdentifierClass);
//        supportedDataTypes = new HashSet<DATA>();
//        supportedIndexTypes = new HashSet<INDX>();
//
//        for(int i=0; i<delegates.length; i++) {
//            StorageSystem<DATA, INDX> delegate = delegates[i];
//            Object lock = writeLocks[i];
//            supportedIndexTypes.addAll(delegate.getSupportedIndexTypes());
//
//            DataStorageRec rec = new DataStorageRec(delegate, lock, i);
//            for(DATA dataType : delegate.getSupportedDataTypes()) {
//                dataToStorageSystemDelegateMap.put(dataType, rec);
//                supportedDataTypes.add(dataType);
//            }
//
//            for(INDX indexType : delegate.getSupportedIndexTypes()) {
//                indexToStorageSystemDelegateMap.put(indexType, rec);
//                supportedIndexTypes.add(indexType);
//            }
//        }
//    }
//
//
//    private void executeInNestedLocks(Runnable runnable, Object[] lock, int index) {
//        if(index==lock.length) {
//            runnable.run();
//        } else {
//            synchronized (lock[index]) {
//                executeInNestedLocks(runnable, lock, index + 1);
//            }
//        }
//    }
//
//
//
//    public void executeWrite(WriteCommand<DATA, INDX> writeCommand) {
//        synchronized (writeLock) {
//            VirtualWriteContextImpl<DATA, INDX> writeContext = new VirtualWriteContextImpl<DATA, INDX>(
//                    delegate.dirtyReadContext()
//            );
//
//            writeCommand.execute(writeContext); // exceptions from here WILL be rethrown, and the changes will be discarded
//
//            // ok, so we got here, so the data application worked fine. Let's replay things quickly.
//            if(!writeContext.isEmpty()) {
//                UpdateDescriptor<DATA, INDX> updateDescriptor = writeContext.produceUpdateDescriptor();
//                // the following call will re-lock the writeLock, but that's OK
//                delegate.executeWrite(new ActualWriteCommand<DATA, INDX>(updateDescriptor));
//                // at this point, the write has been committed
//
//                fire(writeContext.getVersion(), updateDescriptor);
//            } else {
//                // do nothing, since nothing really happened
//            }
//        }
//    }
//
//    public <R> R executeRead(ReadCommand<R, DATA, INDX> command, boolean unlockAsynchronously) {
//        return delegate.executeRead(command, unlockAsynchronously);
//    }
//
//    public ReadContext<DATA, INDX> dirtyReadContext() {
//        return delegate.dirtyReadContext();
//    }
//
//    public void addUpdateListener(UpdateListener<DATA, INDX> dataindxUpdateListener) {
//        if(!listeners.contains(dataindxUpdateListener)) listeners.add(dataindxUpdateListener);
//    }
//
//    public void removeUpdateListener(UpdateListener<DATA, INDX> dataindxUpdateListener) {
//        listeners.remove(dataindxUpdateListener);
//    }
//
//    protected void fire(int version, UpdateDescriptor<DATA, INDX> updateDescriptor) {
//        for(UpdateListener<DATA, INDX> listener : listeners) {
//            try {
//                listener.onUpdate(version, updateDescriptor);
//            } catch (Exception e) {
//                logger.error("Exception while firing", e);
//            }
//        }
//    }
//
//    // NOTE: to reduce garbage, merge with WriteContextImpl
//    private static class ActualWriteCommand<D, I> implements WriteCommand<D, I> {
//        private final UpdateDescriptor<D, I> data;
//
//        public ActualWriteCommand(UpdateDescriptor<D, I> data) {
//            this.data = data;
//        }
//
//        public void execute(WriteContext<D, I> context) {
//            data.applyTo(context);
//        }
//    }
//
//    public Collection<DATA> getSupportedDataTypes() {
//        return delegate.getSupportedDataTypes();
//    }
//
//    public Collection<INDX> getSupportedIndexTypes() {
//        return delegate.getSupportedIndexTypes();
//    }
//
//    private class DataStorageRec implements Comparable<DataStorageRec>{
//        private final StorageSystem<DATA, INDX> delegate;
//        private final Object writeLock;
//        private final int order;
//
//        private DataStorageRec(StorageSystem<DATA, INDX> delegate, Object writeLock, int order) {
//            this.delegate = delegate;
//            this.writeLock = writeLock;
//            this.order = order;
//        }
//
//        public int compareTo(DataStorageRec o) {
//            return this.order - o.order;
//        }
//    }
//}
//
