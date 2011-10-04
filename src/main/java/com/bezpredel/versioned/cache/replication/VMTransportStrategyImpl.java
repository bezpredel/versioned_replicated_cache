package com.bezpredel.versioned.cache.replication;

import com.bezpredel.utils.MultiDistributor;
import com.bezpredel.utils.QueueDistributor;
import com.bezpredel.versioned.cache.UpdateDescriptor;
import com.google.common.base.Throwables;

import java.util.concurrent.*;

public class VMTransportStrategyImpl implements TransportStrategy {
    private final LinkedBlockingDeque<UpdateDescriptor> queue = new LinkedBlockingDeque<UpdateDescriptor>();
    private final ExecutorService snapshotRetrievalExecutorService;
    private final MultiDistributor<UpdateDescriptor> multiDistributor;
    private final MasterCacheSyncServer masterCacheSyncServer;

    private int snapshotRetrievalTimeout = 15000;

    public VMTransportStrategyImpl(ExecutorService snapshotRetrievalExecutorService, MultiDistributor<UpdateDescriptor> multiDistributor, MasterCacheSyncServer masterCacheSyncServer) {
        this.snapshotRetrievalExecutorService = snapshotRetrievalExecutorService;
        this.multiDistributor = multiDistributor;
        this.masterCacheSyncServer = masterCacheSyncServer;

        this.multiDistributor.addDistributor(new QueueDistributor<UpdateDescriptor>(queue));
    }

    public SnapshotUpdateDescriptor retrieveSnapshot() throws WillNotBeAvailableRightAwayException, InterruptedException, TimeoutException {
        Future<SnapshotUpdateDescriptor> future = snapshotRetrievalExecutorService.submit(
                new Callable<SnapshotUpdateDescriptor>() {
                    public SnapshotUpdateDescriptor call() throws Exception {
                        return masterCacheSyncServer.retrieveSnapshotUpdateDescriptor();
                    }
                }
        );

        try {
            return future.get(snapshotRetrievalTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    public UpdateDescriptor fetchNextUpdate() throws TimeoutException, InterruptedException, TransportFailedException {
        return queue.take();
    }

    public UpdateDescriptor tryFetchNextUpdate() {
        return queue.poll();
    }


    public void setSnapshotRetrievalTimeout(int snapshotRetrievalTimeout) {
        this.snapshotRetrievalTimeout = snapshotRetrievalTimeout;
    }
}
