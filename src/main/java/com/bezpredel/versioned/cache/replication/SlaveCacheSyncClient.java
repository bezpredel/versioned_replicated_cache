package com.bezpredel.versioned.cache.replication;

import com.bezpredel.versioned.cache.UpdateDescriptor;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;

public class SlaveCacheSyncClient {
    private final static Logger logger = Logger.getLogger(SlaveCacheSyncClient.class);

    private final TransportStrategy transportStrategy;
    private final SlaveCacheService slaveCacheService;
    private final Thread syncThread;
    private int snapshotBackoffPause = 10000;

    public SlaveCacheSyncClient(TransportStrategy transportStrategy, SlaveCacheService slaveCacheService) {
        this(transportStrategy, slaveCacheService, null);
    }

    public SlaveCacheSyncClient(TransportStrategy transportStrategy, SlaveCacheService slaveCacheService, ThreadFactory threadFactory) {
        this.transportStrategy = transportStrategy;
        this.slaveCacheService = slaveCacheService;
        this.syncThread = threadFactory!=null ? threadFactory.newThread(SYNC) : new Thread(SYNC);
        this.syncThread.setName("Writer for " + slaveCacheService);
    }

    private SnapshotUpdateDescriptor retrieveInitialSnapshot() throws InterruptedException {
        while(true) {
            try {
                return transportStrategy.retrieveSnapshot();
            } catch (TransportStrategy.WillNotBeAvailableRightAwayException e) {
                logger.info("Failed to retrieve snapshot, will retry in " + snapshotBackoffPause);
                Thread.sleep(snapshotBackoffPause);
            }
        }
    }


    private void syncCache() throws InterruptedException, GapInVersionNumbersException, DifferentSessionIdentifierException, TimeoutException {
        SnapshotUpdateDescriptor snapshot = retrieveInitialSnapshot();

        List<UpdateDescriptor> pendingUpdates = new ArrayList<UpdateDescriptor>();
        UpdateDescriptor update;

        int nextExpectedVersion = snapshot.getVersion() + 1;
        Object sessionIdentifier = snapshot.getSessionIdentifier();

        while ((update = transportStrategy.tryFetchNextUpdate()) != null) {
            checkUpdate(update, nextExpectedVersion, sessionIdentifier);
            if(update.getVersion() < nextExpectedVersion) {
                // skipping
            } else {
                pendingUpdates.add(update);
                nextExpectedVersion++;
            }
        }

        slaveCacheService.clear();

        slaveCacheService.replay(snapshot);

        for(UpdateDescriptor updateDescriptor : pendingUpdates) {
            slaveCacheService.replay(updateDescriptor);
        }

        runMessageLoop(nextExpectedVersion, sessionIdentifier);
    }

    private void runMessageLoop(int nextExpectedVersion, Object sessionIdentifier) throws InterruptedException, GapInVersionNumbersException, DifferentSessionIdentifierException, TimeoutException {
        while(true) {
            try {
                UpdateDescriptor update = transportStrategy.fetchNextUpdate();

                if(update instanceof HeartbeatUpdateDescriptor) {
                    checkForSessionIdMismatch(update, sessionIdentifier);
                } else {
                    checkUpdate(update, nextExpectedVersion, sessionIdentifier);
                    slaveCacheService.replay(update);
                    nextExpectedVersion++;
                }

            } catch (TransportStrategy.TransportFailedException e) {
                logger.warn("Transport failed, let's retry");
            }
        }
    }

    private void checkUpdate(UpdateDescriptor update, int nextExpectedVersion, Object sessionIdentifier) throws DifferentSessionIdentifierException, GapInVersionNumbersException {
        checkForSessionIdMismatch(update, sessionIdentifier);

        checkForVersionMismatch(update, nextExpectedVersion);
    }

    private void checkForVersionMismatch(UpdateDescriptor update, int nextExpectedVersion) throws GapInVersionNumbersException {
        if(update.getVersion() > nextExpectedVersion) {
            throw new GapInVersionNumbersException(nextExpectedVersion, update.getVersion());
        }
    }

    private void checkForSessionIdMismatch(UpdateDescriptor update, Object sessionIdentifier) throws DifferentSessionIdentifierException {
        if(!update.getSessionIdentifier().equals(sessionIdentifier)) {
            throw new DifferentSessionIdentifierException(sessionIdentifier, update.getSessionIdentifier());
        }
    }

    private final Runnable SYNC = new Runnable() {
        public void run() {
            try {
                while(true) {
                    try {
                        syncCache();
                    } catch(InterruptedException e) {
                        throw e;
                    } catch (Exception e) {
                        logger.info("Need to resync", e);
                    }
                }
            } catch (InterruptedException e) {
                logger.info("Interrupted, breaking the sync loop");
            }
        }
    };


    private static class DifferentSessionIdentifierException extends Exception {
        private static final long serialVersionUID = -6674439664789621248L;

        private DifferentSessionIdentifierException(Object expected, Object actual) {
            super("Different Session Identifier, expected "+ expected + ", actual " + actual);
        }
    }

    private static class GapInVersionNumbersException extends Exception {
        private static final long serialVersionUID = -6674439664789621248L;

        private GapInVersionNumbersException(int expected, int actual) {
            super("Gap in sequence numbers, expected "+ expected + ", actual " + actual);
        }
    }

    public void setSnapshotBackoffPause(int snapshotBackoffPause) {
        this.snapshotBackoffPause = snapshotBackoffPause;
    }


}
