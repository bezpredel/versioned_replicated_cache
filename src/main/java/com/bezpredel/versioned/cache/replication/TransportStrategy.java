package com.bezpredel.versioned.cache.replication;

import com.bezpredel.versioned.cache.UpdateDescriptor;

import java.util.concurrent.TimeoutException;

public interface TransportStrategy {
    SnapshotUpdateDescriptor retrieveSnapshot() throws WillNotBeAvailableRightAwayException, InterruptedException, TimeoutException/*, general exception?*/;
    UpdateDescriptor fetchNextUpdate() throws TimeoutException, InterruptedException, TransportFailedException;

    /**
     *
     * @return won't return HeartbeatUpdateDescriptor
     */
    UpdateDescriptor tryFetchNextUpdate();

    public static class WillNotBeAvailableRightAwayException extends Exception {
        private static final long serialVersionUID = 237234943661631876L;

        public WillNotBeAvailableRightAwayException() {
        }

        public WillNotBeAvailableRightAwayException(String message) {
            super(message);
        }

        public WillNotBeAvailableRightAwayException(String message, Throwable cause) {
            super(message, cause);
        }

        public WillNotBeAvailableRightAwayException(Throwable cause) {
            super(cause);
        }
    }

    public static class TransportFailedException extends Exception {
        private static final long serialVersionUID = 889709768672544491L;

        public TransportFailedException() {
        }

        public TransportFailedException(String message) {
            super(message);
        }

        public TransportFailedException(String message, Throwable cause) {
            super(message, cause);
        }

        public TransportFailedException(Throwable cause) {
            super(cause);
        }
    }
}
