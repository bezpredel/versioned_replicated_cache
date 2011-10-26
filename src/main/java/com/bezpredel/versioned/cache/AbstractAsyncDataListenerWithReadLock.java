package com.bezpredel.versioned.cache;

import com.bezpredel.collections.Pair;
import com.bezpredel.versioned.datastore.AsyncCommand;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

public abstract class AbstractAsyncDataListenerWithReadLock implements SingleCacheService.DataListener {
    private final Executor executor;

    public AbstractAsyncDataListenerWithReadLock(Executor executor) {
        this.executor = executor;
    }

    public void onDataChanged(SingleCacheService source, int version, List<Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>>> changes) {
        AsyncCommand<Object> task = source.startAsyncRead(new Task(version, changes));

        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            task.cancel();
        }
    }

    protected abstract void doWork(int writeVersion, List<Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>>> changes, SingleCacheService.ReadContext context);

    private class Task implements SingleCacheService.ReadCommand<Object> {
        private final int version;
        private final List<Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>>> changes;

        private Task(int version, List<Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>>> changes) {
            this.version = version;
            this.changes = changes;
        }

        public Object execute(SingleCacheService.ReadContext context) {
            doWork(version, changes, context);
            return null;
        }
    }
}