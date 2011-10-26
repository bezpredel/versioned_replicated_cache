package com.bezpredel.versioned.cache;

import com.bezpredel.collections.Pair;

import java.util.List;
import java.util.concurrent.Executor;

public class AsyncDataListenerAdaptor implements SingleCacheService.DataListener {
    private final Executor executor;
    private final SingleCacheService.DataListener delegate;

    public AsyncDataListenerAdaptor(Executor executor, SingleCacheService.DataListener delegate) {
        this.executor = executor;
        this.delegate = delegate;
    }


    public void onDataChanged(final SingleCacheService source, final int version, final List<Pair<ImmutableCacheableObject<BasicCacheIdentifier>, ImmutableCacheableObject<BasicCacheIdentifier>>> changes) {
        executor.execute(
                new Runnable() {
                    public void run() {
                        delegate.onDataChanged(source, version, changes);
                    }
                }
        );
    }
}
