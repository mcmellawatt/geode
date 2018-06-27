package org.apache.geode.internal.api;

import org.apache.geode.api.AsyncRegion;
import org.apache.geode.cache.Region;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AsyncRegionImpl<K,V> implements AsyncRegion<K, V> {
    Region<K, V> region;

    public AsyncRegionImpl(Region<K, V> region) {
        this.region = region;
    }

    @Override
    public CompletableFuture<V> get(K key) {
        CompletableFuture<V> getFuture = new CompletableFuture<>();

        Executors.newCachedThreadPool().submit(() -> {
            getFuture.complete(region.get(key));
        });

        return getFuture;
    }

    @Override
    public CompletableFuture<V> put(K key, V value) {
        CompletableFuture<V> putFuture = new CompletableFuture<>();

        Executors.newCachedThreadPool().submit(() -> {
            putFuture.complete(region.put(key, value));
        });

        return putFuture;
    }
}
