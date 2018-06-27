package org.apache.geode.api;

import java.util.concurrent.CompletableFuture;

public interface AsyncRegion<K,V> {
    CompletableFuture<V> get(K key);

    /**
     * @return the previous value
     */
    CompletableFuture<V> put(K key, V value);
}
