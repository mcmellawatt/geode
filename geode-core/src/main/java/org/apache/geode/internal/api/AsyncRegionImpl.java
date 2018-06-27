/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.api;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.geode.api.AsyncRegion;
import org.apache.geode.cache.Region;

public class AsyncRegionImpl<K, V> implements AsyncRegion<K, V> {
  private final Region<K, V> region;
  private final ExecutorService threadPool;

  public AsyncRegionImpl(Region<K, V> region) {
    this.region = region;
    threadPool = Executors.newCachedThreadPool();
  }

  private <T> CompletableFuture<T> asyncExecute(Callable<T> callable) {
    CompletableFuture<T> getFuture = new CompletableFuture<>();

    threadPool.submit(() -> {
      try {
        getFuture.complete(callable.call());
      } catch (Exception e) {
        getFuture.completeExceptionally(e);
      }
    });

    return getFuture;
  }

  @Override
  public CompletableFuture<V> get(K key) {
    return asyncExecute(() -> region.get(key));
  }

  @Override
  public CompletableFuture<V> put(K key, V value) {
    return asyncExecute(() -> region.put(key, value));
  }

  @Override
  public CompletableFuture<V> remove(K key) {
    return asyncExecute(() -> region.remove(key));
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return asyncExecute(() -> region.containsKey(key));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    return asyncExecute(() -> region.containsValue(value));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return asyncExecute(() -> region.size());
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return asyncExecute(() -> region.isEmpty());
  }

  @Override
  public CompletableFuture<Void> putAll(Map<? extends K, ? extends V> m) {
    return asyncExecute(() -> region.putAll(m));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return asyncExecute(() -> region.clear());
  }
}
