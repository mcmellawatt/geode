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
package org.apache.geode.cache.query.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * Test QueryMonitor, integrated with its ScheduledThreadPoolExecutor.
 *
 * Mock DefaultQuery.
 */
public class QueryMonitorIntegrationTest {

  // query expiration duration so long that the query never expires
  private static final int NEVER_EXPIRE_MILLIS = 100000;

  // much much smaller than default maximum wait time of GeodeAwaitility
  private static final int EXPIRE_QUICK_MILLIS = 1;

  private InternalCache cache;
  private ExecutionContext executionContext;
  private volatile QueryExecutionTimeoutException queryExecutionTimeoutException;

  @Before
  public void before() {
    cache = mock(InternalCache.class);
    executionContext = new QueryExecutionContext(null, cache, mock(DefaultQuery.class));
  }

  @Test
  public void setLowMemoryTrueCancelsQueriesImmediately() {

    QueryMonitor queryMonitor = null;

    ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
        new ScheduledThreadPoolExecutor(1);

    try {
      queryMonitor = new QueryMonitor(
          scheduledThreadPoolExecutor,
          cache,
          NEVER_EXPIRE_MILLIS);

      queryMonitor.monitorQueryThread(executionContext);

      queryMonitor.setLowMemory(true, 1);

      assertThatThrownBy(executionContext::throwExceptionIfCanceled,
          "Expected setLowMemory(true,_) to cancel query execution immediately, but it didn't.",
          QueryExecutionCanceledException.class);
    } finally {
      if (queryMonitor != null) {
        /*
         * Setting the low-memory state (above) sets it globally. If we fail to reset it here,
         * then subsequent tests, e.g. if we run this test class more than once in succession
         * in the same JVM, as the Geode "stress" test does, will give unexpected results.
         */
        queryMonitor.setLowMemory(false, 1);
      }
    }

    assertThat(scheduledThreadPoolExecutor.getQueue().size()).isZero();
  }

  @Test
  public void monitorQueryThreadCancelsLongRunningQueriesAndSetsExceptionAndThrowsException() {

    final QueryMonitor queryMonitor = new QueryMonitor(
        new ScheduledThreadPoolExecutor(1),
        cache,
        EXPIRE_QUICK_MILLIS);

    startQueryThread(queryMonitor, executionContext);

    GeodeAwaitility.await().until(() -> queryExecutionTimeoutException != null);

    assertThat(queryExecutionTimeoutException)
        .hasMessageContaining("canceled after exceeding max execution time");

    assertThat(queryExecutionTimeoutException).isNotNull();
  }

  private void startQueryThread(final QueryMonitor queryMonitor,
      final ExecutionContext executionContext) {

    final Thread queryThread = new Thread(() -> {
      queryMonitor.monitorQueryThread(executionContext);

      while (true) {
        try {
          executionContext.throwExceptionIfCanceled();
          Thread.sleep(5 * EXPIRE_QUICK_MILLIS);
        } catch (final QueryExecutionTimeoutException e) {
          queryExecutionTimeoutException = e;
          break;
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("Simulated query execution unexpectedly interrupted.");
        }
      }
    });

    queryThread.start();
  }
}
