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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.internal.concurrent.ConcurrentHashSet;

public class QueryMonitor {
  private long maxQueryExecutionTime;
  private Set<IExecutionContext> monitoredExecutionContexts;
  private Boolean isCanceled;

  public QueryMonitor(final long maxQueryExecutionTime) {
    this.maxQueryExecutionTime = maxQueryExecutionTime;
    this.monitoredExecutionContexts = new ConcurrentHashSet<>();
    isCanceled = false;
  }

  /**
   * Start monitoring the query.
   */
  public void startMonitoringQuery(final IExecutionContext executionContext) {
    monitoredExecutionContexts.add(executionContext);
    executionContext.setStartTime(System.currentTimeMillis());
  }

  /**
   * Stop monitoring the query.
   */
  public void stopMonitoringQuery(final IExecutionContext executionContext) {
    monitoredExecutionContexts.remove(executionContext);
    executionContext.setStartTime(-1);
  }

  /**
   * Throw an exception if the query has been canceled. The {@link QueryMonitor} cancels the query
   * if it takes more than the max query execution time.
   *
   * @throws QueryExecutionCanceledException if the query has been canceled
   */
  public void throwExceptionIfQueryCanceled(final IExecutionContext executionContext) {
    if (executionContext.getStartTime() < 0) {
      return;
    }

    if (isCanceled || (System.currentTimeMillis() - executionContext.getStartTime()) >= this.maxQueryExecutionTime) {
      throw new RuntimeException();
    }
  }

  /**
   * Stops query monitoring. Makes this {@link QueryMonitor} unusable for further monitoring.
   */
  public void stopMonitoring() {
    for (IExecutionContext executionContext : monitoredExecutionContexts) {
      stopMonitoringQuery(executionContext);
    }
  }

  /**
   * Cancel all queries that the monitor knows so that they terminate on next throwExceptionIfQueryCanceled
   */
  public void cancelAllQueries() {
    isCanceled = true;
  }
}
