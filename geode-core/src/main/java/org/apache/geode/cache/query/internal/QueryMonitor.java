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

public class QueryMonitor {
  public QueryMonitor(final long maxQueryExecutionTime) {
    // TODO: Timeout a query if maxQueryExecutionTime is exceeded
  }

  /**
   * Start monitoring the query.
   */
  public void startMonitoringQuery(final ExecutionContext executionContext) {
    // TODO: Implement monitor query
  }

  /**
   * Stop monitoring the query.
   */
  public void stopMonitoringQuery(final ExecutionContext executionContext) {
    // TODO: Implement stop monitoring query
  }

  /**
   * Throw an exception if the query has been canceled. The {@link QueryMonitor} cancels the query
   * if it takes more than the max query execution time.
   *
   * @throws QueryExecutionCanceledException if the query has been canceled
   */
  public void throwExceptionIfQueryCanceled() {
    // TODO: Throw a QueryExecutionCanceledException if the query has been canceled
  }

  /**
   * Stops query monitoring. Makes this {@link QueryMonitor} unusable for further monitoring.
   */
  public void stopMonitoring() {
    // TODO: Implement stop monitoring query
  }
}
