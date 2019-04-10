package org.apache.geode.cache.query.internal;

public interface IExecutionContext {
  long getStartTime();

  void setStartTime(long startTime);
}
