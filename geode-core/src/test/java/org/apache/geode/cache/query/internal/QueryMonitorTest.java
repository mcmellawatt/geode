package org.apache.geode.cache.query.internal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

public class QueryMonitorTest {

  private class TestExecutionContext implements IExecutionContext {
    /**
     * -1 indicates that execution context is no longer needing to monitor execution duration.
     */
    private long startTime = -1;

    @Override
    public long getStartTime() {
      return startTime;
    }

    @Override
    public void setStartTime(final long startTime) {
      this.startTime = startTime;
    }
  }

  @Test
  public void createQueryMonitorWithZeroTimeoutThrowsException() {
    TestExecutionContext executionContext = new TestExecutionContext();
    QueryMonitor queryMonitor = new QueryMonitor(0);
    queryMonitor.startMonitoringQuery(executionContext);
    assertThatThrownBy(() -> queryMonitor.throwExceptionIfQueryCanceled(executionContext))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void createQueryMonitorWithLongTimeoutDoesntThrowException() {
    TestExecutionContext executionContext = new TestExecutionContext();
    QueryMonitor queryMonitor = new QueryMonitor(Long.MAX_VALUE);
    queryMonitor.startMonitoringQuery(executionContext);
    for(int i = 0; i < 5; ++i) {
      queryMonitor.throwExceptionIfQueryCanceled(executionContext);
    }
    queryMonitor.stopMonitoringQuery(executionContext);
  }

  @Test
  public void createQueryMonitorDoesntThrowExceptionIfStopped() {
    TestExecutionContext executionContext = new TestExecutionContext();
    QueryMonitor queryMonitor = new QueryMonitor(0);
    executionContext.setStartTime(System.currentTimeMillis() - 500);
    queryMonitor.stopMonitoringQuery(executionContext);
    queryMonitor.throwExceptionIfQueryCanceled(executionContext);
  }

  @Test
  public void createMultipleQueryMonitorStopMonitoringAllIfStopped(){
    TestExecutionContext executionContext0 = new TestExecutionContext();
    TestExecutionContext executionContext1 = new TestExecutionContext();
    QueryMonitor queryMonitor = new QueryMonitor(Long.MAX_VALUE);
    queryMonitor.startMonitoringQuery(executionContext0);
    queryMonitor.startMonitoringQuery(executionContext1);
    queryMonitor.stopMonitoring();
    queryMonitor.throwExceptionIfQueryCanceled(executionContext0);
    queryMonitor.throwExceptionIfQueryCanceled(executionContext1);
  }

  @Test
  public void createMultipleQueryMonitorThrowExceptionAfterCanceled() {
    TestExecutionContext executionContext0 = new TestExecutionContext();
    TestExecutionContext executionContext1 = new TestExecutionContext();
    QueryMonitor queryMonitor = new QueryMonitor(Long.MAX_VALUE);
    queryMonitor.startMonitoringQuery(executionContext0);
    queryMonitor.startMonitoringQuery(executionContext1);
    queryMonitor.cancelAllQueries();
    assertThatThrownBy(() -> queryMonitor.throwExceptionIfQueryCanceled(executionContext0))
        .isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> queryMonitor.throwExceptionIfQueryCanceled(executionContext1))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void createQueryMonitorSimultaneousStartAndStopThreadsafe() throws InterruptedException {
    TestExecutionContext executionContext = new TestExecutionContext();
    QueryMonitor queryMonitor = new QueryMonitor(Long.MAX_VALUE);

    ArrayList<Callable<Void>> concurrentMonitorAndStopMonitorTasks = new ArrayList<>();
    ExecutorService executorService = Executors.newWorkStealingPool();

    concurrentMonitorAndStopMonitorTasks.add(() -> {
      queryMonitor.startMonitoringQuery(executionContext);
      return null;
    });

    concurrentMonitorAndStopMonitorTasks.add(() -> {
      queryMonitor.stopMonitoringQuery(executionContext);
      return null;
    });

    executorService.invokeAll(concurrentMonitorAndStopMonitorTasks);

    executorService.shutdownNow();
  }
}
