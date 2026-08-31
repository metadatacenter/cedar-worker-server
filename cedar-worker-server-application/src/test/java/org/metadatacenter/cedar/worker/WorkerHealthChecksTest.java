package org.metadatacenter.cedar.worker;

import org.junit.jupiter.api.Test;
import org.metadatacenter.worker.QueueProcessorMonitor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerHealthChecksTest {

  @Test
  void queueHealthRequiresLiveConsumersAndEmptyDeadLetterQueues() {
    QueueProcessorMonitor live = monitor("live", true, null, Instant.now());
    QueueProcessorMonitor stopped = monitor("stopped", false, null, null);

    assertTrue(new WorkerQueueConsumersHealthCheck(List.of(live), Map.of("queue", () -> 0)).check().isHealthy());
    assertFalse(new WorkerQueueConsumersHealthCheck(List.of(stopped), Map.of("queue", () -> 0)).check().isHealthy());
    assertFalse(new WorkerQueueConsumersHealthCheck(List.of(live), Map.of("queue", () -> 2)).check().isHealthy());
  }

  @Test
  void queueHealthKeepsAnUnrecoveredProcessingFailureVisible() {
    Instant failure = Instant.now();
    QueueProcessorMonitor failed = monitor("failed", true, failure, failure.minusSeconds(1));
    QueueProcessorMonitor recovered = monitor("recovered", true, failure, failure.plusSeconds(1));

    assertFalse(new WorkerQueueConsumersHealthCheck(List.of(failed), Map.of()).check().isHealthy());
    assertTrue(new WorkerQueueConsumersHealthCheck(List.of(recovered), Map.of()).check().isHealthy());
  }

  private static QueueProcessorMonitor monitor(String name, boolean running, Instant failure, Instant success) {
    return new QueueProcessorMonitor() {
      @Override public String getProcessorName() { return name; }
      @Override public boolean isRunning() { return running; }
      @Override public Instant getLastFailureAt() { return failure; }
      @Override public Instant getLastSuccessAt() { return success; }
    };
  }
}
