package org.metadatacenter.cedar.worker;

import com.codahale.metrics.health.HealthCheck;
import org.junit.jupiter.api.Test;
import org.metadatacenter.worker.QueueProcessorMonitor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerHealthChecksTest {

  @Test
  void queueHealthRequiresLiveConsumers() {
    QueueProcessorMonitor live = monitor("live", true, null, Instant.now());
    QueueProcessorMonitor stopped = monitor("stopped", false, null, null);

    assertTrue(new WorkerQueueConsumersHealthCheck(List.of(live)).check().isHealthy());
    assertFalse(new WorkerQueueConsumersHealthCheck(List.of(stopped)).check().isHealthy());
  }

  @Test
  void queueHealthKeepsAnUnrecoveredProcessingFailureVisible() {
    Instant failure = Instant.now();
    QueueProcessorMonitor failed = monitor("failed", true, failure, failure.minusSeconds(1));
    QueueProcessorMonitor recovered = monitor("recovered", true, failure, failure.plusSeconds(1));

    assertFalse(new WorkerQueueConsumersHealthCheck(List.of(failed)).check().isHealthy());
    assertTrue(new WorkerQueueConsumersHealthCheck(List.of(recovered)).check().isHealthy());
  }

  /**
   * The behaviour this whole split exists for. Dropwizard 500s the admin endpoint when any check is
   * unhealthy, cedar-services.sh accepts only 200 as ready, and prod's 54 parked messages therefore
   * made a worker that had been listening since boot sit out the full 240-second start budget.
   */
  @Test
  void parkedMessagesDoNotMakeTheServerUnhealthy() {
    HealthCheck.Result result = new WorkerDeadLetterHealthCheck(Map.of("app-log", () -> 54L)).check();

    assertTrue(result.isHealthy(), "a parked message is a backlog, not a server that cannot serve");
    assertTrue(result.getMessage().contains("app-log 54"), "and the depth is still named: " + result.getMessage());
  }

  @Test
  void anEmptyDeadLetterQueueSaysSoPlainly() {
    HealthCheck.Result result =
        new WorkerDeadLetterHealthCheck(Map.of("app-log", () -> 0L, "search-permission", () -> 0L)).check();

    assertTrue(result.isHealthy());
    assertEquals("No dead-lettered messages", result.getMessage());
  }

  @Test
  void everyNonEmptyQueueIsNamed() {
    HealthCheck.Result result = new WorkerDeadLetterHealthCheck(Map.of(
        "app-log", () -> 54L,
        "search-permission", () -> 3L,
        "clone-instances", () -> 0L)).check();

    assertTrue(result.getMessage().contains("app-log 54"));
    assertTrue(result.getMessage().contains("search-permission 3"));
    assertFalse(result.getMessage().contains("clone-instances"), "an empty queue is not worth naming");
  }

  /** Sorted, so an operator comparing two polls is reading the same string, not a reshuffled one. */
  @Test
  void queuesAreNamedInAStableOrder() {
    HealthCheck.Result result = new WorkerDeadLetterHealthCheck(Map.of(
        "value-recommender", () -> 1L,
        "app-log", () -> 2L,
        "search-permission", () -> 3L)).check();

    String message = result.getMessage();
    assertTrue(message.indexOf("app-log") < message.indexOf("search-permission"));
    assertTrue(message.indexOf("search-permission") < message.indexOf("value-recommender"));
  }

  /**
   * Redis being unreachable is the redis check's failure to report, not this one's. Reporting it
   * here as well would fail the server twice for one cause.
   */
  @Test
  void anUnreadableDepthIsReportedWithoutFailingTheServer() {
    HealthCheck.Result result = new WorkerDeadLetterHealthCheck(
        Map.of("app-log", () -> { throw new IllegalStateException("Redis is gone"); })).check();

    assertTrue(result.isHealthy());
    assertTrue(result.getMessage().contains("app-log"));
    assertTrue(result.getMessage().contains("Redis is gone"));
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
