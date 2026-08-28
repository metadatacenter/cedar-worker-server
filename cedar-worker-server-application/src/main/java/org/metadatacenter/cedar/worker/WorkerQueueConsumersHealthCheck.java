package org.metadatacenter.cedar.worker;

import com.codahale.metrics.health.HealthCheck;
import org.metadatacenter.worker.QueueProcessorMonitor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WorkerQueueConsumersHealthCheck extends HealthCheck {

  @FunctionalInterface
  public interface QueueDepthProbe {
    long count() throws Exception;
  }

  private final List<QueueProcessorMonitor> processors;
  private final Map<String, QueueDepthProbe> deadLetterQueues;

  public WorkerQueueConsumersHealthCheck(List<QueueProcessorMonitor> processors,
                                         Map<String, QueueDepthProbe> deadLetterQueues) {
    this.processors = List.copyOf(processors);
    this.deadLetterQueues = Map.copyOf(deadLetterQueues);
  }

  @Override
  protected Result check() {
    List<String> problems = new ArrayList<>();
    for (QueueProcessorMonitor processor : processors) {
      if (!processor.isRunning()) {
        problems.add(processor.getProcessorName() + " consumer is not running");
      }
      Instant failure = processor.getLastFailureAt();
      Instant success = processor.getLastSuccessAt();
      if (failure != null && (success == null || failure.isAfter(success))) {
        problems.add(processor.getProcessorName() + " last failed at " + failure);
      }
    }
    for (Map.Entry<String, QueueDepthProbe> queue : deadLetterQueues.entrySet()) {
      try {
        long count = queue.getValue().count();
        if (count > 0) {
          problems.add(queue.getKey() + " dead-letter queue contains " + count + " message(s)");
        }
      } catch (Exception e) {
        problems.add("could not read " + queue.getKey() + " dead-letter queue: " + e.getMessage());
      }
    }
    return problems.isEmpty() ? Result.healthy("All queue consumers are running")
        : Result.unhealthy(String.join("; ", problems));
  }
}
