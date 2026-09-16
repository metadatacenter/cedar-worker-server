package org.metadatacenter.cedar.worker;

import com.codahale.metrics.health.HealthCheck;
import org.metadatacenter.worker.QueueProcessorMonitor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Whether this server's queue consumers are running and processing.
 * <p>
 * <strong>Gating</strong>, in the sense {@code CedarDependencyHealthCheck} defines: a consumer that
 * has stopped, or whose last attempt failed and has not since succeeded, is a server not doing its
 * job, and a deploy should not be called finished while one is in that state.
 * <p>
 * Dead-letter depth used to be checked here too and is not any more - see
 * {@link WorkerDeadLetterHealthCheck} for why parking a message is not the same kind of condition.
 */
public class WorkerQueueConsumersHealthCheck extends HealthCheck {

  private final List<QueueProcessorMonitor> processors;

  public WorkerQueueConsumersHealthCheck(List<QueueProcessorMonitor> processors) {
    this.processors = List.copyOf(processors);
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
    return problems.isEmpty() ? Result.healthy("All queue consumers are running")
        : Result.unhealthy(String.join("; ", problems));
  }
}
