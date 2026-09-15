package org.metadatacenter.cedar.worker;

import com.codahale.metrics.health.HealthCheck;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * How many messages are parked on each dead-letter queue.
 * <p>
 * <strong>Reporting, not gating</strong>, in the sense {@code CedarDependencyHealthCheck} defines:
 * the depth appears in the health message where operators and the Monitor can see it, and the result
 * stays healthy.
 * <p>
 * This was a gating condition until 2026-09-15, folded into {@link WorkerQueueConsumersHealthCheck},
 * and the cost of that was paid on every deploy. Dropwizard's admin endpoint returns 500 when any
 * check is unhealthy; {@code cedar-services.sh} derives readiness from that endpoint and accepts only
 * 200. So a single parked message made {@code cedarcli native start microservices} wait out its full
 * 240-second budget on a worker that had been listening since {@code Started Server @658ms}, and then
 * report the start as failed. Prod's 54 parked messages did exactly that on 2026-09-15, and clearing
 * the queue produced {@code ready worker} on the very next poll, with no restart.
 * <p>
 * The distinction that makes this right is between a server that cannot work and a backlog that
 * needs attention. A parked message is the second: it is a payload safely set aside after
 * {@code MAX_HANDLING_ATTEMPTS} failures, it is not lost, nothing about it stops the consumer from
 * draining everything behind it, and no deploy is made safer by refusing to finish until someone
 * clears it. It still needs to be seen, which is what this check and the Monitor's queue-count page
 * are for - the page reports {@code -dead-letter} and {@code -processing} depths alongside the
 * pending ones for the same reason.
 * <p>
 * The depth is read live on every call, so clearing a queue turns the message green with no restart.
 */
public class WorkerDeadLetterHealthCheck extends HealthCheck {

  @FunctionalInterface
  public interface QueueDepthProbe {
    long count() throws Exception;
  }

  private final Map<String, QueueDepthProbe> deadLetterQueues;

  public WorkerDeadLetterHealthCheck(Map<String, QueueDepthProbe> deadLetterQueues) {
    this.deadLetterQueues = Map.copyOf(deadLetterQueues);
  }

  @Override
  protected Result check() {
    // Sorted so a message that names several queues reads the same way every poll, rather than in
    // whatever order the map happens to iterate.
    List<String> parked = new ArrayList<>();
    List<String> unreadable = new ArrayList<>();
    for (Map.Entry<String, QueueDepthProbe> queue : new TreeMap<>(deadLetterQueues).entrySet()) {
      try {
        long count = queue.getValue().count();
        if (count > 0) {
          parked.add(queue.getKey() + " " + count);
        }
      } catch (Exception e) {
        unreadable.add(queue.getKey() + " (" + e.getMessage() + ")");
      }
    }

    if (parked.isEmpty() && unreadable.isEmpty()) {
      return Result.healthy("No dead-lettered messages");
    }

    List<String> conditions = new ArrayList<>();
    if (!parked.isEmpty()) {
      conditions.add("Dead-lettered messages waiting: " + String.join(", ", parked)
          + ". The server keeps serving; see LOG-QUEUE-RUNBOOK.md 4b before clearing them");
    }
    if (!unreadable.isEmpty()) {
      // Redis being unreadable is a real problem, but it is the redis check's to report: two checks
      // failing for one cause name the cause twice and fix it no faster.
      conditions.add("Could not read the dead-letter depth of " + String.join(", ", unreadable));
    }
    return Result.healthy(String.join("; ", conditions));
  }
}
