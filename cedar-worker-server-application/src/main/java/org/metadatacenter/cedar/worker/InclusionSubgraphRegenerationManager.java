package org.metadatacenter.cedar.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.rest.context.CedarRequestContext;
import org.metadatacenter.rest.context.CedarRequestContextFactory;
import org.metadatacenter.server.search.util.RegenerateInclusionSubgraphTask;
import org.metadatacenter.server.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class InclusionSubgraphRegenerationManager implements Managed {

  private static final Logger log = LoggerFactory.getLogger(InclusionSubgraphRegenerationManager.class);
  private static final int MAX_RETAINED_JOBS = 100;

  @FunctionalInterface
  interface Regeneration {
    void run() throws Exception;
  }

  public enum Status {
    QUEUED, RUNNING, SUCCEEDED, FAILED
  }

  public static final class Job {
    private final String id;
    private final Instant submittedAt;
    private volatile Status status;
    private volatile Instant startedAt;
    private volatile Instant completedAt;
    private volatile String error;

    private Job() {
      id = UUID.randomUUID().toString();
      submittedAt = Instant.now();
      status = Status.QUEUED;
    }

    public String getId() { return id; }
    public Instant getSubmittedAt() { return submittedAt; }
    public Status getStatus() { return status; }
    public Instant getStartedAt() { return startedAt; }
    public Instant getCompletedAt() { return completedAt; }
    public String getError() { return error; }

    private boolean isActive() {
      return status == Status.QUEUED || status == Status.RUNNING;
    }
  }

  public record StartResult(boolean accepted, Job job) {}

  private final Regeneration regeneration;
  private final Map<String, Job> jobs = new LinkedHashMap<>();
  private ExecutorService executor;
  private Job activeJob;

  public InclusionSubgraphRegenerationManager(CedarConfig cedarConfig, UserService userService) {
    this(() -> {
      CedarRequestContext context = CedarRequestContextFactory.fromAdminUser(cedarConfig, userService);
      new RegenerateInclusionSubgraphTask(cedarConfig).regenerateInclusionSubgraph(context);
    });
  }

  InclusionSubgraphRegenerationManager(Regeneration regeneration) {
    this.regeneration = regeneration;
  }

  @Override
  public synchronized void start() {
    if (executor == null || executor.isShutdown()) {
      executor = Executors.newSingleThreadExecutor();
    }
  }

  public synchronized StartResult submit() {
    if (activeJob != null && activeJob.isActive()) {
      return new StartResult(false, activeJob);
    }
    if (executor == null || executor.isShutdown()) {
      throw new IllegalStateException("The inclusion-subgraph job manager is not running");
    }
    Job job = new Job();
    activeJob = job;
    jobs.put(job.id, job);
    trimHistory();
    executor.submit(() -> execute(job));
    return new StartResult(true, job);
  }

  public synchronized Optional<Job> find(String id) {
    return Optional.ofNullable(jobs.get(id));
  }

  private void execute(Job job) {
    job.startedAt = Instant.now();
    job.status = Status.RUNNING;
    Status terminalStatus;
    try {
      regeneration.run();
      terminalStatus = Status.SUCCEEDED;
    } catch (Exception e) {
      job.error = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
      terminalStatus = Status.FAILED;
      log.error("Inclusion-subgraph regeneration job {} failed", job.id, e);
    }
    job.completedAt = Instant.now();
    // Publish the terminal state last so a status reader cannot see completion without its
    // timestamp (or a failure without its error detail).
    job.status = terminalStatus;
  }

  private void trimHistory() {
    while (jobs.size() > MAX_RETAINED_JOBS) {
      String oldest = jobs.keySet().iterator().next();
      if (activeJob != null && oldest.equals(activeJob.id)) {
        return;
      }
      jobs.remove(oldest);
    }
  }

  @Override
  public synchronized void stop() throws InterruptedException {
    if (executor != null) {
      executor.shutdown();
      if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    }
  }
}
