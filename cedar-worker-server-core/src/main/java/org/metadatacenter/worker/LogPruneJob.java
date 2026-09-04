package org.metadatacenter.worker;

import org.metadatacenter.config.environment.CedarEnvironmentVariable;
import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.logging.agg.LogAggregationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Deletes raw log rows that are BOTH aggregated ({@code aggregatedAt} set) AND older than the retention
 * window (plan §5.2). Separate thread from the aggregator, bounded batches with a pause between them, so
 * it never holds a long lock or spikes I/O. Naturally resumable — it just deletes whatever currently
 * matches. Disabled by default ({@code CEDAR_LOG_PRUNE_ENABLED}) because deletion is the one
 * irreversible step; turn it on once the rollups are trusted.
 */
public class LogPruneJob implements Managed {

  private static final Logger log = LoggerFactory.getLogger(LogPruneJob.class);

  private final LogAggregationService service;
  private final boolean enabled;
  private final int retentionDays;
  private final int batchSize;
  private final long pauseMs;
  private final long idleMs;

  private volatile boolean running = true;
  private ExecutorService executor;

  public LogPruneJob(LogAggregationService service) {
    this.service = service;
    this.enabled = Boolean.parseBoolean(JobEnvironment.read(CedarEnvironmentVariable.CEDAR_LOG_PRUNE_ENABLED, "false"));
    this.retentionDays = parseIntSafe(JobEnvironment.read(CedarEnvironmentVariable.CEDAR_LOG_PRUNE_RETENTION_DAYS, "30"), 30);
    this.batchSize = parseIntSafe(JobEnvironment.read(CedarEnvironmentVariable.CEDAR_LOG_PRUNE_BATCH, "2000"), 2000);
    this.pauseMs = parseLongSafe(JobEnvironment.read(CedarEnvironmentVariable.CEDAR_LOG_PRUNE_PAUSE_MS, "500"), 500L);
    this.idleMs = parseLongSafe(JobEnvironment.read(CedarEnvironmentVariable.CEDAR_LOG_PRUNE_IDLE_MS, "3600000"), 3_600_000L);
  }

  @Override
  public void start() {
    executor = Executors.newSingleThreadExecutor();
    executor.submit(this::run);
  }

  @Override
  public void stop() {
    log.info("LogPruneJob.stop()");
    running = false;
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  private void run() {
    if (!enabled) {
      log.info("LogPruneJob disabled (set CEDAR_LOG_PRUNE_ENABLED=true to run).");
      return;
    }
    log.info("LogPruneJob starting: retentionDays={}, batch={}, pauseMs={}", retentionDays, batchSize, pauseMs);
    while (running) {
      int deleted;
      try {
        Instant cutoff = Instant.now().minus(retentionDays, ChronoUnit.DAYS);
        int d1 = service.pruneRequests(cutoff, batchSize);
        int d2 = service.pruneCypher(cutoff, batchSize);
        deleted = d1 + d2;
        if (deleted > 0) {
          log.info("Pruned {} request + {} cypher rows older than {} days.", d1, d2, retentionDays);
        }
      } catch (Exception e) {
        log.error("LogPruneJob batch failed; retrying after the idle interval.", e);
        deleted = 0;
      }
      long wait = deleted > 0 ? pauseMs : idleMs;
      if (!sleep(wait)) {
        return;
      }
    }
  }

  private boolean sleep(long ms) {
    try {
      Thread.sleep(ms);
      return running;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }


  private static int parseIntSafe(String s, int dflt) {
    try {
      return Integer.parseInt(s.trim());
    } catch (RuntimeException e) {
      return dflt;
    }
  }

  private static long parseLongSafe(String s, long dflt) {
    try {
      return Long.parseLong(s.trim());
    } catch (RuntimeException e) {
      return dflt;
    }
  }
}
