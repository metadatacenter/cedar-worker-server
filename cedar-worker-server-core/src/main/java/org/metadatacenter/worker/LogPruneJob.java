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
 * window (plan §5.2). Separate thread from the aggregator. Disabled by default ({@code CEDAR_LOG_PRUNE_ENABLED})
 * because deletion is the one irreversible step; turn it on once the rollups are trusted.
 * <p>
 * Each pass walks each table's ids from the lowest up to the first row it must keep, one primary-key window of
 * {@code CEDAR_LOG_PRUNE_BATCH} ids per statement and transaction, pausing after each window that deleted something.
 * A statement therefore locks at most one window's rows. The old {@code DELETE ... ORDER BY id LIMIT n} let the
 * optimizer pick the aggregatedAt index and locked ~10.4M rows per 1000-row batch on prod (2026-10-16), until it
 * timed out. Naturally resumable: a pass deletes whatever matches when it reaches it, and the next starts over.
 */
public class LogPruneJob implements Managed {

  private static final Logger log = LoggerFactory.getLogger(LogPruneJob.class);
  /** Log a long pass's progress every this many windows. */
  private static final int PROGRESS_EVERY = 1000;

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
    log.info("LogPruneJob starting: retentionDays={}, window={} ids, pauseMs={}", retentionDays, batchSize, pauseMs);
    while (running) {
      try {
        Instant cutoff = Instant.now().minus(retentionDays, ChronoUnit.DAYS);
        long d1 = prunePass(true, cutoff);
        long d2 = running ? prunePass(false, cutoff) : 0;
        if (d1 + d2 > 0) {
          log.info("Pruned {} request + {} cypher rows older than {} days.", d1, d2, retentionDays);
        }
      } catch (Exception e) {
        log.error("LogPruneJob pass failed; retrying after the idle interval.", e);
      }
      if (!sleep(idleMs)) {
        return;
      }
    }
  }

  /**
   * One pass over one table.
   *
   * @param request true = log_request, false = log_cypher
   * @return rows deleted
   */
  long prunePass(boolean request, Instant cutoff) {
    String table = request ? "log_request" : "log_cypher";
    long[] range = request ? service.requestPruneRange(cutoff) : service.cypherPruneRange(cutoff);
    long from = range[0];
    long to = range[1];
    long deleted = 0;
    long windows = 0;
    while (running && from >= 0 && from < to) {
      long[] window = request ? service.pruneRequestWindow(from, to, cutoff, batchSize)
          : service.pruneCypherWindow(from, to, cutoff, batchSize);
      deleted += window[1];
      from = window[0];
      if (++windows % PROGRESS_EVERY == 0) {
        log.info("Pruning {}: at id {} of {}, {} rows deleted so far.", table, from, to, deleted);
      }
      if (window[1] > 0 && !sleep(pauseMs)) {
        break;
      }
    }
    return deleted;
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
