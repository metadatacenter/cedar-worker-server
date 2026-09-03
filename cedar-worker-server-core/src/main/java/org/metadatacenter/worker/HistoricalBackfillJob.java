package org.metadatacenter.worker;

import org.metadatacenter.config.environment.CedarEnvironmentVariable;
import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.logging.agg.LogAggregationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;

/**
 * One-time drain of the frozen {@code *_pre284} history into the {@code agg_*} rollups (plan §5.3).
 * <p>
 * Lifecycle mirrors {@link AppLoggerQueueProcessor}: a single background thread, a {@code running}
 * flag flipped on shutdown. It is throttled (small batches + a pause) and confined to an off-peak UTC
 * window so it never overloads the log DB, and it is restartable — the cursor lives in
 * {@code log_aggregation_state}, so a crash or a window boundary just resumes where it left off.
 * <p>
 * Disabled by default; enable deliberately with {@code CEDAR_LOG_BACKFILL_ENABLED=true}. On drain it
 * marks the source {@code READY_TO_DROP}; the actual {@code DROP TABLE} is a manual step.
 */
public class HistoricalBackfillJob implements Managed {

  private static final Logger log = LoggerFactory.getLogger(HistoricalBackfillJob.class);
  private static final long OUT_OF_WINDOW_SLEEP_MS = 60_000L;

  private final LogAggregationService service;
  private final boolean enabled;
  private final int batchSize;
  private final long pauseMs;
  private final int windowStartHourUtc;
  private final int windowEndHourUtc; // == start means "always on"

  private volatile boolean running = true;
  private ExecutorService executor;

  public HistoricalBackfillJob(LogAggregationService service) {
    this.service = service;
    this.enabled = Boolean.parseBoolean(JobEnvironment.read(CedarEnvironmentVariable.CEDAR_LOG_BACKFILL_ENABLED, "false"));
    this.batchSize = parseIntSafe(JobEnvironment.read(CedarEnvironmentVariable.CEDAR_LOG_BACKFILL_BATCH, "5000"), 5000);
    this.pauseMs = parseLongSafe(JobEnvironment.read(CedarEnvironmentVariable.CEDAR_LOG_BACKFILL_PAUSE_MS, "500"), 500L);
    int[] w = parseWindow(JobEnvironment.read(CedarEnvironmentVariable.CEDAR_LOG_BACKFILL_WINDOW_UTC, "2-6"));
    this.windowStartHourUtc = w[0];
    this.windowEndHourUtc = w[1];
  }

  @Override
  public void start() {
    executor = Executors.newSingleThreadExecutor();
    executor.submit(this::run);
  }

  @Override
  public void stop() {
    log.info("HistoricalBackfillJob.stop()");
    running = false;
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  private void run() {
    if (!enabled) {
      log.info("HistoricalBackfillJob disabled (set CEDAR_LOG_BACKFILL_ENABLED=true to run).");
      return;
    }
    log.info("HistoricalBackfillJob starting: batch={}, pauseMs={}, windowUtc={}-{}",
        batchSize, pauseMs, windowStartHourUtc, windowEndHourUtc);
    try {
      service.initBackfillState(LogAggregationService.SRC_REQUEST_HIST);
      service.initBackfillState(LogAggregationService.SRC_CYPHER_HIST);
      drain(LogAggregationService.SRC_REQUEST_HIST, () -> service.runBackfillRequestBatch(batchSize));
      drain(LogAggregationService.SRC_CYPHER_HIST, () -> service.runBackfillCypherBatch(batchSize));
      if (running) {
        // keep history's worst instances before the *_pre284 tables are (manually) dropped
        service.captureHistoryOutliers();
      }
      log.info("HistoricalBackfillJob finished.");
    } catch (Exception e) {
      // best-effort, like the queue consumer: log and stop; next boot resumes from the cursor
      log.error("HistoricalBackfillJob aborted; will resume from the persisted cursor on next start.", e);
    }
  }

  private void drain(String sourceTable, LongSupplier batch) {
    while (running) {
      if (!inWindow()) {
        if (!sleep(OUT_OF_WINDOW_SLEEP_MS)) {
          return;
        }
        continue;
      }
      long rowsRead = batch.getAsLong();
      if (rowsRead == 0) {
        service.markBackfillComplete(sourceTable);
        log.info("Drained {} -> READY_TO_DROP.", sourceTable);
        return;
      }
      if (!sleep(pauseMs)) {
        return;
      }
    }
  }

  private boolean inWindow() {
    if (windowStartHourUtc == windowEndHourUtc) {
      return true; // always on
    }
    int h = Instant.now().atZone(ZoneOffset.UTC).getHour();
    if (windowStartHourUtc < windowEndHourUtc) {
      return h >= windowStartHourUtc && h < windowEndHourUtc;
    }
    return h >= windowStartHourUtc || h < windowEndHourUtc; // window wraps past midnight
  }

  /** @return true if the sleep completed, false if interrupted/stopping. */
  private boolean sleep(long ms) {
    try {
      Thread.sleep(ms);
      return running;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }


  private static int[] parseWindow(String spec) {
    try {
      if (spec == null || spec.isBlank()) {
        return new int[]{0, 0};
      }
      String[] parts = spec.split("-");
      int s = Integer.parseInt(parts[0].trim()) % 24;
      int e = Integer.parseInt(parts[1].trim()) % 24;
      return new int[]{s, e};
    } catch (RuntimeException ex) {
      return new int[]{2, 6};
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
