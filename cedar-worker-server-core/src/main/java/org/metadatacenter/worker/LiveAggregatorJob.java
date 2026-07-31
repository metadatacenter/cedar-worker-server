package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.logging.agg.LogAggregationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Ongoing aggregation of the live tables (plan §5.1). A catch-up scheduler: on each wake it aggregates
 * the earliest not-yet-settled-and-aggregated UTC day, so after downtime it simply processes every day
 * it missed. Non-destructive — it only folds rows into the rollups and stamps {@code aggregatedAt};
 * deletion is the separate {@link LogPruneJob}. Disabled by default ({@code CEDAR_LOG_LIVE_AGG_ENABLED}).
 * <p>
 * A day is eligible once it has closed plus a settle margin, because the raw rows are written async off
 * Redis and mutate (start/end) — the margin lets a day fully drain before it is aggregated.
 */
public class LiveAggregatorJob implements Managed {

  private static final Logger log = LoggerFactory.getLogger(LiveAggregatorJob.class);

  private final LogAggregationService service;
  private final boolean enabled;
  private final int batchSize;
  private final long pauseMs;
  private final long pollIntervalMs;
  private final int settleMarginHours;

  private volatile boolean running = true;
  private ExecutorService executor;

  public LiveAggregatorJob(LogAggregationService service) {
    this.service = service;
    this.enabled = Boolean.parseBoolean(env("CEDAR_LOG_LIVE_AGG_ENABLED", "false"));
    this.batchSize = parseIntSafe(env("CEDAR_LOG_LIVE_AGG_BATCH", "2000"), 2000);
    this.pauseMs = parseLongSafe(env("CEDAR_LOG_LIVE_AGG_PAUSE_MS", "200"), 200L);
    this.pollIntervalMs = parseLongSafe(env("CEDAR_LOG_LIVE_AGG_POLL_MS", "900000"), 900_000L);
    this.settleMarginHours = parseIntSafe(env("CEDAR_LOG_LIVE_AGG_MARGIN_HOURS", "3"), 3);
  }

  @Override
  public void start() {
    executor = Executors.newSingleThreadExecutor();
    executor.submit(this::run);
  }

  @Override
  public void stop() {
    log.info("LiveAggregatorJob.stop()");
    running = false;
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  private void run() {
    if (!enabled) {
      log.info("LiveAggregatorJob disabled (set CEDAR_LOG_LIVE_AGG_ENABLED=true to run).");
      return;
    }
    log.info("LiveAggregatorJob starting: batch={}, pauseMs={}, pollMs={}, settleMarginHours={}",
        batchSize, pauseMs, pollIntervalMs, settleMarginHours);
    while (running) {
      boolean did;
      try {
        boolean r = processDay(true);
        boolean c = processDay(false);
        did = r || c;
      } catch (Exception e) {
        log.error("LiveAggregatorJob batch failed; retrying after the poll interval.", e);
        did = false;
      }
      if (!did && !sleep(pollIntervalMs)) {
        return;
      }
    }
  }

  /**
   * Process the earliest eligible day for one source.
   *
   * @param request true = log_request, false = log_cypher
   * @return true if a day was aggregated (caller should look again immediately for the next day)
   */
  private boolean processDay(boolean request) {
    Instant earliest = request ? service.earliestUnaggregatedRequestTime()
        : service.earliestUnaggregatedCypherTime();
    if (earliest == null) {
      return false;
    }
    Instant dayStart = earliest.truncatedTo(ChronoUnit.DAYS);
    Instant dayEnd = dayStart.plus(1, ChronoUnit.DAYS);
    if (dayEnd.isAfter(Instant.now().minus(settleMarginHours, ChronoUnit.HOURS))) {
      return false; // day not settled yet
    }
    long total = 0;
    while (running) {
      long n = request ? service.aggregateLiveRequestBatch(dayStart, dayEnd, batchSize)
          : service.aggregateLiveCypherBatch(dayStart, dayEnd, batchSize);
      if (n == 0) {
        break;
      }
      total += n;
      if (!sleep(pauseMs)) {
        return true;
      }
    }
    log.info("Aggregated live {} day {} ({} rows).", request ? "request" : "cypher", dayStart, total);
    return true;
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

  private static String env(String key, String dflt) {
    String v = System.getenv(key);
    return (v == null || v.isEmpty()) ? dflt : v;
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
