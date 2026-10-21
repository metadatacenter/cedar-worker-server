package org.metadatacenter.worker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.config.environment.CedarEnvironmentVariableProvider;
import org.metadatacenter.model.SystemComponent;
import org.metadatacenter.server.logging.agg.LogAggregationService;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Three background jobs fold the request and Cypher logs into rollups and delete what they no longer need: the live
 * aggregator, which aggregates each settled day; the one-time backfill of the frozen history, confined to an off-peak
 * window; and the prune, which deletes aggregated rows past their retention. Each is off unless enabled, takes its
 * pace from the environment, survives a failed pass, and stops when the worker stops.
 */
@Timeout(30)
class LogAggregationJobsTest {

  private static Map<String, String> workerEnvironment;

  private final LogAggregationService service = mock(LogAggregationService.class);

  @BeforeAll
  static void keepTheWorkerEnvironment() {
    workerEnvironment = CedarEnvironmentVariableProvider.getFor(SystemComponent.SERVER_WORKER);
  }

  @AfterEach
  void restoreTheWorkerEnvironment() {
    CedarConfig.getInstance(workerEnvironment);
  }

  /** Gives the jobs exactly these settings, whatever the process environment holds for them. */
  private static void configure(Map<String, String> settings) {
    Map<String, String> environment = new HashMap<>(workerEnvironment);
    environment.keySet().removeIf(name -> name.startsWith("CEDAR_LOG_LIVE_AGG_")
        || name.startsWith("CEDAR_LOG_BACKFILL_") || name.startsWith("CEDAR_LOG_PRUNE_"));
    environment.putAll(settings);
    CedarConfig.getInstance(environment);
  }

  @Test
  void eachJobIsOffUnlessEnabled() throws Exception {
    configure(Map.of());
    HistoricalBackfillJob backfill = new HistoricalBackfillJob(service);
    LiveAggregatorJob live = new LiveAggregatorJob(service);
    LogPruneJob prune = new LogPruneJob(service);

    backfill.start();
    live.start();
    prune.start();
    Thread.sleep(200);
    backfill.stop();
    live.stop();
    prune.stop();

    verifyNoInteractions(service);
  }

  @Test
  void theBackfillDrainsBothHistoriesThenKeepsTheirOutliers() {
    configure(Map.of("CEDAR_LOG_BACKFILL_ENABLED", "true", "CEDAR_LOG_BACKFILL_BATCH", "7",
        "CEDAR_LOG_BACKFILL_PAUSE_MS", "1", "CEDAR_LOG_BACKFILL_WINDOW_UTC", "0-0"));
    when(service.runBackfillRequestBatch(7)).thenReturn(7L, 7L, 0L);
    when(service.runBackfillCypherBatch(7)).thenReturn(3L, 0L);
    HistoricalBackfillJob backfill = new HistoricalBackfillJob(service);

    backfill.start();
    verify(service, timeout(10_000)).captureHistoryOutliers();
    backfill.stop();

    InOrder order = inOrder(service);
    order.verify(service).initBackfillState(LogAggregationService.SRC_REQUEST_HIST);
    order.verify(service).initBackfillState(LogAggregationService.SRC_CYPHER_HIST);
    order.verify(service, times(3)).runBackfillRequestBatch(7);
    order.verify(service).markBackfillComplete(LogAggregationService.SRC_REQUEST_HIST);
    order.verify(service, times(2)).runBackfillCypherBatch(7);
    order.verify(service).markBackfillComplete(LogAggregationService.SRC_CYPHER_HIST);
    order.verify(service).captureHistoryOutliers();
  }

  @Test
  void outsideItsWindowTheBackfillWaitsUntilStopped() throws Exception {
    int hour = Instant.now().atZone(ZoneOffset.UTC).getHour();
    configure(Map.of("CEDAR_LOG_BACKFILL_ENABLED", "true",
        "CEDAR_LOG_BACKFILL_WINDOW_UTC", (hour + 2) % 24 + "-" + (hour + 3) % 24));
    HistoricalBackfillJob backfill = new HistoricalBackfillJob(service);

    backfill.start();
    verify(service, timeout(10_000)).initBackfillState(LogAggregationService.SRC_CYPHER_HIST);
    verify(service, after(200).never()).runBackfillRequestBatch(anyInt());
    backfill.stop();

    verify(service, after(200).never()).markBackfillComplete(any());
    verify(service, never()).captureHistoryOutliers();
  }

  @Test
  void aFailedBackfillBatchEndsTheRunWithoutMarkingItsHistoryDrained() {
    configure(Map.of("CEDAR_LOG_BACKFILL_ENABLED", "true", "CEDAR_LOG_BACKFILL_BATCH", "many",
        "CEDAR_LOG_BACKFILL_PAUSE_MS", "soon", "CEDAR_LOG_BACKFILL_WINDOW_UTC", "0-0"));
    when(service.runBackfillRequestBatch(anyInt())).thenThrow(new IllegalStateException("the log database is down"));
    HistoricalBackfillJob backfill = new HistoricalBackfillJob(service);

    backfill.start();
    verify(service, timeout(10_000)).runBackfillRequestBatch(5000);
    backfill.stop();

    verify(service, after(200).never()).markBackfillComplete(any());
    verify(service, never()).runBackfillCypherBatch(anyInt());
    verify(service, never()).captureHistoryOutliers();
  }

  @Test
  void theBackfillWindowIsReadInWholeUtcHoursAndMayWrapPastMidnight() {
    assertArrayEquals(new int[]{2, 6}, HistoricalBackfillJob.parseWindow("2-6"));
    assertArrayEquals(new int[]{22, 2}, HistoricalBackfillJob.parseWindow(" 22 - 26 "));
    assertArrayEquals(new int[]{0, 0}, HistoricalBackfillJob.parseWindow(""), "no window means always");
    assertArrayEquals(new int[]{2, 6}, HistoricalBackfillJob.parseWindow("nights"), "a malformed window is 2-6");
    assertArrayEquals(new int[]{2, 6}, HistoricalBackfillJob.parseWindow("4"));

    assertTrue(HistoricalBackfillJob.inWindow(2, 6, 2));
    assertTrue(HistoricalBackfillJob.inWindow(2, 6, 5));
    assertFalse(HistoricalBackfillJob.inWindow(2, 6, 6), "the window ends before its end hour");
    assertFalse(HistoricalBackfillJob.inWindow(2, 6, 1));
    assertTrue(HistoricalBackfillJob.inWindow(22, 2, 23));
    assertTrue(HistoricalBackfillJob.inWindow(22, 2, 1));
    assertFalse(HistoricalBackfillJob.inWindow(22, 2, 2));
    assertFalse(HistoricalBackfillJob.inWindow(22, 2, 12));
    assertTrue(HistoricalBackfillJob.inWindow(5, 5, 17));
  }

  @Test
  void thePruneWalksEachTableInIdWindowsUpToTheEndOfItsRange() {
    configure(Map.of("CEDAR_LOG_PRUNE_ENABLED", "true", "CEDAR_LOG_PRUNE_RETENTION_DAYS", "10",
        "CEDAR_LOG_PRUNE_BATCH", "50", "CEDAR_LOG_PRUNE_PAUSE_MS", "1", "CEDAR_LOG_PRUNE_IDLE_MS", "60000"));
    when(service.requestPruneRange(any())).thenReturn(new long[]{100, 300});
    when(service.pruneRequestWindow(eq(100L), eq(300L), any(), eq(50))).thenReturn(new long[]{150, 5});
    when(service.pruneRequestWindow(eq(150L), eq(300L), any(), eq(50))).thenReturn(new long[]{-1, 0});
    when(service.cypherPruneRange(any())).thenReturn(new long[]{7, 7});
    LogPruneJob prune = new LogPruneJob(service);
    Instant expected = Instant.now().minus(10, ChronoUnit.DAYS);

    prune.start();
    ArgumentCaptor<Instant> cutoff = ArgumentCaptor.forClass(Instant.class);
    verify(service, timeout(10_000)).cypherPruneRange(cutoff.capture());
    prune.stop();

    verify(service, times(2)).pruneRequestWindow(anyLong(), anyLong(), any(), anyInt());
    verify(service, never()).pruneCypherWindow(anyLong(), anyLong(), any(), anyInt());
    assertTrue(Duration.between(expected, cutoff.getValue()).abs().compareTo(Duration.ofMinutes(1)) < 0,
        "the cutoff is the retention before now: " + cutoff.getValue());
  }

  @Test
  void aPrunePassStopsAtTheEndOfItsRangeWithoutAskingForMore() {
    configure(Map.of());
    when(service.cypherPruneRange(any())).thenReturn(new long[]{1, 5});
    when(service.pruneCypherWindow(eq(1L), eq(5L), any(), eq(2000))).thenReturn(new long[]{5, 3});
    LogPruneJob prune = new LogPruneJob(service);

    assertEquals(3, prune.prunePass(false, Instant.now()));
    verify(service, times(1)).pruneCypherWindow(anyLong(), anyLong(), any(), anyInt());
  }

  @Test
  void aFailedPruneIsTriedAgainAfterTheIdleInterval() {
    configure(Map.of("CEDAR_LOG_PRUNE_ENABLED", "true", "CEDAR_LOG_PRUNE_IDLE_MS", "1"));
    when(service.requestPruneRange(any())).thenThrow(new IllegalStateException("the log database is down"))
        .thenReturn(new long[]{0, 0});
    when(service.cypherPruneRange(any())).thenReturn(new long[]{0, 0});
    LogPruneJob prune = new LogPruneJob(service);

    prune.start();
    verify(service, timeout(10_000).atLeast(2)).requestPruneRange(any());
    verify(service, timeout(10_000).atLeast(1)).cypherPruneRange(any());
    prune.stop();
  }

  @Test
  void stoppingThePruneEndsItsPauseAndItsPass() {
    configure(Map.of("CEDAR_LOG_PRUNE_ENABLED", "true", "CEDAR_LOG_PRUNE_PAUSE_MS", "60000"));
    when(service.requestPruneRange(any())).thenReturn(new long[]{0, 1_000_000});
    when(service.pruneRequestWindow(anyLong(), anyLong(), any(), anyInt())).thenReturn(new long[]{2000, 10});
    LogPruneJob prune = new LogPruneJob(service);

    prune.start();
    verify(service, timeout(10_000)).pruneRequestWindow(anyLong(), anyLong(), any(), eq(2000));
    prune.stop();

    verify(service, after(200).times(1)).pruneRequestWindow(anyLong(), anyLong(), any(), anyInt());
    verify(service, never()).cypherPruneRange(any());
  }

  @Test
  void theLiveAggregatorAggregatesEachSettledDayAndKeepsItsOutliers() {
    configure(Map.of("CEDAR_LOG_LIVE_AGG_ENABLED", "true", "CEDAR_LOG_LIVE_AGG_BATCH", "100",
        "CEDAR_LOG_LIVE_AGG_PAUSE_MS", "1", "CEDAR_LOG_LIVE_AGG_POLL_MS", "1"));
    Instant dayStart = Instant.parse("2026-01-05T00:00:00Z");
    when(service.earliestUnaggregatedRequestTime()).thenReturn(dayStart.plusSeconds(3600), (Instant) null);
    when(service.aggregateLiveRequestBatch(any(), any(), anyInt())).thenReturn(10L, 0L);
    LiveAggregatorJob live = new LiveAggregatorJob(service);

    live.start();
    verify(service, timeout(10_000)).captureLiveRequestDayOutliers(dayStart, dayStart.plus(1, ChronoUnit.DAYS));
    live.stop();

    verify(service, times(2)).aggregateLiveRequestBatch(dayStart, dayStart.plus(1, ChronoUnit.DAYS), 100);
    verify(service, never()).aggregateLiveCypherBatch(any(), any(), anyInt());
    verify(service, never()).captureLiveCypherDayOutliers(any(), any());
  }

  @Test
  void aDayThatHasNotSettledOrNoDayAtAllIsLeftAlone() {
    configure(Map.of());
    LiveAggregatorJob live = new LiveAggregatorJob(service);
    when(service.earliestUnaggregatedRequestTime()).thenReturn(Instant.now());

    assertFalse(live.processDay(true), "today closes in the future, so it has not settled");
    assertFalse(live.processDay(false), "no unaggregated Cypher row means no day to aggregate");
    verify(service, never()).aggregateLiveRequestBatch(any(), any(), anyInt());
    verify(service, never()).aggregateLiveCypherBatch(any(), any(), anyInt());
  }

  @Test
  void aFailedLivePassIsTriedAgainAfterThePollInterval() {
    configure(Map.of("CEDAR_LOG_LIVE_AGG_ENABLED", "true", "CEDAR_LOG_LIVE_AGG_POLL_MS", "1",
        "CEDAR_LOG_LIVE_AGG_BATCH", "lots"));
    when(service.earliestUnaggregatedRequestTime()).thenThrow(new IllegalStateException("the log database is down"));
    LiveAggregatorJob live = new LiveAggregatorJob(service);

    live.start();
    verify(service, timeout(10_000).atLeast(2)).earliestUnaggregatedRequestTime();
    live.stop();
  }

  @Test
  void stoppingTheLiveAggregatorMidDayLeavesTheDayForTheNextStart() {
    configure(Map.of("CEDAR_LOG_LIVE_AGG_ENABLED", "true", "CEDAR_LOG_LIVE_AGG_PAUSE_MS", "60000"));
    when(service.earliestUnaggregatedCypherTime()).thenReturn(Instant.parse("2026-01-05T08:00:00Z"));
    when(service.aggregateLiveCypherBatch(any(), any(), anyInt())).thenReturn(10L);
    LiveAggregatorJob live = new LiveAggregatorJob(service);

    live.start();
    verify(service, timeout(10_000)).aggregateLiveCypherBatch(any(), any(), eq(2000));
    live.stop();

    verify(service, after(200).times(1)).aggregateLiveCypherBatch(any(), any(), anyInt());
    verify(service, never()).captureLiveCypherDayOutliers(any(), any());
  }
}
