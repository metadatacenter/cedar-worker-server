package org.metadatacenter.worker;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.metadatacenter.server.logging.agg.LogAggregationService;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LiveAggregatorJobTest {
  @ParameterizedTest @ValueSource(booleans = {true, false})
  void aDayOwnedByAnotherWorkerUsesIdlePollingRatherThanSpinning(boolean request) {
    LogAggregationService service = mock(LogAggregationService.class);
    Instant old = Instant.parse("2020-01-01T00:00:00Z");
    if (request) when(service.earliestUnaggregatedRequestTime()).thenReturn(old);
    else when(service.earliestUnaggregatedCypherTime()).thenReturn(old);
    // Earliest eligible row exists, but SKIP LOCKED returns no rows owned by this worker.
    assertFalse(new LiveAggregatorJob(service).processDay(request));
    if (request) verify(service).aggregateLiveRequestBatch(any(), any(), anyInt());
    else verify(service).aggregateLiveCypherBatch(any(), any(), anyInt());
    verify(service, never()).captureLiveRequestDayOutliers(any(), any());
    verify(service, never()).captureLiveCypherDayOutliers(any(), any());
  }
}
