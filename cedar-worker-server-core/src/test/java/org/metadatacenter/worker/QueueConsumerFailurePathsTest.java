package org.metadatacenter.worker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.metadatacenter.model.SystemComponent;
import org.metadatacenter.server.logging.AppLoggerExecutorService;
import org.metadatacenter.server.logging.AppLoggerQueueService;
import org.metadatacenter.server.logging.model.AppLogMessage;
import org.metadatacenter.server.logging.model.AppLogSubType;
import org.metadatacenter.server.logging.model.AppLogType;
import org.metadatacenter.server.queue.util.EmbeddedRedis;
import org.metadatacenter.server.queue.util.PermissionQueueService;
import org.metadatacenter.server.queue.util.QueueServiceWithBlockingQueue;
import org.metadatacenter.server.queue.util.QueueTestConfig;
import org.metadatacenter.server.search.SearchPermissionQueueEvent;
import org.metadatacenter.server.search.SearchPermissionQueueEventType;
import org.metadatacenter.server.search.permission.SearchPermissionExecutorService;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * The two ways the search-permission and application-log consumers can lose work, against a real
 * Redis: a message claimed in the moment the consumer stops, and a batch applied whose
 * acknowledgement Redis refuses. Either way the message must stay in flight, so that the next start
 * of the consumer applies it, rather than be dropped. Losing it loses a permission update or a log
 * record.
 */
@Timeout(60)
@Tag("integration")
class QueueConsumerFailurePathsTest {

  private static final long RETRY_DELAY_MILLIS = 10;

  private final List<AutoCloseable> stops = new ArrayList<>();
  private EmbeddedRedis redis;

  @AfterEach
  void tearDown() throws Exception {
    for (AutoCloseable stop : stops) {
      try {
        stop.close();
      } catch (Exception e) {
        // A consumer's stop closes its queue, so a queue may already be closed here.
      }
    }
    if (redis != null) {
      redis.close();
    }
  }

  // A message claimed as the consumer stops

  @Test
  void aPermissionEventClaimedAsTheConsumerStopsIsAppliedByTheNextStart() throws Exception {
    redis = EmbeddedRedis.start();
    PermissionQueueService queue = spy(permissionQueue());
    SearchPermissionExecutorService stopped = mock(SearchPermissionExecutorService.class);
    PermissionQueueProcessor processor = new PermissionQueueProcessor(queue, stopped, RETRY_DELAY_MILLIS);
    stopWhenAMessageArrives(queue, processor::stop, processor::isRunning);
    processor.start();

    permissionQueue().enqueueEvent(permissionEvent("claimed-while-stopping"));

    awaitStopped(processor::isRunning);
    verify(stopped, never()).handleEvent(any());
    verify(stopped, never()).handleEvents(any());
    assertEquals(1, permissionQueue().inFlightCount(), "the claimed event stays in flight");

    SearchPermissionExecutorService next = mock(SearchPermissionExecutorService.class);
    PermissionQueueProcessor restarted = new PermissionQueueProcessor(permissionQueue(), next, RETRY_DELAY_MILLIS);
    stops.add(restarted::stop);
    restarted.start();

    ArgumentCaptor<SearchPermissionQueueEvent> applied = ArgumentCaptor.forClass(SearchPermissionQueueEvent.class);
    verify(next, timeout(10_000)).handleEvent(applied.capture());
    assertEquals("claimed-while-stopping", applied.getValue().getId());
  }

  @Test
  void anAppLogClaimedAsTheConsumerStopsIsWrittenByTheNextStart() throws Exception {
    redis = EmbeddedRedis.start();
    AppLoggerQueueService queue = spy(appLogQueue());
    AppLoggerExecutorService stopped = mock(AppLoggerExecutorService.class);
    AppLoggerQueueProcessor processor = new AppLoggerQueueProcessor(queue, stopped, RETRY_DELAY_MILLIS);
    stopWhenAMessageArrives(queue, processor::stop, processor::isRunning);
    processor.start();

    appLogQueue().enqueueEvent(appLog("claimed-while-stopping"));

    awaitStopped(processor::isRunning);
    verify(stopped, never()).handleLog(any());
    verify(stopped, never()).handleLogs(any());
    assertEquals(1, appLogQueue().inFlightCount(), "the claimed message stays in flight");

    AppLoggerExecutorService next = mock(AppLoggerExecutorService.class);
    AppLoggerQueueProcessor restarted = new AppLoggerQueueProcessor(appLogQueue(), next, RETRY_DELAY_MILLIS);
    stops.add(restarted::stop);
    restarted.start();

    ArgumentCaptor<AppLogMessage> written = ArgumentCaptor.forClass(AppLogMessage.class);
    verify(next, timeout(10_000)).handleLog(written.capture());
    assertEquals("claimed-while-stopping", written.getValue().getGlobalRequestId());
  }

  // A batch whose acknowledgement Redis refuses

  /**
   * The consumer applied the batch and Redis refused to acknowledge it, so the batch is still in
   * flight. The consumer fails, starts again, recovers the batch and applies it once more, which a
   * permission projection takes no harm from, and then acknowledges it.
   */
  @Test
  void aPermissionBatchWhoseAcknowledgementIsRefusedIsAppliedAgain() throws Exception {
    redis = EmbeddedRedis.start();
    PermissionQueueService queue = spy(permissionQueue());
    doReturn(false).doCallRealMethod().when(queue).acknowledge(anyString());
    queue.enqueueEvent(permissionEvent("first"));
    queue.enqueueEvent(permissionEvent("second"));
    List<List<String>> batches = new ArrayList<>();
    SearchPermissionExecutorService executor = mock(SearchPermissionExecutorService.class);
    doAnswer(call -> {
      List<SearchPermissionQueueEvent> events = call.getArgument(0);
      batches.add(events.stream().map(SearchPermissionQueueEvent::getId).toList());
      return null;
    }).when(executor).handleEvents(any());
    PermissionQueueProcessor processor =
        new PermissionQueueProcessor(queue, executor, RETRY_DELAY_MILLIS, RETRY_DELAY_MILLIS);
    stops.add(processor::stop);

    processor.start();

    verify(executor, timeout(10_000).times(2)).handleEvents(any());
    assertEquals(List.of(List.of("first", "second"), List.of("first", "second")), batches);
    awaitAcknowledged(permissionQueue());
  }

  @Test
  void anAppLogBatchWhoseAcknowledgementIsRefusedIsWrittenAgain() throws Exception {
    redis = EmbeddedRedis.start();
    AppLoggerQueueService queue = spy(appLogQueue());
    doReturn(false).doCallRealMethod().when(queue).acknowledge(anyString());
    queue.enqueueEvent(appLog("first"));
    queue.enqueueEvent(appLog("second"));
    List<List<String>> batches = new ArrayList<>();
    AppLoggerExecutorService executor = mock(AppLoggerExecutorService.class);
    doAnswer(call -> {
      List<AppLogMessage> messages = call.getArgument(0);
      batches.add(messages.stream().map(AppLogMessage::getGlobalRequestId).toList());
      return null;
    }).when(executor).handleLogs(any());
    AppLoggerQueueProcessor processor =
        new AppLoggerQueueProcessor(queue, executor, RETRY_DELAY_MILLIS, RETRY_DELAY_MILLIS);
    stops.add(processor::stop);

    processor.start();

    verify(executor, timeout(10_000).times(2)).handleLogs(any());
    assertEquals(List.of(List.of("first", "second"), List.of("first", "second")), batches);
    awaitAcknowledged(appLogQueue());
  }

  // Helpers

  private PermissionQueueService permissionQueue() {
    PermissionQueueService queue = new PermissionQueueService(QueueTestConfig.onPort(redis.port()));
    stops.add(queue::close);
    return queue;
  }

  private AppLoggerQueueService appLogQueue() {
    AppLoggerQueueService queue = new AppLoggerQueueService(QueueTestConfig.onPort(redis.port()));
    stops.add(queue::close);
    return queue;
  }

  private static SearchPermissionQueueEvent permissionEvent(String id) {
    return new SearchPermissionQueueEvent(id, SearchPermissionQueueEventType.RESOURCE_PERMISSION_CHANGED);
  }

  private static AppLogMessage appLog(String globalRequestId) {
    return new AppLogMessage(SystemComponent.SERVER_RESOURCE, AppLogType.REQUEST_FILTER, AppLogSubType.END,
        globalRequestId, "local");
  }

  /**
   * Makes the consumer stop in the moment after Redis hands it a message and before it handles it.
   * Stopping runs on its own thread, as it does in a server, and the consumer is held until it has
   * begun.
   */
  private static void stopWhenAMessageArrives(QueueServiceWithBlockingQueue queue, ThrowingRunnable stop,
                                              BooleanSupplier running) {
    AtomicBoolean stopping = new AtomicBoolean();
    Answer<List<String>> claimThenStop = call -> {
      @SuppressWarnings("unchecked")
      List<String> claimed = (List<String>) call.callRealMethod();
      if (claimed != null && !claimed.isEmpty() && stopping.compareAndSet(false, true)) {
        new Thread(() -> {
          try {
            stop.run();
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        }, "stop-the-consumer").start();
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (running.getAsBoolean() && System.nanoTime() < deadline) {
          Thread.onSpinWait();
        }
      }
      return claimed;
    };
    doAnswer(claimThenStop).when(queue).waitForMessages();
  }

  private static void awaitStopped(BooleanSupplier running) {
    long deadline = System.nanoTime() + 10_000_000_000L;
    while (running.getAsBoolean()) {
      assertTrue(System.nanoTime() < deadline, "the consumer did not stop");
      Thread.onSpinWait();
    }
  }

  /** Waits until nothing is in flight, which after the batch was applied again means it was acknowledged. */
  private static void awaitAcknowledged(QueueServiceWithBlockingQueue queue) throws InterruptedException {
    long deadline = System.nanoTime() + 10_000_000_000L;
    while (queue.inFlightCount() > 0) {
      assertTrue(System.nanoTime() < deadline, "the batch was never acknowledged");
      Thread.sleep(10);
    }
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
