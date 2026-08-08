package org.metadatacenter.worker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.metadatacenter.config.CacheServerPersistent;
import org.metadatacenter.server.queue.util.EmbeddedRedis;
import org.metadatacenter.server.queue.util.PermissionQueueService;
import org.metadatacenter.server.queue.util.QueueTestConfig;
import org.metadatacenter.server.search.SearchPermissionQueueEvent;
import org.metadatacenter.server.search.SearchPermissionQueueEventType;
import org.metadatacenter.server.search.permission.SearchPermissionExecutorService;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * The search-permission consumer, driven end to end against a real Redis.
 * <p>
 * This is the loop that keeps the search index's permissions in step with the graph. Its contract
 * is easy to state and was previously unchecked: what is enqueued gets handled, in order; one bad
 * message does not stop the ones behind it; and stop() ends the loop promptly rather than leaving a
 * thread running or hanging shutdown.
 */
@Timeout(60)
class PermissionQueueProcessorTest {

  private EmbeddedRedis redis;
  private PermissionQueueService queueService;
  private PermissionQueueProcessor processor;

  private void startWith(SearchPermissionExecutorService executor) throws Exception {
    redis = EmbeddedRedis.start();
    CacheServerPersistent config = QueueTestConfig.onPort(redis.port());
    queueService = new PermissionQueueService(config);
    processor = new PermissionQueueProcessor(queueService, executor);
    processor.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (processor != null) {
      processor.stop();
    }
    if (redis != null) {
      redis.close();
    }
  }

  private static SearchPermissionQueueEvent event(String id) {
    return new SearchPermissionQueueEvent(id, SearchPermissionQueueEventType.RESOURCE_PERMISSION_CHANGED);
  }

  /** A producer enqueues; the consumer picks it up and hands it to the executor. */
  @Test
  void anEnqueuedEventReachesTheExecutor() throws Exception {
    SearchPermissionExecutorService executor = mock(SearchPermissionExecutorService.class);
    startWith(executor);

    queueService.enqueueEvent(event("artifact-1"));

    ArgumentCaptor<SearchPermissionQueueEvent> handled =
        ArgumentCaptor.forClass(SearchPermissionQueueEvent.class);
    verify(executor, timeout(20_000)).handleEvent(handled.capture());

    assertEquals("artifact-1", handled.getValue().getId());
    assertEquals(SearchPermissionQueueEventType.RESOURCE_PERMISSION_CHANGED,
        handled.getValue().getEventType());
  }

  @Test
  void eventsAreHandledInTheOrderTheyWereEnqueued() throws Exception {
    SearchPermissionExecutorService executor = mock(SearchPermissionExecutorService.class);
    startWith(executor);

    queueService.enqueueEvent(event("first"));
    queueService.enqueueEvent(event("second"));
    queueService.enqueueEvent(event("third"));

    ArgumentCaptor<SearchPermissionQueueEvent> handled =
        ArgumentCaptor.forClass(SearchPermissionQueueEvent.class);
    verify(executor, timeout(20_000).times(3)).handleEvent(handled.capture());

    assertEquals(java.util.List.of("first", "second", "third"),
        handled.getAllValues().stream().map(SearchPermissionQueueEvent::getId).toList());
  }

  /**
   * A message the executor chokes on must not take the consumer down with it. The queue is shared
   * across the estate, so one poisonous event stalling the loop would stop every later permission
   * change from reaching the index.
   * <p>
   * The window is deliberately shorter than the consumer's retry delay. Handling the failure inside
   * the message loop takes milliseconds; letting it escape to the outer loop costs a reconnect and
   * a wait, so a tight timeout is what separates "absorbed" from "recovered from".
   */
  @Test
  void afailingEventDoesNotStopTheOnesBehindIt() throws Exception {
    SearchPermissionExecutorService executor = mock(SearchPermissionExecutorService.class);
    doThrow(new RuntimeException("indexing blew up")).when(executor).handleEvent(any());
    startWith(executor);

    queueService.enqueueEvent(event("poison"));
    queueService.enqueueEvent(event("after-the-poison"));

    ArgumentCaptor<SearchPermissionQueueEvent> handled =
        ArgumentCaptor.forClass(SearchPermissionQueueEvent.class);
    verify(executor, timeout(5_000).times(2)).handleEvent(handled.capture());

    assertTrue(handled.getAllValues().stream().anyMatch(e -> "after-the-poison".equals(e.getId())),
        "the event behind the failing one must still be handled");
  }

  /**
   * Shutdown has to end the loop, not merely ask it to. Asserting that stop() returns proves
   * nothing - it never blocks on the worker thread - so this checks the consumer is actually dead:
   * an event enqueued afterwards, by a producer that outlives the processor, must go unhandled.
   */
  @Test
  @Timeout(30)
  void stopReallyEndsTheConsumer() throws Exception {
    SearchPermissionExecutorService executor = mock(SearchPermissionExecutorService.class);
    startWith(executor);
    // A producer of its own, so closing the processor's service does not take the queue with it
    PermissionQueueService producer = new PermissionQueueService(QueueTestConfig.onPort(redis.port()));

    producer.enqueueEvent(event("before-stop"));
    verify(executor, timeout(20_000)).handleEvent(any());

    processor.stop();
    processor = null; // already stopped; keep @AfterEach from stopping it twice

    producer.enqueueEvent(event("after-stop"));
    // Long enough that a consumer still running would have picked it up several times over
    Thread.sleep(2_000);
    verify(executor, times(1)).handleEvent(any());

    try (redis.clients.jedis.Jedis jedis = new redis.clients.jedis.Jedis("127.0.0.1", redis.port())) {
      assertTrue(jedis.llen(QueueTestConfig.queueName(
              org.metadatacenter.server.queue.util.QueueService.SEARCH_PERMISSION_QUEUE_ID)) > 0,
          "the post-stop event should still be sitting on the queue, unconsumed");
    }
    producer.close();
  }

  /**
   * The queue is gone when the consumer starts. It must keep retrying rather than die, and stop()
   * must still return - a shutdown that hangs on an unreachable queue would block the whole
   * application from stopping.
   */
  @Test
  @Timeout(30)
  void anUnreachableQueueSuspendsTheConsumerWithoutKillingItOrShutdown() throws Exception {
    SearchPermissionExecutorService executor = mock(SearchPermissionExecutorService.class);
    CacheServerPersistent offline = QueueTestConfig.onPort(EmbeddedRedis.freePort());
    PermissionQueueService offlineQueue = new PermissionQueueService(offline);
    PermissionQueueProcessor offlineProcessor = new PermissionQueueProcessor(offlineQueue, executor);

    offlineProcessor.start();
    offlineProcessor.stop();
  }
}
