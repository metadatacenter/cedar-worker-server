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
import static org.mockito.ArgumentMatchers.argThat;
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
   * The consumer retries a failing event a few times before parking it, so the event behind it
   * waits out those attempts. The window covers them; what it must not cover is the outer loop's
   * far longer reconnect-and-wait, which is what letting the failure escape the message loop would
   * cost.
   */
  @Test
  void afailingEventDoesNotStopTheOnesBehindIt() throws Exception {
    SearchPermissionExecutorService executor = mock(SearchPermissionExecutorService.class);
    doThrow(new RuntimeException("indexing blew up")).when(executor).handleEvent(any());
    startWith(executor);

    queueService.enqueueEvent(event("poison"));
    queueService.enqueueEvent(event("after-the-poison"));

    // Named rather than counted: the retries of the poisonous event are themselves invocations, so
    // a count would be satisfied without the second event ever being reached
    verify(executor, timeout(9_000)).handleEvent(argThat(e -> "after-the-poison".equals(e.getId())));
  }

  /**
   * A transient failure is what the retry is for. The event must end up applied, and must not also
   * be parked - a dead-letter entry for an event that did land would send someone replaying work
   * that was already done.
   */
  @Test
  void anEventThatFailsOnceIsRetriedAndApplied() throws Exception {
    SearchPermissionExecutorService executor = mock(SearchPermissionExecutorService.class);
    doThrow(new RuntimeException("neo4j blipped")).doNothing().when(executor).handleEvent(any());
    startWith(executor);

    queueService.enqueueEvent(event("artifact-1"));

    verify(executor, timeout(20_000).times(2)).handleEvent(any());
    Thread.sleep(1_000);
    assertEquals(0, queueService.deadLetterCount(), "an event that succeeded must not be parked");
  }

  /**
   * The defect this covers: BLPOP takes the event off the queue before handling it, so an event the
   * consumer could not apply used to be logged and forgotten, leaving the search index's
   * permissions silently out of step with the graph. It has to survive somewhere.
   */
  @Test
  void anEventThatKeepsFailingIsParkedRatherThanLost() throws Exception {
    SearchPermissionExecutorService executor = mock(SearchPermissionExecutorService.class);
    doThrow(new RuntimeException("indexing blew up")).when(executor).handleEvent(any());
    startWith(executor);

    queueService.enqueueEvent(event("artifact-1"));

    verify(executor, timeout(20_000).times(3)).handleEvent(any());

    long deadline = System.currentTimeMillis() + 10_000;
    while (queueService.deadLetterCount() == 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(100);
    }
    assertEquals(1, queueService.deadLetterCount(),
        "the event the consumer gave up on must be recoverable from the dead-letter queue");

    try (redis.clients.jedis.Jedis jedis = new redis.clients.jedis.Jedis("127.0.0.1", redis.port())) {
      String parked = jedis.lrange(queueService.getDeadLetterQueueName(), 0, -1).get(0);
      assertTrue(parked.contains("artifact-1"), "the parked payload should be the original message");
    }
  }

  /**
   * Retrying will not make a malformed message parse, so it is parked on the first attempt rather
   * than costing the events behind it three trips through the executor.
   */
  @Test
  void aMessageThatCannotBeParsedIsParkedWithoutRetrying() throws Exception {
    SearchPermissionExecutorService executor = mock(SearchPermissionExecutorService.class);
    startWith(executor);

    try (redis.clients.jedis.Jedis jedis = new redis.clients.jedis.Jedis("127.0.0.1", redis.port())) {
      jedis.rpush(QueueTestConfig.queueName(
          org.metadatacenter.server.queue.util.QueueService.SEARCH_PERMISSION_QUEUE_ID), "not json at all");
    }

    long deadline = System.currentTimeMillis() + 20_000;
    while (queueService.deadLetterCount() == 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(100);
    }
    assertEquals(1, queueService.deadLetterCount(), "an unparseable message must be kept, not dropped");
    verify(executor, times(0)).handleEvent(any());
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
