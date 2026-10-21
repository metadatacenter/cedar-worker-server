package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.metadatacenter.config.CacheServerPersistent;
import org.metadatacenter.id.CedarTemplateId;
import org.metadatacenter.model.SystemComponent;
import org.metadatacenter.server.logging.AppLoggerExecutorService;
import org.metadatacenter.server.logging.AppLoggerQueueService;
import org.metadatacenter.server.logging.model.AppLogMessage;
import org.metadatacenter.server.logging.model.AppLogSubType;
import org.metadatacenter.server.logging.model.AppLogType;
import org.metadatacenter.server.queue.util.CloneInstancesQueueService;
import org.metadatacenter.server.queue.util.EmbeddedRedis;
import org.metadatacenter.server.queue.util.PermissionQueueService;
import org.metadatacenter.server.queue.util.QueueService;
import org.metadatacenter.server.queue.util.QueueServiceWithBlockingQueue;
import org.metadatacenter.server.queue.util.QueueTestConfig;
import org.metadatacenter.server.resource.CloneInstancesExecutorService;
import org.metadatacenter.server.resource.CloneInstancesQueueEvent;
import org.metadatacenter.server.search.SearchPermissionQueueEvent;
import org.metadatacenter.server.search.SearchPermissionQueueEventType;
import org.metadatacenter.server.search.permission.SearchPermissionExecutorService;
import org.mockito.invocation.InvocationOnMock;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * The worker's three queue consumers share one lifecycle. Each claims a message from Redis, hands it to its handler,
 * and acknowledges it once handled. A message that cannot be read is parked on the dead-letter queue unhandled, and a
 * message the dead-letter queue refuses stays in flight for recovery. A Redis outage suspends a consumer without ending
 * it, and stopping a consumer that waits to retry leaves its message in flight. The health checks read what each
 * consumer reports: whether it runs, and when it last succeeded and last failed.
 */
@Timeout(60)
@Tag("integration")
class QueueConsumerLifecycleTest {

  enum Consumer {
    APP_LOG("app-log", QueueService.APP_LOG_QUEUE_ID),
    CLONE_INSTANCES("clone-instances", QueueService.CLONE_INSTANCES_QUEUE_ID),
    SEARCH_PERMISSION("search-permission", QueueService.SEARCH_PERMISSION_QUEUE_ID);

    final String processorName;
    final String queueId;

    Consumer(String processorName, String queueId) {
      this.processorName = processorName;
      this.queueId = queueId;
    }
  }

  /** A consumer's two faces: the lifecycle the application manages and what it reports to the health checks. */
  private record Running(Managed lifecycle, QueueProcessorMonitor monitor) {
  }

  private EmbeddedRedis redis;
  private CacheServerPersistent config;
  private QueueServiceWithBlockingQueue reader;
  private Running running;
  private final List<Object> handled = new CopyOnWriteArrayList<>();
  private final AtomicInteger failuresLeft = new AtomicInteger();

  @AfterEach
  void tearDown() throws Exception {
    if (running != null) {
      running.lifecycle().stop();
    }
    if (reader != null) {
      reader.close();
    }
    if (redis != null) {
      redis.close();
    }
  }

  @ParameterizedTest
  @EnumSource(Consumer.class)
  void anUnreachableQueueIsReportedWhileTheConsumerKeepsRunning(Consumer consumer) throws Exception {
    CacheServerPersistent offline = QueueTestConfig.onPort(EmbeddedRedis.freePort());
    Running offlineConsumer = switch (consumer) {
      case APP_LOG -> running(new AppLoggerQueueProcessor(new AppLoggerQueueService(offline), appLogHandler()));
      case CLONE_INSTANCES -> running(new CloneInstancesQueueProcessor(new CloneInstancesQueueService(offline),
          cloneHandler()));
      case SEARCH_PERMISSION -> running(new PermissionQueueProcessor(new PermissionQueueService(offline),
          permissionHandler()));
    };
    QueueProcessorMonitor monitor = offlineConsumer.monitor();

    assertEquals(consumer.processorName, monitor.getProcessorName());
    assertFalse(monitor.isRunning(), "a consumer that has not started does not run");
    offlineConsumer.lifecycle().start();
    await(() -> monitor.getLastFailureAt() != null, "the outage is reported");
    assertTrue(monitor.isRunning(), "the outage does not end the consumer");
    assertNull(monitor.getLastSuccessAt());

    long started = System.nanoTime();
    offlineConsumer.lifecycle().stop();
    assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(5), "stopping does not wait out the retry delay");
    assertFalse(monitor.isRunning());
  }

  @ParameterizedTest
  @EnumSource(Consumer.class)
  void aHandledMessageIsAcknowledgedAndReportedAsASuccess(Consumer consumer) throws Exception {
    QueueServiceWithBlockingQueue queue = startWith(consumer, 10, false);

    enqueue(consumer, queue);

    await(() -> handled.size() == 1 && reader.inFlightCount() == 0, "the message is handled and acknowledged");
    assertNotNull(running.monitor().getLastSuccessAt());
    assertTrue(running.monitor().isRunning());
    assertEquals(0, reader.deadLetterCount());
  }

  @ParameterizedTest
  @EnumSource(Consumer.class)
  void aMessageThatIsNullOrNotJsonIsParkedUnhandled(Consumer consumer) throws Exception {
    startWith(consumer, 10, false);

    push(consumer, "null");
    push(consumer, "not json at all");

    await(() -> reader.deadLetterCount() == 2, "both messages are parked");
    assertEquals(List.of(), handled);
    assertEquals(0, reader.inFlightCount());
    assertNotNull(running.monitor().getLastFailureAt());
  }

  @ParameterizedTest
  @EnumSource(Consumer.class)
  void aMessageTheDeadLetterQueueRefusesStaysInFlight(Consumer consumer) throws Exception {
    QueueServiceWithBlockingQueue queue = startWith(consumer, 10, true);

    push(consumer, "not json at all");

    verify(queue, timeout(20_000)).deadLetter("not json at all");
    assertEquals(1, reader.inFlightCount(), "the message waits in the processing list for the next start");
    assertEquals(0, reader.deadLetterCount());
  }

  @ParameterizedTest
  @EnumSource(Consumer.class)
  void stoppingAConsumerThatWaitsToRetryLeavesItsMessageInFlight(Consumer consumer) throws Exception {
    QueueServiceWithBlockingQueue queue = startWith(consumer, 60_000, false);
    failuresLeft.set(Integer.MAX_VALUE);

    enqueue(consumer, queue);
    await(() -> handled.size() == 1, "the first attempt fails");

    long started = System.nanoTime();
    running.lifecycle().stop();
    running = null;
    assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(5), "stopping does not wait out the retry delay");
    assertEquals(1, handled.size(), "a stopped consumer does not try again");
    assertEquals(1, reader.inFlightCount(), "the message waits in the processing list for the next start");
  }

  @Test
  void anApplicationLogThatFailsOnceIsHandledOnTheRetry() throws Exception {
    QueueServiceWithBlockingQueue queue = startWith(Consumer.APP_LOG, 10, false);
    failuresLeft.set(1);

    enqueue(Consumer.APP_LOG, queue);

    await(() -> handled.size() == 2 && reader.inFlightCount() == 0, "the retry succeeds and is acknowledged");
    assertEquals(0, reader.deadLetterCount());
  }

  /** Cloning is not idempotent: a completed clone retries only its acknowledgement, until the consumer stops. */
  @Test
  void aCompletedCloneWaitsForItsAcknowledgementWithoutCloningAgain() throws Exception {
    QueueServiceWithBlockingQueue queue = startWith(Consumer.CLONE_INSTANCES, 10, false, true);

    enqueue(Consumer.CLONE_INSTANCES, queue);
    verify(queue, timeout(20_000).atLeast(3)).acknowledge(anyString());
    running.lifecycle().stop();
    running = null;

    assertEquals(1, handled.size());
    assertEquals(1, reader.inFlightCount());
  }

  /**
   * Starts the consumer against a fresh Redis, through a spy of its queue, with a handler that records each message
   * and fails as many times as {@link #failuresLeft} says. The dead-letter queue refuses every message when asked to.
   */
  private QueueServiceWithBlockingQueue startWith(Consumer consumer, long retryDelayMillis, boolean refuseDeadLetters)
      throws Exception {
    return startWith(consumer, retryDelayMillis, refuseDeadLetters, false);
  }

  /**
   * As above, and the queue also refuses every acknowledgement when asked to. The spy is stubbed before the consumer
   * starts, because stubbing a spy that another thread is already calling can hand that thread the stub's answer.
   */
  private QueueServiceWithBlockingQueue startWith(Consumer consumer, long retryDelayMillis, boolean refuseDeadLetters,
                                                  boolean refuseAcknowledgements) throws Exception {
    redis = EmbeddedRedis.start();
    config = QueueTestConfig.onPort(redis.port());
    reader = queue(consumer);
    QueueServiceWithBlockingQueue queue = spy(queue(consumer));
    if (refuseDeadLetters) {
      doReturn(false).when(queue).deadLetter(anyString());
    }
    if (refuseAcknowledgements) {
      doReturn(false).when(queue).acknowledge(anyString());
    }
    running = switch (consumer) {
      case APP_LOG -> running(new AppLoggerQueueProcessor((AppLoggerQueueService) queue, appLogHandler(),
          retryDelayMillis));
      case CLONE_INSTANCES -> running(new CloneInstancesQueueProcessor((CloneInstancesQueueService) queue,
          cloneHandler(), retryDelayMillis));
      case SEARCH_PERMISSION -> running(new PermissionQueueProcessor((PermissionQueueService) queue,
          permissionHandler(), retryDelayMillis));
    };
    running.lifecycle().start();
    return queue;
  }

  private QueueServiceWithBlockingQueue queue(Consumer consumer) {
    return switch (consumer) {
      case APP_LOG -> new AppLoggerQueueService(config);
      case CLONE_INSTANCES -> new CloneInstancesQueueService(config);
      case SEARCH_PERMISSION -> new PermissionQueueService(config);
    };
  }

  /** Enqueues a message the consumer can read, the way the servers that produce it do. */
  private static void enqueue(Consumer consumer, QueueServiceWithBlockingQueue queue) {
    switch (consumer) {
      case APP_LOG -> ((AppLoggerQueueService) queue).enqueueEvent(new AppLogMessage(SystemComponent.SERVER_RESOURCE,
          AppLogType.REQUEST_FILTER, AppLogSubType.END, "global", "local"));
      case CLONE_INSTANCES -> ((CloneInstancesQueueService) queue).enqueueEvent(new CloneInstancesQueueEvent(
          CedarTemplateId.build("https://repo.metadatacenter.orgx/templates/old"),
          CedarTemplateId.build("https://repo.metadatacenter.orgx/templates/new"), "cloned"));
      case SEARCH_PERMISSION -> ((PermissionQueueService) queue).enqueueEvent(new SearchPermissionQueueEvent(
          "artifact-1", SearchPermissionQueueEventType.RESOURCE_PERMISSION_CHANGED));
    }
  }

  /** Puts a raw message on the consumer's queue, bypassing the producer. */
  private void push(Consumer consumer, String message) {
    try (Jedis jedis = new Jedis("127.0.0.1", redis.port())) {
      jedis.rpush(QueueTestConfig.queueName(consumer.queueId), message);
    }
  }

  private static <P extends Managed & QueueProcessorMonitor> Running running(P processor) {
    return new Running(processor, processor);
  }

  private AppLoggerExecutorService appLogHandler() {
    AppLoggerExecutorService handler = mock(AppLoggerExecutorService.class);
    doAnswer(this::handle).when(handler).handleLog(any());
    return handler;
  }

  private CloneInstancesExecutorService cloneHandler() throws Exception {
    CloneInstancesExecutorService handler = mock(CloneInstancesExecutorService.class);
    doAnswer(this::handle).when(handler).handleEvent(any());
    return handler;
  }

  private SearchPermissionExecutorService permissionHandler() throws Exception {
    SearchPermissionExecutorService handler = mock(SearchPermissionExecutorService.class);
    doAnswer(this::handle).when(handler).handleEvent(any());
    doAnswer(this::handle).when(handler).handleEvents(anyList());
    return handler;
  }

  private Object handle(InvocationOnMock call) {
    handled.add(call.getArgument(0));
    if (failuresLeft.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
      throw new IllegalStateException("the store is down");
    }
    return null;
  }

  private static void await(BooleanSupplier condition, String what) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        fail("timed out waiting until " + what);
      }
      Thread.sleep(10);
    }
  }
}
