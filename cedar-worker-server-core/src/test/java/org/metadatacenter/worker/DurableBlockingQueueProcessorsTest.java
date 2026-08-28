package org.metadatacenter.worker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.metadatacenter.id.CedarTemplateId;
import org.metadatacenter.model.SystemComponent;
import org.metadatacenter.server.logging.AppLoggerExecutorService;
import org.metadatacenter.server.logging.AppLoggerQueueService;
import org.metadatacenter.server.logging.model.AppLogMessage;
import org.metadatacenter.server.logging.model.AppLogSubType;
import org.metadatacenter.server.logging.model.AppLogType;
import org.metadatacenter.server.queue.util.CloneInstancesQueueService;
import org.metadatacenter.server.queue.util.EmbeddedRedis;
import org.metadatacenter.server.queue.util.QueueTestConfig;
import org.metadatacenter.server.resource.CloneInstancesExecutorService;
import org.metadatacenter.server.resource.CloneInstancesNotRetryableException;
import org.metadatacenter.server.resource.CloneInstancesQueueEvent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Timeout(30)
class DurableBlockingQueueProcessorsTest {

  private EmbeddedRedis redis;
  private CloneInstancesQueueProcessor cloneProcessor;
  private AppLoggerQueueProcessor logProcessor;

  @AfterEach
  void tearDown() throws Exception {
    if (cloneProcessor != null) {
      cloneProcessor.stop();
    }
    if (logProcessor != null) {
      logProcessor.stop();
    }
    if (redis != null) {
      redis.close();
    }
  }

  @Test
  void aCloneHandlerFailureIsRetriedThenDeadLettered() throws Exception {
    redis = EmbeddedRedis.start();
    CloneInstancesQueueService queue = new CloneInstancesQueueService(QueueTestConfig.onPort(redis.port()));
    CloneInstancesExecutorService executor = mock(CloneInstancesExecutorService.class);
    doThrow(new IllegalStateException("clone failed")).when(executor).handleEvent(any());
    cloneProcessor = new CloneInstancesQueueProcessor(queue, executor);
    cloneProcessor.start();

    queue.enqueueEvent(new CloneInstancesQueueEvent(
        CedarTemplateId.build("https://repo.metadatacenter.orgx/templates/old"),
        CedarTemplateId.build("https://repo.metadatacenter.orgx/templates/new"), "cloned"));

    verify(executor, timeout(10_000).times(3)).handleEvent(any());
    awaitDeadLetter(queue);
    assertEquals(0, queue.inFlightCount());
  }

  /**
   * Cloning is not idempotent, so a failure the executor marks as not retryable — the clone
   * already changed the workspace, or its per-instance failures are deterministic — must
   * dead-letter after one attempt. Re-running it would duplicate every copy that succeeded.
   */
  @Test
  void aCloneFailureMarkedNotRetryableIsDeadLetteredWithoutARetry() throws Exception {
    redis = EmbeddedRedis.start();
    CloneInstancesQueueService queue = new CloneInstancesQueueService(QueueTestConfig.onPort(redis.port()));
    CloneInstancesExecutorService executor = mock(CloneInstancesExecutorService.class);
    doThrow(new CloneInstancesNotRetryableException("part of the clone was created"))
        .when(executor).handleEvent(any());
    cloneProcessor = new CloneInstancesQueueProcessor(queue, executor);
    cloneProcessor.start();

    queue.enqueueEvent(new CloneInstancesQueueEvent(
        CedarTemplateId.build("https://repo.metadatacenter.orgx/templates/old"),
        CedarTemplateId.build("https://repo.metadatacenter.orgx/templates/new"), "cloned"));

    awaitDeadLetter(queue);
    verify(executor, times(1)).handleEvent(any());
    assertEquals(0, queue.inFlightCount());
  }

  @Test
  void anAppLogHandlerFailureIsRetriedThenDeadLettered() throws Exception {
    redis = EmbeddedRedis.start();
    AppLoggerQueueService queue = new AppLoggerQueueService(QueueTestConfig.onPort(redis.port()));
    AppLoggerExecutorService executor = mock(AppLoggerExecutorService.class);
    doThrow(new IllegalStateException("database failed")).when(executor).handleLog(any());
    logProcessor = new AppLoggerQueueProcessor(queue, executor);
    logProcessor.start();

    queue.enqueueEvent(new AppLogMessage(SystemComponent.SERVER_RESOURCE, AppLogType.REQUEST_FILTER,
        AppLogSubType.END, "global", "local"));

    verify(executor, timeout(10_000).times(3)).handleLog(any());
    awaitDeadLetter(queue);
    assertEquals(0, queue.inFlightCount());
  }

  private static void awaitDeadLetter(org.metadatacenter.server.queue.util.QueueServiceWithBlockingQueue queue)
      throws InterruptedException {
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
    while (queue.deadLetterCount() != 1 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(1, queue.deadLetterCount());
  }
}
