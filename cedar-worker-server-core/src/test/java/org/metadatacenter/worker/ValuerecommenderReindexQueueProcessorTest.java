package org.metadatacenter.worker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.metadatacenter.id.CedarTemplateId;
import org.metadatacenter.id.CedarTemplateInstanceId;
import org.metadatacenter.server.queue.util.EmbeddedRedis;
import org.metadatacenter.server.queue.util.QueueTestConfig;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexExecutorService;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexQueueService;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessage;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessageActionType;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessageResourceType;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * The value-recommender reindex consumer. Unlike its siblings this one polls on an interval and
 * drains a whole batch per poll, then drops the messages that should not trigger a reindex.
 * <p>
 * That filter is the part worth pinning: a template only needs reindexing when it is updated, so
 * created and deleted templates are discarded, while instances are always kept. Getting it wrong is
 * silent - either wasted reindexing or a stale recommender.
 */
@Timeout(90)
class ValuerecommenderReindexQueueProcessorTest {

  private static final String TEMPLATE = "https://repo.metadatacenter.orgx/templates/t1";
  private static final String INSTANCE = "https://repo.metadatacenter.orgx/template-instances/i1";

  private EmbeddedRedis redis;
  private ValuerecommenderReindexQueueService queueService;
  private ValuerecommenderReindexQueueProcessor processor;

  private ValuerecommenderReindexExecutorService prepareProcessor() throws Exception {
    redis = EmbeddedRedis.start();
    queueService = new ValuerecommenderReindexQueueService(QueueTestConfig.onPort(redis.port()));
    ValuerecommenderReindexExecutorService executor = mock(ValuerecommenderReindexExecutorService.class);
    processor = new ValuerecommenderReindexQueueProcessor(queueService, executor);
    return executor;
  }

  private ValuerecommenderReindexExecutorService startProcessor() throws Exception {
    ValuerecommenderReindexExecutorService executor = prepareProcessor();
    processor.start();
    return executor;
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

  private static ValuerecommenderReindexMessage message(ValuerecommenderReindexMessageResourceType type,
                                                        ValuerecommenderReindexMessageActionType action) {
    return new ValuerecommenderReindexMessage(CedarTemplateId.build(TEMPLATE),
        CedarTemplateInstanceId.build(INSTANCE), type, action);
  }

  @SuppressWarnings("unchecked")
  private static ArgumentCaptor<List<ValuerecommenderReindexMessage>> listCaptor() {
    return ArgumentCaptor.forClass(List.class);
  }

  @Test
  void anUpdatedTemplateIsHandedToTheExecutor() throws Exception {
    ValuerecommenderReindexExecutorService executor = startProcessor();

    queueService.enqueueEvent(message(ValuerecommenderReindexMessageResourceType.TEMPLATE,
        ValuerecommenderReindexMessageActionType.UPDATED));

    ArgumentCaptor<List<ValuerecommenderReindexMessage>> batch = listCaptor();
    verify(executor, timeout(30_000)).handleMessages(batch.capture());

    assertEquals(1, batch.getValue().size());
    assertEquals(ValuerecommenderReindexMessageResourceType.TEMPLATE, batch.getValue().get(0).getResourceType());
  }

  /**
   * A created or deleted template carries no new values to recommend from, so it is dropped before
   * the executor sees it. Both are enqueued together with nothing else, so a poll that forwarded
   * them would call the executor.
   */
  @Test
  void aCreatedOrDeletedTemplateIsDropped() throws Exception {
    ValuerecommenderReindexExecutorService executor = startProcessor();

    queueService.enqueueEvent(message(ValuerecommenderReindexMessageResourceType.TEMPLATE,
        ValuerecommenderReindexMessageActionType.CREATED));
    queueService.enqueueEvent(message(ValuerecommenderReindexMessageResourceType.TEMPLATE,
        ValuerecommenderReindexMessageActionType.DELETED));

    // Long enough for several poll intervals to pass
    Thread.sleep(15_000);
    verify(executor, never()).handleMessages(any());

    assertEquals(0, queueService.messageCount(), "the messages were still drained, just not forwarded");
  }

  /** An instance is relevant whatever happened to it, so no action type is filtered out. */
  @Test
  void instanceMessagesSurviveEveryActionType() throws Exception {
    ValuerecommenderReindexExecutorService executor = startProcessor();

    queueService.enqueueEvent(message(ValuerecommenderReindexMessageResourceType.INSTANCE,
        ValuerecommenderReindexMessageActionType.CREATED));
    queueService.enqueueEvent(message(ValuerecommenderReindexMessageResourceType.INSTANCE,
        ValuerecommenderReindexMessageActionType.DELETED));

    ArgumentCaptor<List<ValuerecommenderReindexMessage>> batch = listCaptor();
    verify(executor, timeout(30_000)).handleMessages(batch.capture());

    assertTrue(batch.getValue().size() >= 1, "instance messages should not be filtered");
    assertTrue(batch.getAllValues().stream().flatMap(List::stream)
            .allMatch(m -> m.getResourceType() == ValuerecommenderReindexMessageResourceType.INSTANCE),
        "only instance messages were enqueued");
  }

  /**
   * The poll drains a whole batch rather than one message, so several messages queued between two
   * polls reach the executor together instead of one per interval.
   */
  @Test
  void oneBatchIsDeliveredPerPollRatherThanOneMessage() throws Exception {
    ValuerecommenderReindexExecutorService executor = prepareProcessor();

    for (int i = 0; i < 5; i++) {
      queueService.enqueueEvent(message(ValuerecommenderReindexMessageResourceType.INSTANCE,
          ValuerecommenderReindexMessageActionType.UPDATED));
    }
    processor.start();

    ArgumentCaptor<List<ValuerecommenderReindexMessage>> batch = listCaptor();
    verify(executor, timeout(30_000)).handleMessages(batch.capture());

    assertTrue(batch.getValue().size() > 1,
        "a poll should deliver the batch it drained, not a single message: got " + batch.getValue().size());
  }

  @Test
  void aPollClaimsNoMoreThanTheConfiguredBatchLimit() throws Exception {
    ValuerecommenderReindexExecutorService executor = prepareProcessor();
    for (int i = 0; i < ValuerecommenderReindexQueueProcessor.MAX_BATCH_SIZE + 5; i++) {
      queueService.enqueueEvent(message(ValuerecommenderReindexMessageResourceType.INSTANCE,
          ValuerecommenderReindexMessageActionType.UPDATED));
    }
    processor.start();

    ArgumentCaptor<List<ValuerecommenderReindexMessage>> batch = listCaptor();
    verify(executor, timeout(30_000)).handleMessages(batch.capture());

    assertEquals(ValuerecommenderReindexQueueProcessor.MAX_BATCH_SIZE, batch.getValue().size());
  }

  @Test
  void aFailedBatchIsRetriedThenDeadLetteredInsteadOfLost() throws Exception {
    ValuerecommenderReindexExecutorService executor = prepareProcessor();
    doThrow(new IllegalStateException("reindex failed")).when(executor).handleMessages(any());
    queueService.enqueueEvent(message(ValuerecommenderReindexMessageResourceType.INSTANCE,
        ValuerecommenderReindexMessageActionType.UPDATED));
    processor.start();

    verify(executor, timeout(30_000).times(3)).handleMessages(any());
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
    while (queueService.deadLetterCount() != 1 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }

    assertEquals(1, queueService.deadLetterCount());
    assertEquals(0, queueService.inFlightCount());
  }
}
