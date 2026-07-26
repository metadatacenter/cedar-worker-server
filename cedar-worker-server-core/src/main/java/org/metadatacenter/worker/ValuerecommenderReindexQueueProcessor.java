package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexExecutorService;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexQueueService;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessage;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessageActionType;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessageResourceType;
import org.metadatacenter.util.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Drains the value-recommender reindex queue and hands the batched messages to the reindex
 * executor. The queue is non-blocking (a whole batch is read per poll, then de-duplicated), so
 * this polls on a fixed interval rather than blocking on a dequeue. It replaces an earlier
 * Sundial-scheduled job: Sundial was a scheduling framework wrapping this one queue drain, so the
 * poll loop is kept in-process here, consistent with the worker's other queue consumers and with
 * no third-party scheduler. Each poll borrows its own connection, so a queue (Redis) outage makes
 * a poll fail and be retried on the next interval rather than ending the consumer.
 */
public class ValuerecommenderReindexQueueProcessor implements Managed {

  private static final Logger log = LoggerFactory.getLogger(ValuerecommenderReindexQueueProcessor.class);

  private static final int POLL_INTERVAL_SECONDS = 5;

  private final ValuerecommenderReindexQueueService valuerecommenderQueueService;
  private final ValuerecommenderReindexExecutorService valuerecommenderExecutorService;
  private volatile boolean doProcessing;
  private ExecutorService executor;

  public ValuerecommenderReindexQueueProcessor(ValuerecommenderReindexQueueService valuerecommenderQueueService,
                                               ValuerecommenderReindexExecutorService valuerecommenderExecutorService) {
    this.valuerecommenderQueueService = valuerecommenderQueueService;
    this.valuerecommenderExecutorService = valuerecommenderExecutorService;
    this.doProcessing = true;
  }

  @Override
  public void start() throws Exception {
    log.info("ValuerecommenderReindexQueueProcessor.start()");
    executor = Executors.newSingleThreadExecutor();
    executor.submit(this::pollLoop);
  }

  private void pollLoop() {
    while (doProcessing) {
      try {
        processMessages();
      } catch (Exception e) {
        // A poll must never end the consumer: log the failure (typically an unreachable Redis)
        // and try again on the next interval
        log.error("The value-recommender reindex poll failed, probably because the queue (Redis) "
            + "became unreachable. Retrying on the next interval.", e);
      }
      try {
        Thread.sleep(POLL_INTERVAL_SECONDS * 1000L);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    log.info("ValuerecommenderReindexQueueProcessor finished gracefully");
  }

  private void processMessages() {
    List<String> logMessages = valuerecommenderQueueService.getAllMessages();
    if (logMessages.size() > 0) {
      log.info("Message count: " + logMessages.size());
    }
    if (!logMessages.isEmpty()) {
      List<ValuerecommenderReindexMessage> messages = new ArrayList<>();
      for (String msg : logMessages) {
        ValuerecommenderReindexMessage message = null;
        try {
          message = JsonMapper.MAPPER.readValue(msg, ValuerecommenderReindexMessage.class);
        } catch (IOException e) {
          log.error("There was an error while deserializing message", e);
        }
        if (message != null) {
          boolean doAdd = true;
          if (message.getResourceType() == ValuerecommenderReindexMessageResourceType.TEMPLATE &&
              message.getActionType() != ValuerecommenderReindexMessageActionType.UPDATED) {
            doAdd = false;
          }
          if (doAdd) {
            messages.add(message);
          }
        }
      }
      if (!messages.isEmpty()) {
        try {
          valuerecommenderExecutorService.handleMessages(messages);
        } catch (Exception e) {
          log.error("There was an error while handling the messages", e);
        }
      } else {
        log.warn("After analyzing messages, none remained to be processed.");
      }
    }
  }

  @Override
  public void stop() throws Exception {
    log.info("ValuerecommenderReindexQueueProcessor.stop()");
    doProcessing = false;
    if (executor != null) {
      executor.shutdownNow();
    }
    valuerecommenderQueueService.close();
  }

}
