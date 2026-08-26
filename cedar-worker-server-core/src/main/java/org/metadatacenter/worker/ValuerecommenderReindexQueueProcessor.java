package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.queue.util.RepeatedFailureLogger;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexExecutorService;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexQueueService;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessage;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessageActionType;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessageResourceType;
import org.metadatacenter.util.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Drains the value-recommender reindex queue and hands the batched messages to the reindex
 * executor. The queue is non-blocking (a whole batch is read per poll, then de-duplicated), so
 * this polls on a fixed interval rather than blocking on a dequeue. It replaces an earlier
 * Sundial-scheduled job: Sundial was a scheduling framework wrapping this one queue drain, so the
 * poll loop is kept in-process here, consistent with the worker's other queue consumers and with
 * no third-party scheduler. Each poll borrows its own connection, so a queue (Redis) outage makes
 * a poll fail and be retried on the next interval rather than ending the consumer.
 */
public class ValuerecommenderReindexQueueProcessor implements Managed, QueueProcessorMonitor {

  private static final Logger log = LoggerFactory.getLogger(ValuerecommenderReindexQueueProcessor.class);

  private static final int POLL_INTERVAL_SECONDS = 5;
  static final int MAX_BATCH_SIZE = 100;
  private static final int MAX_HANDLING_ATTEMPTS = 3;

  private final ValuerecommenderReindexQueueService valuerecommenderQueueService;
  private final ValuerecommenderReindexExecutorService valuerecommenderExecutorService;
  private volatile boolean doProcessing;
  private ExecutorService executor;
  private Future<?> workerFuture;
  private volatile Instant lastFailureAt;
  private volatile Instant lastSuccessAt;
  private int consecutiveHandlingFailures;
  private final RepeatedFailureLogger pollFailureLogger = new RepeatedFailureLogger();

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
    workerFuture = executor.submit(this::pollLoop);
  }

  private void pollLoop() {
    while (doProcessing) {
      try {
        processMessages();
      } catch (Exception e) {
        markFailure();
        // A poll must never end the consumer: log the failure (typically an unreachable Redis)
        // and try again on the next interval. An outage spans many polls, so only the first
        // failure carries a stack trace
        pollFailureLogger.report(log, "The value-recommender reindex poll failed, probably because "
            + "the queue (Redis) became unreachable. Retrying on the next interval.", "failures", e);
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
    List<String> logMessages = valuerecommenderQueueService.claimMessages(MAX_BATCH_SIZE);
    if (logMessages.isEmpty()) {
      markSuccess();
    }
    if (logMessages.size() > 0) {
      log.info("Message count: " + logMessages.size());
    }
    if (!logMessages.isEmpty()) {
      List<ValuerecommenderReindexMessage> messages = new ArrayList<>();
      List<String> acceptedRawMessages = new ArrayList<>();
      for (String msg : logMessages) {
        ValuerecommenderReindexMessage message = null;
        Exception messageError = null;
        try {
          message = JsonMapper.MAPPER.readValue(msg, ValuerecommenderReindexMessage.class);
        } catch (IOException e) {
          log.error("There was an error while deserializing message", e);
          markFailure();
          messageError = e;
        }
        if (message != null) {
          boolean doAdd = true;
          if (message.getResourceType() == ValuerecommenderReindexMessageResourceType.TEMPLATE &&
              message.getActionType() != ValuerecommenderReindexMessageActionType.UPDATED) {
            doAdd = false;
          }
          if (doAdd) {
            messages.add(message);
            acceptedRawMessages.add(msg);
          } else {
            acknowledge(msg);
          }
        } else {
          deadLetter(msg, messageError == null
              ? new IllegalArgumentException("The value-recommender message was null") : messageError);
        }
      }
      if (!messages.isEmpty()) {
        try {
          valuerecommenderExecutorService.handleMessages(messages);
          for (String rawMessage : acceptedRawMessages) {
            acknowledge(rawMessage);
          }
          consecutiveHandlingFailures = 0;
          markSuccess();
        } catch (Exception e) {
          consecutiveHandlingFailures++;
          markFailure();
          log.error("There was an error while handling the messages", e);
          if (consecutiveHandlingFailures >= MAX_HANDLING_ATTEMPTS) {
            for (String rawMessage : acceptedRawMessages) {
              deadLetter(rawMessage, e);
            }
            consecutiveHandlingFailures = 0;
          }
        }
      } else {
        log.warn("After analyzing messages, none remained to be processed.");
      }
    }
  }

  private void acknowledge(String rawMessage) {
    if (!valuerecommenderQueueService.acknowledge(rawMessage)) {
      throw new IllegalStateException("The processed value-recommender message could not be acknowledged");
    }
  }

  private void deadLetter(String rawMessage, Exception cause) {
    if (!valuerecommenderQueueService.deadLetter(rawMessage)) {
      log.error("The value-recommender message could not be moved to the dead-letter queue; "
          + "it remains in-flight for recovery", cause);
    } else {
      log.error("The value-recommender message was moved to "
          + valuerecommenderQueueService.getDeadLetterQueueName(), cause);
    }
  }

  private void markFailure() { lastFailureAt = Instant.now(); }
  private void markSuccess() { lastSuccessAt = Instant.now(); }

  @Override public String getProcessorName() { return "value-recommender"; }
  @Override public boolean isRunning() { return doProcessing && workerFuture != null && !workerFuture.isDone(); }
  @Override public Instant getLastFailureAt() { return lastFailureAt; }
  @Override public Instant getLastSuccessAt() { return lastSuccessAt; }

  @Override
  public void stop() throws Exception {
    log.info("ValuerecommenderReindexQueueProcessor.stop()");
    doProcessing = false;
    if (executor != null) {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
    valuerecommenderQueueService.close();
  }

}
