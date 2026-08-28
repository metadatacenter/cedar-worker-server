package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.queue.util.CloneInstancesQueueService;
import org.metadatacenter.server.queue.util.RepeatedFailureLogger;
import org.metadatacenter.server.resource.CloneInstancesExecutorService;
import org.metadatacenter.server.resource.CloneInstancesNotRetryableException;
import org.metadatacenter.server.resource.CloneInstancesQueueEvent;
import org.metadatacenter.util.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CloneInstancesQueueProcessor implements Managed, QueueProcessorMonitor {

  private static final Logger log = LoggerFactory.getLogger(CloneInstancesQueueProcessor.class);

  private final CloneInstancesQueueService cloneInstancesQueueService;
  private final CloneInstancesExecutorService cloneInstancesExecutorService;
  private volatile boolean doProcessing;
  private ExecutorService executor;
  private Future<?> workerFuture;
  private volatile Instant lastFailureAt;
  private volatile Instant lastSuccessAt;
  private final RepeatedFailureLogger consumerFailureLogger = new RepeatedFailureLogger();

  public CloneInstancesQueueProcessor(CloneInstancesQueueService cloneInstancesQueueService,
                                      CloneInstancesExecutorService cloneInstancesExecutorService) {
    this.cloneInstancesQueueService = cloneInstancesQueueService;
    this.cloneInstancesExecutorService = cloneInstancesExecutorService;
    doProcessing = true;
  }

  private static final int RETRY_DELAY_SECONDS = 10;
  private static final int MAX_HANDLING_ATTEMPTS = 3;
  private static final int HANDLING_RETRY_DELAY_MILLIS = 1000;

  private void digestMessages() {
    log.info("CloneInstancesQueueProcessor.start()");
    while (doProcessing) {
      try {
        consumeMessages();
      } catch (Exception e) {
        if (doProcessing) {
          markFailure();
          // The consumer must never die silently: log the failure and keep retrying, so a
          // queue (Redis) outage suspends processing instead of ending it. An outage lasts across
          // many retries, so only the first failure carries a stack trace
          consumerFailureLogger.report(log, "The clone instances queue consumer failed, probably because "
              + "the queue (Redis) became unreachable. Retrying in " + RETRY_DELAY_SECONDS + " seconds.",
              "failures", e);
          try {
            Thread.sleep(RETRY_DELAY_SECONDS * 1000L);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
    }
  }

  private void consumeMessages() {
    cloneInstancesQueueService.initializeBlockingQueue();
    markSuccess();
    List<String> cloneInstancesMessages;
    while (doProcessing) {
      cloneInstancesMessages = cloneInstancesQueueService.waitForMessages();
      CloneInstancesQueueEvent event = null;
      if (cloneInstancesMessages != null && !cloneInstancesMessages.isEmpty()) {
        log.info("Got clone instances message.");
        String value = cloneInstancesMessages.get(1);
        if (!doProcessing) {
          // Leave a message claimed during shutdown in the processing list. The next worker
          // initialization recovers it ahead of newer work, so shutdown cannot discard it.
          break;
        }
        Exception messageError = null;
        try {
          event = JsonMapper.MAPPER.readValue(value, CloneInstancesQueueEvent.class);
        } catch (IOException e) {
          log.error("There was an error while deserializing message", e);
          markFailure();
          messageError = e;
        }
        if (event != null) {
          log.info("    old id: " + event.getOldId());
          log.info("    new id: " + event.getNewId());
          log.info(" createdAt: " + event.getCreatedAt());
          handleWithRetries(event, value);
        } else if (doProcessing) {
          log.warn("Unable to handle message, it is null.");
          deadLetter(value, messageError == null
              ? new IllegalArgumentException("The clone-instances message was null") : messageError);
        }
      }
    }
    log.info("CloneInstancesQueueProcessor finished gracefully");
  }

  @Override
  public void start() throws Exception {
    executor = Executors.newSingleThreadExecutor();
    workerFuture = executor.submit(this::digestMessages);
  }

  @Override
  public void stop() throws Exception {
    log.info("CloneInstancesQueueProcessor.stop()");
    log.info("Set looping flag to false");
    doProcessing = false;
    cloneInstancesQueueService.interruptWait();
    if (executor != null) {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
    log.info("Close Jedis");
    cloneInstancesQueueService.close();
  }

  private void handleWithRetries(CloneInstancesQueueEvent event, String rawMessage) {
    for (int attempt = 1; attempt <= MAX_HANDLING_ATTEMPTS; attempt++) {
      if (!doProcessing) {
        return;
      }
      try {
        cloneInstancesExecutorService.handleEvent(event);
        if (!cloneInstancesQueueService.acknowledge(rawMessage)) {
          throw new IllegalStateException("The processed message could not be acknowledged");
        }
        markSuccess();
        return;
      } catch (Exception e) {
        markFailure();
        // Cloning is not idempotent, and the executor says so when a re-run would duplicate what
        // already succeeded (or repeat per-instance failures no retry can fix). Such an event goes
        // straight to the dead-letter queue.
        if (e instanceof CloneInstancesNotRetryableException) {
          log.error("The clone-instances event is not retried: re-running it would duplicate the "
              + "part of the clone that succeeded.");
          deadLetter(rawMessage, e);
          return;
        }
        if (attempt == MAX_HANDLING_ATTEMPTS) {
          deadLetter(rawMessage, e);
          return;
        }
        try {
          Thread.sleep(HANDLING_RETRY_DELAY_MILLIS);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private void deadLetter(String rawMessage, Exception cause) {
    if (!cloneInstancesQueueService.deadLetter(rawMessage)) {
      log.error("The clone-instances message could not be moved to the dead-letter queue; "
          + "it remains in-flight for recovery", cause);
    } else {
      log.error("The clone-instances message was moved to "
          + cloneInstancesQueueService.getDeadLetterQueueName(), cause);
    }
  }

  private void markFailure() { lastFailureAt = Instant.now(); }
  private void markSuccess() { lastSuccessAt = Instant.now(); }

  @Override public String getProcessorName() { return "clone-instances"; }
  @Override public boolean isRunning() { return doProcessing && workerFuture != null && !workerFuture.isDone(); }
  @Override public Instant getLastFailureAt() { return lastFailureAt; }
  @Override public Instant getLastSuccessAt() { return lastSuccessAt; }
}
