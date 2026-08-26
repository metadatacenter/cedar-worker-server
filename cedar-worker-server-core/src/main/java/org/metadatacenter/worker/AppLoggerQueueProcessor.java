package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.logging.AppLoggerExecutorService;
import org.metadatacenter.server.logging.AppLoggerQueueService;
import org.metadatacenter.server.logging.model.AppLogMessage;
import org.metadatacenter.server.queue.util.RepeatedFailureLogger;
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

public class AppLoggerQueueProcessor implements Managed, QueueProcessorMonitor {

  private static final Logger log = LoggerFactory.getLogger(AppLoggerQueueProcessor.class);

  private final AppLoggerQueueService appLoggerQueueService;
  private final AppLoggerExecutorService appLoggerExecutorService;
  private volatile boolean doProcessing;
  private ExecutorService executor;
  private Future<?> workerFuture;
  private volatile Instant lastFailureAt;
  private volatile Instant lastSuccessAt;
  private final RepeatedFailureLogger consumerFailureLogger = new RepeatedFailureLogger();

  public AppLoggerQueueProcessor(AppLoggerQueueService appLoggerQueueService,
                                 AppLoggerExecutorService appLoggerExecutorService) {
    this.appLoggerQueueService = appLoggerQueueService;
    this.appLoggerExecutorService = appLoggerExecutorService;
    doProcessing = true;
  }

  private static final int RETRY_DELAY_SECONDS = 10;
  private static final int MAX_HANDLING_ATTEMPTS = 3;
  private static final int HANDLING_RETRY_DELAY_MILLIS = 1000;

  private void digestMessages() {
    log.info("AppLoggerQueueProcessor.start()");
    while (doProcessing) {
      try {
        consumeMessages();
      } catch (Exception e) {
        if (doProcessing) {
          markFailure();
          // The consumer must never die silently: log the failure and keep retrying, so a
          // queue (Redis) outage suspends processing instead of ending it. An outage lasts across
          // many retries, so only the first failure carries a stack trace
          consumerFailureLogger.report(log, "The application log queue consumer failed, probably because "
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
    log.info("AppLoggerQueueProcessor finished gracefully");
  }

  private void consumeMessages() {
    appLoggerQueueService.initializeBlockingQueue();
    markSuccess();
    log.info("Message count in queue:" + appLoggerQueueService.messageCount());
    List<String> logMessages;
    while (doProcessing) {
      logMessages = appLoggerQueueService.waitForMessages();
      AppLogMessage appLog = null;
      if (logMessages != null && !logMessages.isEmpty()) {
        String value = logMessages.get(1);
        if (!doProcessing) {
          // Leave a message claimed during shutdown in the processing list. The next worker
          // initialization recovers it ahead of newer work, so shutdown cannot discard it.
          break;
        }
        Exception messageError = null;
        try {
          appLog = JsonMapper.MAPPER.readValue(value, AppLogMessage.class);
        } catch (IOException e) {
          log.error("There was an error while deserializing message", e);
          markFailure();
          messageError = e;
        }
        if (appLog != null) {
          handleWithRetries(appLog, value);
        } else if (doProcessing) {
          log.warn("Unable to handle message, it is null.");
          deadLetter(value, messageError == null
              ? new IllegalArgumentException("The application log message was null") : messageError);
        }
      }
    }
  }

  @Override
  public void start() throws Exception {
    executor = Executors.newSingleThreadExecutor();
    workerFuture = executor.submit(this::digestMessages);
  }

  @Override
  public void stop() throws Exception {
    log.info("AppLoggerQueueProcessor.stop()");
    log.info("Set looping flag to false");
    doProcessing = false;
    appLoggerQueueService.interruptWait();
    if (executor != null) {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
    log.info("Close Jedis");
    appLoggerQueueService.close();
  }

  private void handleWithRetries(AppLogMessage message, String rawMessage) {
    for (int attempt = 1; attempt <= MAX_HANDLING_ATTEMPTS; attempt++) {
      if (!doProcessing) {
        return;
      }
      try {
        appLoggerExecutorService.handleLog(message);
        if (!appLoggerQueueService.acknowledge(rawMessage)) {
          throw new IllegalStateException("The processed message could not be acknowledged");
        }
        markSuccess();
        return;
      } catch (Exception e) {
        markFailure();
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
    if (!appLoggerQueueService.deadLetter(rawMessage)) {
      log.error("The application log message could not be moved to the dead-letter queue; "
          + "it remains in-flight for recovery", cause);
    } else {
      log.error("The application log message was moved to " + appLoggerQueueService.getDeadLetterQueueName(), cause);
    }
  }

  private void markFailure() { lastFailureAt = Instant.now(); }
  private void markSuccess() { lastSuccessAt = Instant.now(); }

  @Override public String getProcessorName() { return "app-log"; }
  @Override public boolean isRunning() { return doProcessing && workerFuture != null && !workerFuture.isDone(); }
  @Override public Instant getLastFailureAt() { return lastFailureAt; }
  @Override public Instant getLastSuccessAt() { return lastSuccessAt; }
}
