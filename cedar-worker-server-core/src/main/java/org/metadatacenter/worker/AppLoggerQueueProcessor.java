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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AppLoggerQueueProcessor implements Managed {

  private static final Logger log = LoggerFactory.getLogger(AppLoggerQueueProcessor.class);

  private final AppLoggerQueueService appLoggerQueueService;
  private final AppLoggerExecutorService appLoggerExecutorService;
  private volatile boolean doProcessing;
  private ExecutorService executor;
  private final RepeatedFailureLogger consumerFailureLogger = new RepeatedFailureLogger();

  public AppLoggerQueueProcessor(AppLoggerQueueService appLoggerQueueService,
                                 AppLoggerExecutorService appLoggerExecutorService) {
    this.appLoggerQueueService = appLoggerQueueService;
    this.appLoggerExecutorService = appLoggerExecutorService;
    doProcessing = true;
  }

  private static final int RETRY_DELAY_SECONDS = 10;

  private void digestMessages() {
    log.info("AppLoggerQueueProcessor.start()");
    while (doProcessing) {
      try {
        consumeMessages();
      } catch (Exception e) {
        if (doProcessing) {
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
    log.info("Message count in queue:" + appLoggerQueueService.messageCount());
    List<String> logMessages;
    while (doProcessing) {
      logMessages = appLoggerQueueService.waitForMessages();
      AppLogMessage appLog = null;
      if (logMessages != null && !logMessages.isEmpty()) {
        String value = logMessages.get(1);
        try {
          appLog = JsonMapper.MAPPER.readValue(value, AppLogMessage.class);
        } catch (IOException e) {
          log.error("There was an error while deserializing message", e);
        }
      }
      if (appLog != null) {
        try {
          appLoggerExecutorService.handleLog(appLog);
        } catch (Exception e) {
          log.error("There was an error while handling the message", e);
        }
      } else {
        log.warn("Unable to handle message, it is null.");
      }
    }
  }

  @Override
  public void start() throws Exception {
    executor = Executors.newSingleThreadExecutor();
    executor.submit(this::digestMessages);
  }

  @Override
  public void stop() throws Exception {
    log.info("AppLoggerQueueProcessor.stop()");
    log.info("Set looping flag to false");
    doProcessing = false;
    log.info("Close Jedis");
    appLoggerQueueService.enqueueEvent(null);
    appLoggerQueueService.close();
    if (executor != null) {
      executor.shutdown();
    }
  }
}
