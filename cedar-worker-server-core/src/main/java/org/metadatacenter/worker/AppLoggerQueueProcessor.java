package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.logging.AppLoggerExecutorService;
import org.metadatacenter.server.logging.AppLoggerQueueService;
import org.metadatacenter.server.logging.model.AppLogMessage;
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
  private boolean doProcessing;

  public AppLoggerQueueProcessor(AppLoggerQueueService appLoggerQueueService,
                                 AppLoggerExecutorService appLoggerExecutorService) {
    this.appLoggerQueueService = appLoggerQueueService;
    this.appLoggerExecutorService = appLoggerExecutorService;
    doProcessing = true;
  }

  private void digestMessages() {
    log.info("AppLoggerQueueProcessor.start()");
    appLoggerQueueService.initializeBlockingQueue();
    List<String> logMessages;
    while (doProcessing) {
      log.info("Waiting for a message in the app logger queue.");
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
        log.error("Unable to handle message, it is null.");
      }
    }
    log.info("SearchPermissionQueueProcessor finished gracefully");
  }

  @Override
  public void start() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
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
  }
}
