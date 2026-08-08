package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.queue.util.CloneInstancesQueueService;
import org.metadatacenter.server.queue.util.RepeatedFailureLogger;
import org.metadatacenter.server.resource.CloneInstancesExecutorService;
import org.metadatacenter.server.resource.CloneInstancesQueueEvent;
import org.metadatacenter.util.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CloneInstancesQueueProcessor implements Managed {

  private static final Logger log = LoggerFactory.getLogger(CloneInstancesQueueProcessor.class);

  private final CloneInstancesQueueService cloneInstancesQueueService;
  private final CloneInstancesExecutorService cloneInstancesExecutorService;
  private volatile boolean doProcessing;
  private ExecutorService executor;
  private final RepeatedFailureLogger consumerFailureLogger = new RepeatedFailureLogger();

  public CloneInstancesQueueProcessor(CloneInstancesQueueService cloneInstancesQueueService,
                                      CloneInstancesExecutorService cloneInstancesExecutorService) {
    this.cloneInstancesQueueService = cloneInstancesQueueService;
    this.cloneInstancesExecutorService = cloneInstancesExecutorService;
    doProcessing = true;
  }

  private static final int RETRY_DELAY_SECONDS = 10;

  private void digestMessages() {
    log.info("CloneInstancesQueueProcessor.start()");
    while (doProcessing) {
      try {
        consumeMessages();
      } catch (Exception e) {
        if (doProcessing) {
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
    List<String> cloneInstancesMessages;
    while (doProcessing) {
      log.info("Waiting for a message in the clone instances queue.");
      cloneInstancesMessages = cloneInstancesQueueService.waitForMessages();
      CloneInstancesQueueEvent event = null;
      if (cloneInstancesMessages != null && !cloneInstancesMessages.isEmpty()) {
        log.info("Got clone instances message.");
        String value = cloneInstancesMessages.get(1);
        try {
          event = JsonMapper.MAPPER.readValue(value, CloneInstancesQueueEvent.class);
        } catch (IOException e) {
          log.error("There was an error while deserializing message", e);
        }
      }
      if (event != null) {
        try {
          log.info("    old id: " + event.getOldId());
          log.info("    new id: " + event.getNewId());
          log.info(" createdAt: " + event.getCreatedAt());
          cloneInstancesExecutorService.handleEvent(event);
        } catch (Exception e) {
          log.error("There was an error while handling the message", e);
        }
      } else {
        log.warn("Unable to handle message, it is null.");
      }
    }
    log.info("CloneInstancesQueueProcessor finished gracefully");
  }

  @Override
  public void start() throws Exception {
    executor = Executors.newSingleThreadExecutor();
    executor.submit(this::digestMessages);
  }

  @Override
  public void stop() throws Exception {
    log.info("CloneInstancesQueueProcessor.stop()");
    log.info("Set looping flag to false");
    doProcessing = false;
    log.info("Close Jedis");
    cloneInstancesQueueService.enqueueEvent(null);
    cloneInstancesQueueService.close();
    if (executor != null) {
      executor.shutdown();
    }
  }
}
