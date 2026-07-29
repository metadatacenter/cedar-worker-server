package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.queue.util.PermissionQueueService;
import org.metadatacenter.server.search.SearchPermissionQueueEvent;
import org.metadatacenter.server.search.permission.SearchPermissionExecutorService;
import org.metadatacenter.util.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PermissionQueueProcessor implements Managed {

  private static final Logger log = LoggerFactory.getLogger(PermissionQueueProcessor.class);

  private final PermissionQueueService permissionQueueService;
  private final SearchPermissionExecutorService searchPermissionExecutorService;
  // Written by the lifecycle thread in stop(), read by the processing thread in digestMessages():
  // must be volatile or the processing thread may never observe the stop and loop forever.
  private volatile boolean doProcessing;
  private ExecutorService executor;

  public PermissionQueueProcessor(PermissionQueueService permissionQueueService,
                                  SearchPermissionExecutorService searchPermissionExecutorService) {
    this.permissionQueueService = permissionQueueService;
    this.searchPermissionExecutorService = searchPermissionExecutorService;
    doProcessing = true;
  }

  private static final int RETRY_DELAY_SECONDS = 10;

  private void digestMessages() {
    log.info("SearchPermissionQueueProcessor.start()");
    while (doProcessing) {
      try {
        consumeMessages();
      } catch (Exception e) {
        if (doProcessing) {
          // The consumer must never die silently: log the failure and keep retrying, so a
          // queue (Redis) outage suspends processing instead of ending it
          log.error("The search permission queue consumer failed, probably because the queue (Redis) became unreachable. "
              + "Retrying in " + RETRY_DELAY_SECONDS + " seconds.", e);
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
    permissionQueueService.initializeBlockingQueue();
    List<String> permissionMessages;
    while (doProcessing) {
      log.info("Waiting for a message in the search permission queue.");
      permissionMessages = permissionQueueService.waitForMessages();
      SearchPermissionQueueEvent event = null;
      if (permissionMessages != null && !permissionMessages.isEmpty()) {
        log.info("Got permission message.");
        String value = permissionMessages.get(1);
        try {
          event = JsonMapper.MAPPER.readValue(value, SearchPermissionQueueEvent.class);
        } catch (IOException e) {
          log.error("There was an error while deserializing message", e);
        }
      }
      if (event != null) {
        try {
          log.info("  event id: " + event.getId());
          log.info("      type: " + event.getEventType());
          log.info(" createdAt: " + event.getCreatedAt());
          searchPermissionExecutorService.handleEvent(event);
        } catch (Exception e) {
          log.error("There was an error while handling the message", e);
        }
      } else {
        log.warn("Unable to handle message, it is null.");
      }
    }
    log.info("SearchPermissionQueueProcessor finished gracefully");
  }

  @Override
  public void start() throws Exception {
    executor = Executors.newSingleThreadExecutor();
    executor.submit(this::digestMessages);
  }

  @Override
  public void stop() throws Exception {
    log.info("SearchPermissionQueueProcessor.stop()");
    log.info("Set looping flag to false");
    doProcessing = false;
    log.info("Close Jedis");
    permissionQueueService.enqueueEvent(null);
    permissionQueueService.close();
    // Reclaim the worker thread. digestMessages() has already been asked to stop (doProcessing) and
    // unblocked (the null event above), so an orderly shutdown lets it finish and terminates the thread.
    if (executor != null) {
      executor.shutdown();
    }
  }
}
