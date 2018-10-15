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
  private boolean doProcessing;

  public PermissionQueueProcessor(PermissionQueueService permissionQueueService,
                                  SearchPermissionExecutorService searchPermissionExecutorService) {
    this.permissionQueueService = permissionQueueService;
    this.searchPermissionExecutorService = searchPermissionExecutorService;
    doProcessing = true;
  }

  private void digestMessages() {
    log.info("SearchPermissionQueueProcessor.start()");
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
    log.info("SearchPermissionQueueProcessor.stop()");
    log.info("Set looping flag to false");
    doProcessing = false;
    log.info("Close Jedis");
    permissionQueueService.enqueueEvent(null);
    permissionQueueService.close();
  }
}
