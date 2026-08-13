package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.queue.util.PermissionQueueService;
import org.metadatacenter.server.queue.util.RepeatedFailureLogger;
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
  private final RepeatedFailureLogger consumerFailureLogger = new RepeatedFailureLogger();

  public PermissionQueueProcessor(PermissionQueueService permissionQueueService,
                                  SearchPermissionExecutorService searchPermissionExecutorService) {
    this.permissionQueueService = permissionQueueService;
    this.searchPermissionExecutorService = searchPermissionExecutorService;
    doProcessing = true;
  }

  private static final int RETRY_DELAY_SECONDS = 10;
  private static final int MAX_HANDLING_ATTEMPTS = 3;
  // Kept short on purpose. The queue is drained by a single thread, so every second spent retrying
  // one event is a second the events behind it wait. A retry is here to absorb a brief blip; an
  // outage that outlasts it belongs in the dead-letter queue, to be replayed once the cause is
  // fixed, rather than holding up the estate's permission updates
  private static final int HANDLING_RETRY_DELAY_MILLIS = 1000;

  private void digestMessages() {
    log.info("SearchPermissionQueueProcessor.start()");
    while (doProcessing) {
      try {
        consumeMessages();
      } catch (Exception e) {
        if (doProcessing) {
          // The consumer must never die silently: log the failure and keep retrying, so a
          // queue (Redis) outage suspends processing instead of ending it. An outage lasts across
          // many retries, so only the first failure carries a stack trace
          consumerFailureLogger.report(log, "The search permission queue consumer failed, probably because "
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
    permissionQueueService.initializeBlockingQueue();
    List<String> permissionMessages;
    while (doProcessing) {
      log.info("Waiting for a message in the search permission queue.");
      permissionMessages = permissionQueueService.waitForMessages();
      SearchPermissionQueueEvent event = null;
      String value = null;
      if (permissionMessages != null && !permissionMessages.isEmpty()) {
        log.info("Got permission message.");
        value = permissionMessages.get(1);
        try {
          event = JsonMapper.MAPPER.readValue(value, SearchPermissionQueueEvent.class);
        } catch (IOException e) {
          log.error("There was an error while deserializing message", e);
          // Retrying will not make an unparseable message parse, so park it immediately
          deadLetter(value, e);
        }
      }
      if (event != null) {
        log.info("  event id: " + event.getId());
        log.info("      type: " + event.getEventType());
        log.info(" createdAt: " + event.getCreatedAt());
        handleWithRetries(event, value);
      } else {
        log.warn("Unable to handle message, it is null.");
      }
    }
    log.info("SearchPermissionQueueProcessor finished gracefully");
  }

  /**
   * Applies an event, retrying a failure before giving up on it.
   * <p>
   * The message is already off the queue by the time it is handled, so a failure here is the last
   * chance to keep the event. Most failures are an unreachable Neo4j or OpenSearch, which a retry
   * moments later clears; what a retry cannot clear goes to the dead-letter queue, where it stays
   * visible and replayable rather than being lost and leaving search permissions stale.
   */
  private void handleWithRetries(SearchPermissionQueueEvent event, String rawMessage) {
    for (int attempt = 1; attempt <= MAX_HANDLING_ATTEMPTS; attempt++) {
      try {
        searchPermissionExecutorService.handleEvent(event);
        return;
      } catch (Exception e) {
        if (attempt == MAX_HANDLING_ATTEMPTS) {
          log.error("There was an error while handling the message. Giving up after "
              + MAX_HANDLING_ATTEMPTS + " attempts.", e);
          deadLetter(rawMessage, e);
          return;
        }
        log.warn("There was an error while handling the message. Attempt " + attempt + " of "
            + MAX_HANDLING_ATTEMPTS + ", retrying in " + HANDLING_RETRY_DELAY_MILLIS + " ms.", e);
        if (!sleepBeforeRetry()) {
          return;
        }
      }
    }
  }

  /**
   * @return whether the wait completed. A false means the thread was interrupted and the caller
   * must stop rather than continue retrying.
   */
  private boolean sleepBeforeRetry() {
    try {
      Thread.sleep(HANDLING_RETRY_DELAY_MILLIS);
      return true;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private void deadLetter(String rawMessage, Exception cause) {
    if (permissionQueueService.deadLetter(rawMessage)) {
      log.error("The message was moved to " + permissionQueueService.getDeadLetterQueueName()
          + ". Search permissions for the affected resources are stale until it is replayed.", cause);
    } else {
      log.error("The message could not be moved to the dead-letter queue and is lost. "
          + "Search permissions for the affected resources are stale.", cause);
    }
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
