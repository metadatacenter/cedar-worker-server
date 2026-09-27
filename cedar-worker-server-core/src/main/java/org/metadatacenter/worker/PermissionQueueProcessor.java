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
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PermissionQueueProcessor implements Managed, QueueProcessorMonitor {

  private static final Logger log = LoggerFactory.getLogger(PermissionQueueProcessor.class);

  private final PermissionQueueService permissionQueueService;
  private final SearchPermissionExecutorService searchPermissionExecutorService;
  private final long handlingRetryDelayMillis;
  // Written by the lifecycle thread in stop(), read by the processing thread in digestMessages():
  // must be volatile or the processing thread may never observe the stop and loop forever.
  private volatile boolean doProcessing;
  private ExecutorService executor;
  private Future<?> workerFuture;
  private volatile Instant lastFailureAt;
  private volatile Instant lastSuccessAt;
  private final RepeatedFailureLogger consumerFailureLogger = new RepeatedFailureLogger();

  public PermissionQueueProcessor(PermissionQueueService permissionQueueService,
                                  SearchPermissionExecutorService searchPermissionExecutorService) {
    this(permissionQueueService, searchPermissionExecutorService, HANDLING_RETRY_DELAY_MILLIS);
  }

  PermissionQueueProcessor(PermissionQueueService permissionQueueService,
                           SearchPermissionExecutorService searchPermissionExecutorService,
                           long handlingRetryDelayMillis) {
    this.permissionQueueService = permissionQueueService;
    this.searchPermissionExecutorService = searchPermissionExecutorService;
    this.handlingRetryDelayMillis = handlingRetryDelayMillis;
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
          markFailure();
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
    markSuccess();
    List<String> permissionMessages;
    while (doProcessing) {
      permissionMessages = permissionQueueService.waitForMessages();
      if (permissionMessages != null && !permissionMessages.isEmpty()) {
        log.info("Got permission message.");
        String value = permissionMessages.get(1);
        if (!doProcessing) {
          // Leave a message claimed during shutdown in the processing list. The next worker
          // initialization recovers it ahead of newer work, so shutdown cannot discard it.
          break;
        }
        List<String> claimed = new java.util.ArrayList<>();
        claimed.add(value);
        claimed.addAll(permissionQueueService.claimAvailableMessages(511));
        List<SearchPermissionQueueEvent> events = new java.util.ArrayList<>();
        List<String> validMessages = new java.util.ArrayList<>();
        for (String raw : claimed) {
          try {
            var event = JsonMapper.TOLERANT_MAPPER.readValue(raw, SearchPermissionQueueEvent.class);
            if (event == null || event.getEventType() == null || event.getId() == null) {
              throw new IllegalArgumentException("Incomplete search-permission event");
            }
            events.add(event);
            validMessages.add(raw);
          } catch (IOException | IllegalArgumentException e) {
            markFailure();
            if (doProcessing) deadLetter(raw, e);
          }
        }
        if (!doProcessing) break;
        if (events.size() == 1) {
          handleWithRetries(events.get(0), validMessages.get(0));
        } else if (!events.isEmpty()) {
          boolean applied = false;
          try {
            searchPermissionExecutorService.handleEvents(events);
            applied = true;
          } catch (Exception failure) {
            markFailure();
            // Isolate a poison event instead of dead-lettering unrelated members of its batch.
            log.warn("Permission batch failed; retrying its events individually", failure);
          }
          if (applied) {
            for (String raw : validMessages) {
              if (!permissionQueueService.acknowledge(raw)) {
                throw new IllegalStateException("The projected batch could not be acknowledged");
              }
            }
            markSuccess();
          } else {
            for (int i = 0; i < events.size() && doProcessing; i++) {
              handleWithRetries(events.get(i), validMessages.get(i));
            }
          }
        }
      }
    }
    log.info("SearchPermissionQueueProcessor finished gracefully");
  }

  /**
   * Applies an event, retrying a failure before giving up on it.
   * <p>
   * The message is already claimed into the processing list by the time it is handled. Most
   * failures are an unreachable Neo4j or OpenSearch, which a retry moments later clears; what a
   * retry cannot clear goes to the dead-letter queue, where it stays visible and replayable rather
   * than leaving search permissions stale.
   */
  private void handleWithRetries(SearchPermissionQueueEvent event, String rawMessage) {
    for (int attempt = 1; attempt <= MAX_HANDLING_ATTEMPTS; attempt++) {
      if (!doProcessing) {
        return;
      }
      try {
        searchPermissionExecutorService.handleEvent(event);
        if (!permissionQueueService.acknowledge(rawMessage)) {
          throw new IllegalStateException("The processed message could not be acknowledged");
        }
        markSuccess();
        return;
      } catch (Exception e) {
        markFailure();
        if (attempt == MAX_HANDLING_ATTEMPTS) {
          log.error("There was an error while handling the message. Giving up after "
              + MAX_HANDLING_ATTEMPTS + " attempts.", e);
          deadLetter(rawMessage, e);
          return;
        }
        log.warn("There was an error while handling the message. Attempt " + attempt + " of "
            + MAX_HANDLING_ATTEMPTS + ", retrying in " + handlingRetryDelayMillis + " ms.", e);
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
      Thread.sleep(handlingRetryDelayMillis);
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
      log.error("The message could not be moved to the dead-letter queue; it remains in-flight "
          + "for recovery. Search permissions for the affected resources remain stale.", cause);
    }
  }

  @Override
  public void start() throws Exception {
    executor = Executors.newSingleThreadExecutor();
    workerFuture = executor.submit(this::digestMessages);
  }

  @Override
  public void stop() throws Exception {
    log.info("SearchPermissionQueueProcessor.stop()");
    log.info("Set looping flag to false");
    doProcessing = false;
    permissionQueueService.interruptWait();
    // Also interrupt a retry delay or the outer connection-backoff delay.
    if (executor != null) {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
    searchPermissionExecutorService.close();
    log.info("Close Jedis");
    permissionQueueService.close();
  }

  private void markFailure() {
    lastFailureAt = Instant.now();
  }

  private void markSuccess() {
    lastSuccessAt = Instant.now();
  }

  @Override
  public String getProcessorName() {
    return "search-permission";
  }

  @Override
  public boolean isRunning() {
    return doProcessing && workerFuture != null && !workerFuture.isDone();
  }

  @Override
  public Instant getLastFailureAt() {
    return lastFailureAt;
  }

  @Override
  public Instant getLastSuccessAt() {
    return lastSuccessAt;
  }
}
