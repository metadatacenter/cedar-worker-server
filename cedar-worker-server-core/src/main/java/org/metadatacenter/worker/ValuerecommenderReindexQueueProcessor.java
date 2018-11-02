package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexExecutorService;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexQueueService;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessage;
import org.metadatacenter.util.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ValuerecommenderReindexQueueProcessor implements Managed {

  private static final Logger log = LoggerFactory.getLogger(ValuerecommenderReindexQueueProcessor.class);

  private final ValuerecommenderReindexQueueService valuerecommenderQueueService;
  private final ValuerecommenderReindexExecutorService valuerecommenderExecutorService;
  private final long sleepMillis = 60 * 1000;
  private boolean doProcessing;

  public ValuerecommenderReindexQueueProcessor(ValuerecommenderReindexQueueService valuerecommenderQueueService,
                                               ValuerecommenderReindexExecutorService valuerecommenderExecutorService) {
    this.valuerecommenderQueueService = valuerecommenderQueueService;
    this.valuerecommenderExecutorService = valuerecommenderExecutorService;
    doProcessing = true;
  }

  private void digestMessages() {
    log.info("ValuerecommenderReindexQueueProcessor.start()");
    valuerecommenderQueueService.initializeNonBlockingQueue();
    List<String> logMessages;
    while (doProcessing) {
      logMessages = valuerecommenderQueueService.getAllMessages();
      List<ValuerecommenderReindexMessage> messages = new ArrayList<>();
      if (logMessages != null && !logMessages.isEmpty()) {
        for (String msg : logMessages) {
          ValuerecommenderReindexMessage message = null;
          try {
            message = JsonMapper.MAPPER.readValue(msg, ValuerecommenderReindexMessage.class);
          } catch (IOException e) {
            log.error("There was an error while deserializing message", e);
          }
          if (message != null) {
            messages.add(message);
          }
        }
      }
      if (!messages.isEmpty()) {
        try {
          valuerecommenderExecutorService.handleMessages(messages);
        } catch (Exception e) {
          log.error("There was an error while handling the messages", e);
        }
      } else {
        log.warn("Unable to handle messages, it is an empty list.");
      }
      try {
        Thread.sleep(sleepMillis);
      } catch (InterruptedException e) {
        log.error("Error while sleeping", e);
      }
    }
    log.info("ValuerecommenderReindexQueueProcessor finished gracefully");
  }

  @Override
  public void start() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(this::digestMessages);
  }

  @Override
  public void stop() throws Exception {
    log.info("ValuerecommenderReindexQueueProcessor.stop()");
    log.info("Set looping flag to false");
    doProcessing = false;
    log.info("Close Jedis");
    valuerecommenderQueueService.enqueueEvent(null);
    valuerecommenderQueueService.close();
  }
}
