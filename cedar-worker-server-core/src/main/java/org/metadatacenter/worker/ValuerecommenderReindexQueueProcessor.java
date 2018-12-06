package org.metadatacenter.worker;

import org.knowm.sundial.exceptions.JobInterruptException;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexExecutorService;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexQueueService;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessage;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessageActionType;
import org.metadatacenter.server.valuerecommender.model.ValuerecommenderReindexMessageResourceType;
import org.metadatacenter.util.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ValuerecommenderReindexQueueProcessor extends org.knowm.sundial.Job {

  private static final Logger log = LoggerFactory.getLogger(ValuerecommenderReindexQueueProcessor.class);

  private static ValuerecommenderReindexQueueService valuerecommenderQueueService;
  private static ValuerecommenderReindexExecutorService valuerecommenderExecutorService;

  public ValuerecommenderReindexQueueProcessor() {
  }

  public static void init(ValuerecommenderReindexQueueService valuerecommenderQueueService,
                          ValuerecommenderReindexExecutorService valuerecommenderExecutorService) {
    ValuerecommenderReindexQueueProcessor.valuerecommenderQueueService = valuerecommenderQueueService;
    ValuerecommenderReindexQueueProcessor.valuerecommenderExecutorService = valuerecommenderExecutorService;
  }

  @Override
  public void doRun() throws JobInterruptException {
    //log.info("doRun...");
    List<String> logMessages = valuerecommenderQueueService.getAllMessages();
    if (logMessages.size() > 0) {
      log.info("Message count: " + logMessages.size());
    }
    if (!logMessages.isEmpty()) {
      List<ValuerecommenderReindexMessage> messages = new ArrayList<>();
      for (String msg : logMessages) {
        ValuerecommenderReindexMessage message = null;
        try {
          message = JsonMapper.MAPPER.readValue(msg, ValuerecommenderReindexMessage.class);
        } catch (IOException e) {
          log.error("There was an error while deserializing message", e);
        }
        if (message != null) {
          boolean doAdd = true;
          if (message.getResourceType() == ValuerecommenderReindexMessageResourceType.TEMPLATE &&
              message.getActionType() != ValuerecommenderReindexMessageActionType.UPDATED) {
            doAdd = false;
          }
          if (doAdd) {
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
        log.warn("After analyzing messages, none remained to be processed.");
      }
    }
  }

}
