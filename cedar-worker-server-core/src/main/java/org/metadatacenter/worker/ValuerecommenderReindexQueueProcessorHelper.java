package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexQueueService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValuerecommenderReindexQueueProcessorHelper implements Managed {

  private static final Logger log = LoggerFactory.getLogger(ValuerecommenderReindexQueueProcessorHelper.class);

  private final ValuerecommenderReindexQueueService valuerecommenderQueueService;

  public ValuerecommenderReindexQueueProcessorHelper(ValuerecommenderReindexQueueService valuerecommenderQueueService) {
    this.valuerecommenderQueueService = valuerecommenderQueueService;
    this.valuerecommenderQueueService.initializeNonBlockingQueue();
  }

  @Override
  public void start() throws Exception {
    log.info("ValuerecommenderReindexQueueProcessorHelper.start()");
  }

  @Override
  public void stop() throws Exception {
    log.info("ValuerecommenderReindexQueueProcessorHelper.stop()");
    log.info("Close Jedis");
    valuerecommenderQueueService.close();
  }
}
