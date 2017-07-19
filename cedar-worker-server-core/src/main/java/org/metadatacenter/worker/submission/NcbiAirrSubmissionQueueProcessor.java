package org.metadatacenter.worker.submission;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.config.CacheServerPersistent;
import org.metadatacenter.queue.NcbiAirrSubmissionQueueEvent;
import org.metadatacenter.server.cache.util.CacheService;
import org.metadatacenter.queue.QueueEvent;
import org.metadatacenter.util.json.JsonMapper;
import org.metadatacenter.worker.submission.NcbiAirrSubmissionExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NcbiAirrSubmissionQueueProcessor implements Managed {

  private static final Logger log = LoggerFactory.getLogger(NcbiAirrSubmissionExecutorService.class);

  private final CacheService cacheService;
  private final CacheServerPersistent cacheConfig;
  private final NcbiAirrSubmissionExecutorService ncbiAirrSubmissionExecutorService;
  private boolean doProcessing;
  private Jedis jedis;

  public NcbiAirrSubmissionQueueProcessor(CacheService cacheService, CacheServerPersistent cacheConfig,
                                          NcbiAirrSubmissionExecutorService ncbiAirrSubmissionExecutorService) {
    this.cacheService = cacheService;
    this.cacheConfig = cacheConfig;
    this.ncbiAirrSubmissionExecutorService = ncbiAirrSubmissionExecutorService;
    doProcessing = true;
  }

  private void digestMessages() {
    log.info("NcbiAirrSubmissionQueueProcessor.start()");
    jedis = cacheService.getJedis();
    List<String> submissions = null;
    String queueName = cacheConfig.getQueueName(QueueEvent.NCBI_SUBMISSION_QUEUE_ID);
    while (doProcessing) {
      log.info("Waiting for a submission in the NCBI-AIRR submission queue");
      submissions = jedis.blpop(0, queueName);
      log.info("Got the submission on: " + queueName);
      //String value = submissions.get(1);
      String value = submissions.get(0);
      NcbiAirrSubmissionQueueEvent event = null;
      try {
        event = JsonMapper.MAPPER.readValue(value, NcbiAirrSubmissionQueueEvent.class);
      } catch (IOException e) {
        log.error("There was an error while deserializing submission", e);
      }
      if (event != null) {
        try {
          log.info("   Event id: " + event.getId());
          log.info("   queue id: " + event.getQueueId());
          log.info("  no. files: " + event.getSubmission().getLocalFilePaths().size());
          log.info(" created at: " + event.getCreatedAt());
          ncbiAirrSubmissionExecutorService.handleEvent(event);
        } catch (Exception e) {
          log.error("There was an error while handling the message", e);
        }
      } else {
        log.error("Unable to handle message, it is null.");
      }
    }
    log.info("NcbiAirrSubmissionQueueProcessor finished gracefully");
  }

  @Override
  public void start() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      this.digestMessages();
    });
  }

  @Override
  public void stop() throws Exception {
    log.info("NcbiAirrSubmissionQueueProcessor.stop()");
    log.info("set looping flag to false");
    doProcessing = false;
    log.info("close Jedis");
    jedis.close();
  }
}
