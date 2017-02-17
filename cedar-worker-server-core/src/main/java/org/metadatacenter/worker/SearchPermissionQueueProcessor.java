package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.config.CacheServerPersistent;
import org.metadatacenter.server.cache.util.CacheService;
import org.metadatacenter.server.search.SearchPermissionQueueEvent;
import org.metadatacenter.server.search.permission.SearchPermissionExecutorService;
import org.metadatacenter.util.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SearchPermissionQueueProcessor implements Managed {

  private static final Logger log = LoggerFactory.getLogger(SearchPermissionExecutorService.class);

  private final CacheService cacheService;
  private final CacheServerPersistent cacheConfig;
  private final SearchPermissionExecutorService searchPermissionExecutorService;
  private boolean doProcessing;
  private Jedis jedis;

  public SearchPermissionQueueProcessor(CacheService cacheService, CacheServerPersistent cacheConfig,
                                        SearchPermissionExecutorService searchPermissionExecutorService) {
    this.cacheService = cacheService;
    this.cacheConfig = cacheConfig;
    this.searchPermissionExecutorService = searchPermissionExecutorService;
    doProcessing = true;
  }

  private void digestMessages() {
    log.info("SearchPermissionQueueProcessor.start()");
    jedis = cacheService.getJedis();
    List<String> messages = null;
    String queueName = cacheConfig.getQueueName(CacheService.SEARCH_PERMISSION_QUEUE_ID);
    while (doProcessing) {
      log.info("Waiting for a message in the search permission queue");
      messages = jedis.blpop(0, queueName);
      log.info("Got the message on: " + queueName);
      //key is the name of the variable
      //String key = messages.get(0);
      String value = messages.get(1);
      SearchPermissionQueueEvent event = null;
      try {
        event = JsonMapper.MAPPER.readValue(value, SearchPermissionQueueEvent.class);
      } catch (IOException e) {
        log.error("There was an error while deserializing message", e);
      }
      if (event != null) {
        try {
          log.info("  Event id: " + event.getId());
          log.info("      type: " + event.getEventType());
          log.info(" createdAt: " + event.getCreatedAt());
          log.info("  parentId: " + (event.getParentId() == null ? "null" : event.getParentId().getId()));
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
    executor.submit(() -> {
      this.digestMessages();
    });
  }

  @Override
  public void stop() throws Exception {
    log.info("SearchPermissionQueueProcessor.stop()");
    log.info("set looping flag to false");
    doProcessing = false;
    log.info("close Jedis");
    jedis.close();
  }
}
