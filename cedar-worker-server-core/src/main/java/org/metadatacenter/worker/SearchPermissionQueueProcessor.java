package org.metadatacenter.worker;

import io.dropwizard.lifecycle.Managed;
import org.metadatacenter.config.CacheServerPersistent;
import org.metadatacenter.server.cache.util.CacheService;
import org.metadatacenter.server.search.SearchPermissionQueueEvent;
import org.metadatacenter.server.search.permission.PermissionSearchService;
import org.metadatacenter.server.search.permission.SearchPermissionExecutorService;
import org.metadatacenter.util.json.JsonMapper;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SearchPermissionQueueProcessor implements Managed {

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
    System.out.println("SearchPermissionQueueProcessor.start()");
    jedis = cacheService.getJedis();
    List<String> messages = null;
    while (doProcessing) {
      System.out.println("Waiting for a message in the search permission queue");
      messages = jedis.blpop(0, cacheConfig.getQueueName(CacheService.SEARCH_PERMISSION_QUEUE_ID));
      System.out.println("Got the message");
      String key = messages.get(0);
      String value = messages.get(1);
      System.out.println("KEY  :" + key);
      System.out.println("VALUE:" + value);
      SearchPermissionQueueEvent event = null;
      try {
        event = JsonMapper.MAPPER.readValue(value, SearchPermissionQueueEvent.class);
      } catch (IOException e) {
        //TODO: log here
        e.printStackTrace();
      }
      if (event != null) {
        System.out.println("Event id  :" + event.getId());
        System.out.println("      type:" + event.getEventType());
        System.out.println(" createdAt:" + event.getCreatedAt());
        searchPermissionExecutorService.handleEvent(event);
      } else {
        System.out.println("Error decoding message");
      }
    }
    System.out.println("SearchPermissionQueueProcessor finished");
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
    System.out.println("SearchPermissionQueueProcessor.stop()");
    System.out.println("set looping flag to false");
    doProcessing = false;
    System.out.println("close Jedis");
    jedis.close();
  }
}
