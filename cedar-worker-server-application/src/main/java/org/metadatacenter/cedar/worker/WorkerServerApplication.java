package org.metadatacenter.cedar.worker;

import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.metadatacenter.bridge.CedarDataServices;
import org.metadatacenter.cedar.util.dw.CedarMicroserviceApplication;
import org.metadatacenter.cedar.worker.health.WorkerServerHealthCheck;
import org.metadatacenter.cedar.worker.resources.IndexResource;
import org.metadatacenter.server.cache.util.CacheService;
import org.metadatacenter.server.search.elasticsearch.service.*;
import org.metadatacenter.server.search.permission.SearchPermissionExecutorService;
import org.metadatacenter.server.search.util.IndexUtils;
import org.metadatacenter.worker.SearchPermissionQueueProcessor;

public class WorkerServerApplication extends CedarMicroserviceApplication<WorkerServerConfiguration> {

  private static CacheService cacheService;
  private static SearchPermissionExecutorService searchPermissionExecutorService;
  private static UserPermissionIndexingService userPermissionIndexingService;
  private static GroupPermissionIndexingService groupPermissionIndexingService;
  private static NodeSearchingService nodeSearchingService;
  private static GroupPermissionSearchingService groupPermissionSearchingService;

  public static void main(String[] args) throws Exception {
    new WorkerServerApplication().run(args);
  }

  @Override
  public String getName() {
    return "cedar-worker-server";
  }

  @Override
  public void initializeApp(Bootstrap<WorkerServerConfiguration> bootstrap) {
    CedarDataServices.initializeFolderServices(cedarConfig);
    cacheService = new CacheService(cedarConfig.getCacheConfig().getPersistent());

    IndexUtils indexUtils = new IndexUtils(cedarConfig);

    ElasticsearchServiceFactory esServiceFactory = ElasticsearchServiceFactory.getInstance(cedarConfig);

    userPermissionIndexingService = esServiceFactory.userPermissionsIndexingService();
    groupPermissionIndexingService = esServiceFactory.groupPermissionsIndexingService();
    nodeSearchingService = esServiceFactory.nodeSearchingService();
    groupPermissionSearchingService = esServiceFactory.groupPermissionSearchingService();

    searchPermissionExecutorService = new SearchPermissionExecutorService(cedarConfig, indexUtils,
        userPermissionIndexingService, groupPermissionIndexingService, nodeSearchingService,
        groupPermissionSearchingService);
  }

  @Override
  public void runApp(WorkerServerConfiguration configuration, Environment environment) {

    final IndexResource index = new IndexResource();
    environment.jersey().register(index);

    final WorkerServerHealthCheck healthCheck = new WorkerServerHealthCheck();
    environment.healthChecks().register("message", healthCheck);

    SearchPermissionQueueProcessor searchPermissionProcessor = new SearchPermissionQueueProcessor(cacheService,
        cedarConfig.getCacheConfig().getPersistent(),
        searchPermissionExecutorService);
    environment.lifecycle().manage(searchPermissionProcessor);
  }
}
