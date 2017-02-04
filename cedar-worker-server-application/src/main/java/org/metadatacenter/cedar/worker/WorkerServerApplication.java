package org.metadatacenter.cedar.worker;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.metadatacenter.bridge.CedarDataServices;
import org.metadatacenter.cedar.util.dw.CedarDropwizardApplicationUtil;
import org.metadatacenter.cedar.worker.health.WorkerServerHealthCheck;
import org.metadatacenter.cedar.worker.resources.IndexResource;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.config.ElasticsearchConfig;
import org.metadatacenter.config.ElasticsearchSettingsMappingsConfig;
import org.metadatacenter.server.cache.util.CacheService;
import org.metadatacenter.server.search.elasticsearch.ElasticsearchService;
import org.metadatacenter.server.search.permission.PermissionSearchService;
import org.metadatacenter.server.search.permission.SearchPermissionExecutorService;
import org.metadatacenter.worker.SearchPermissionQueueProcessor;

public class WorkerServerApplication extends Application<WorkerServerConfiguration> {

  private static CedarConfig cedarConfig;
  private static CacheService cacheService;
  private static SearchPermissionExecutorService searchPermissionExecutorService;
  private static PermissionSearchService permissionSearchService;


  public static void main(String[] args) throws Exception {
    new WorkerServerApplication().run(args);
  }

  @Override
  public String getName() {
    return "worker-server";
  }

  @Override
  public void initialize(Bootstrap<WorkerServerConfiguration> bootstrap) {
    cedarConfig = CedarConfig.getInstance();
    CedarDataServices.getInstance(cedarConfig);

    CedarDropwizardApplicationUtil.setupKeycloak();

    cacheService = new CacheService(cedarConfig.getCacheConfig().getPersistent());

    ElasticsearchConfig esc = cedarConfig.getElasticsearchConfig();
    ElasticsearchSettingsMappingsConfig essmc = cedarConfig.getElasticsearchSettingsMappingsConfig();

    permissionSearchService = new PermissionSearchService(cedarConfig, new ElasticsearchService(esc, essmc),
        esc.getIndex(),
        esc.getTypePermissions()
    );

    searchPermissionExecutorService = new SearchPermissionExecutorService(cedarConfig, permissionSearchService);
  }

  @Override
  public void run(WorkerServerConfiguration configuration, Environment environment) {

    final IndexResource index = new IndexResource();
    environment.jersey().register(index);

    final WorkerServerHealthCheck healthCheck = new WorkerServerHealthCheck();
    environment.healthChecks().register("message", healthCheck);

    CedarDropwizardApplicationUtil.setupEnvironment(environment);

    SearchPermissionQueueProcessor searchPermissionProcessor = new SearchPermissionQueueProcessor(cacheService,
        cedarConfig.getCacheConfig().getPersistent(),
        searchPermissionExecutorService);
    environment.lifecycle().manage(searchPermissionProcessor);

  }
}
