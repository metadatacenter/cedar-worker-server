package org.metadatacenter.cedar.worker;

import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.knowm.dropwizard.sundial.SundialBundle;
import org.knowm.dropwizard.sundial.SundialConfiguration;
import org.metadatacenter.bridge.CedarDataServices;
import org.metadatacenter.cedar.util.dw.CedarMicroserviceApplication;
import org.metadatacenter.cedar.worker.health.WorkerServerHealthCheck;
import org.metadatacenter.cedar.worker.resources.IndexResource;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.model.ServerName;
import org.metadatacenter.server.logging.AppLoggerExecutorService;
import org.metadatacenter.server.logging.dao.ApplicationCypherLogDAO;
import org.metadatacenter.server.logging.dao.ApplicationRequestLogDAO;
import org.metadatacenter.server.logging.dbmodel.ApplicationCypherLog;
import org.metadatacenter.server.logging.dbmodel.ApplicationRequestLog;
import org.metadatacenter.server.queue.util.PermissionQueueService;
import org.metadatacenter.server.search.elasticsearch.service.NodeIndexingService;
import org.metadatacenter.server.search.elasticsearch.service.NodeSearchingService;
import org.metadatacenter.server.search.permission.SearchPermissionExecutorService;
import org.metadatacenter.server.search.util.IndexUtils;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexExecutorService;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexQueueService;
import org.metadatacenter.worker.AppLoggerQueueProcessor;
import org.metadatacenter.worker.PermissionQueueProcessor;
import org.metadatacenter.worker.ValuerecommenderReindexQueueProcessor;
import org.metadatacenter.worker.ValuerecommenderReindexQueueProcessorHelper;

public class WorkerServerApplication extends CedarMicroserviceApplication<WorkerServerConfiguration> {

  private HibernateBundle<WorkerServerConfiguration> hibernate;
  private ApplicationRequestLogDAO requestLogDAO;
  private ApplicationCypherLogDAO cypherLogDAO;
  private static PermissionQueueService permissionQueueService;
  private static SearchPermissionExecutorService searchPermissionExecutorService;
  private static AppLoggerExecutorService appLoggerExecutorService;
  private static ValuerecommenderReindexQueueService valuerecommenderQueueService;
  private static ValuerecommenderReindexExecutorService valuerecommenderExecutorService;

  public static void main(String[] args) throws Exception {
    new WorkerServerApplication().run(args);
  }

  @Override
  protected ServerName getServerName() {
    return ServerName.WORKER;
  }

  @Override
  protected void initializeWithBootstrap(Bootstrap<WorkerServerConfiguration> bootstrap, CedarConfig cedarConfig) {
    hibernate = new CedarWorkerHibernateBundle(
        cedarConfig,
        ApplicationRequestLog.class, new Class[]{
        ApplicationCypherLog.class,
    }
    );
    bootstrap.addBundle(hibernate);

    SundialBundle<WorkerServerConfiguration> sundialBundle = new SundialBundle<>() {
      @Override
      public SundialConfiguration getSundialConfiguration(WorkerServerConfiguration configuration) {
        return cedarConfig.getSundialConfig();
      }
    };
    bootstrap.addBundle(sundialBundle);
  }

  @Override
  public void initializeApp() {

    requestLogDAO = new ApplicationRequestLogDAO(hibernate.getSessionFactory());
    cypherLogDAO = new ApplicationCypherLogDAO(hibernate.getSessionFactory());

    permissionQueueService = new PermissionQueueService(cedarConfig.getCacheConfig().getPersistent());

    IndexUtils indexUtils = new IndexUtils(cedarConfig);
    NodeSearchingService nodeSearchingService = indexUtils.getNodeSearchingService();
    NodeIndexingService nodeIndexingService = indexUtils.getNodeIndexingService();

    searchPermissionExecutorService = new SearchPermissionExecutorService(cedarConfig, indexUtils,
        nodeSearchingService, nodeIndexingService);

    appLoggerExecutorService = new UnitOfWorkAwareProxyFactory(hibernate)
        .create(AppLoggerExecutorService.class,
            new Class[]{ApplicationRequestLogDAO.class, ApplicationCypherLogDAO.class},
            new Object[]{requestLogDAO, cypherLogDAO});

    valuerecommenderQueueService =
        new ValuerecommenderReindexQueueService(cedarConfig.getCacheConfig().getPersistent());
    valuerecommenderExecutorService =
        new ValuerecommenderReindexExecutorService(cedarConfig, valuerecommenderQueueService);
    valuerecommenderExecutorService.init(userService);
  }

  @Override
  public void runApp(WorkerServerConfiguration configuration, Environment environment) {

    final IndexResource index = new IndexResource();
    environment.jersey().register(index);

    final WorkerServerHealthCheck healthCheck = new WorkerServerHealthCheck();
    environment.healthChecks().register("message", healthCheck);

    PermissionQueueProcessor searchPermissionProcessor = new PermissionQueueProcessor(permissionQueueService,
        searchPermissionExecutorService);
    environment.lifecycle().manage(searchPermissionProcessor);

    AppLoggerQueueProcessor appLoggerQueueProcessor = new AppLoggerQueueProcessor(appLoggerQueueService,
        appLoggerExecutorService);
    environment.lifecycle().manage(appLoggerQueueProcessor);

    ValuerecommenderReindexQueueProcessorHelper valuerecommenderReindexQueueProcessorHelper =
        new ValuerecommenderReindexQueueProcessorHelper(valuerecommenderQueueService);
    environment.lifecycle().manage(valuerecommenderReindexQueueProcessorHelper);
    ValuerecommenderReindexQueueProcessor.init(valuerecommenderQueueService, valuerecommenderExecutorService);
  }
}
