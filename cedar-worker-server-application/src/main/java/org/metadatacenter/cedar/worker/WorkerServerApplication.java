package org.metadatacenter.cedar.worker;

import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import org.metadatacenter.cedar.util.dw.CedarDefaultHealthCheck;
import org.metadatacenter.cedar.util.dw.CedarMicroserviceApplication;
import org.metadatacenter.cedar.worker.resources.CommandInclusionSubgraphResource;
import org.metadatacenter.cedar.worker.resources.IndexResource;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.model.ServerName;
import org.metadatacenter.server.logging.AppLoggerExecutorService;
import org.metadatacenter.server.logging.agg.LogAggregationService;
import org.metadatacenter.server.logging.dao.ApplicationCypherLogDAO;
import org.metadatacenter.server.logging.dao.ApplicationRequestLogDAO;
import org.metadatacenter.server.logging.dao.agg.AggregationRollupDAO;
import org.metadatacenter.server.logging.dao.agg.LogAggregationStateDAO;
import org.metadatacenter.server.logging.dbmodel.ApplicationCypherLog;
import org.metadatacenter.server.logging.dbmodel.ApplicationRequestLog;
import org.metadatacenter.server.logging.dbmodel.agg.AggCypherHourly;
import org.metadatacenter.server.logging.dbmodel.agg.AggCypherQueryCatalog;
import org.metadatacenter.server.logging.dbmodel.agg.AggRequestHourly;
import org.metadatacenter.server.logging.dbmodel.agg.AggRequestUserHourly;
import org.metadatacenter.server.logging.dbmodel.agg.LogAggregationState;
import org.metadatacenter.server.queue.util.CloneInstancesQueueService;
import org.metadatacenter.server.queue.util.PermissionQueueService;
import org.metadatacenter.server.resource.CloneInstancesExecutorService;
import org.metadatacenter.server.search.elasticsearch.service.NodeIndexingService;
import org.metadatacenter.server.search.elasticsearch.service.NodeSearchingService;
import org.metadatacenter.server.search.permission.SearchPermissionExecutorService;
import org.metadatacenter.server.search.util.IndexUtils;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexExecutorService;
import org.metadatacenter.server.valuerecommender.ValuerecommenderReindexQueueService;
import org.metadatacenter.worker.*;

public class WorkerServerApplication extends CedarMicroserviceApplication<WorkerServerConfiguration> {

  private HibernateBundle<WorkerServerConfiguration> hibernate;
  private ApplicationRequestLogDAO requestLogDAO;
  private ApplicationCypherLogDAO cypherLogDAO;
  private static LogAggregationService logAggregationService;
  private static PermissionQueueService permissionQueueService;
  private static SearchPermissionExecutorService searchPermissionExecutorService;
  private static CloneInstancesQueueService cloneInstancesQueueService;
  private static CloneInstancesExecutorService cloneInstancesExecutorService;
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
        AggRequestHourly.class,
        AggCypherHourly.class,
        AggRequestUserHourly.class,
        AggCypherQueryCatalog.class,
        LogAggregationState.class,
    }
    );
    bootstrap.addBundle(hibernate);
  }

  @Override
  public void initializeApp() {

    requestLogDAO = new ApplicationRequestLogDAO(hibernate.getSessionFactory());
    cypherLogDAO = new ApplicationCypherLogDAO(hibernate.getSessionFactory());

    permissionQueueService = new PermissionQueueService(cedarConfig.getCacheConfig().getPersistent());

    IndexUtils indexUtils = new IndexUtils(cedarConfig);
    NodeSearchingService nodeSearchingService = indexUtils.getNodeSearchingService();
    NodeIndexingService nodeIndexingService = indexUtils.getNodeIndexingService();
    ValuerecommenderReindexQueueService valuerecommenderReindexQueueService =
        new ValuerecommenderReindexQueueService(cedarConfig.getCacheConfig().getPersistent());

    searchPermissionExecutorService = new SearchPermissionExecutorService(cedarConfig, indexUtils,
        nodeSearchingService, nodeIndexingService);

    CloneInstancesExecutorService.injectServices(nodeIndexingService, valuerecommenderReindexQueueService);
    cloneInstancesQueueService = new CloneInstancesQueueService(cedarConfig.getCacheConfig().getPersistent());
    cloneInstancesExecutorService = new CloneInstancesExecutorService(cedarConfig);

    appLoggerExecutorService = new UnitOfWorkAwareProxyFactory(hibernate)
        .create(AppLoggerExecutorService.class,
            new Class[]{ApplicationRequestLogDAO.class, ApplicationCypherLogDAO.class},
            new Object[]{requestLogDAO, cypherLogDAO});

    AggregationRollupDAO aggregationRollupDAO = new AggregationRollupDAO(hibernate.getSessionFactory());
    LogAggregationStateDAO logAggregationStateDAO = new LogAggregationStateDAO(hibernate.getSessionFactory());
    logAggregationService = new UnitOfWorkAwareProxyFactory(hibernate)
        .create(LogAggregationService.class,
            new Class[]{AggregationRollupDAO.class, LogAggregationStateDAO.class},
            new Object[]{aggregationRollupDAO, logAggregationStateDAO});

    valuerecommenderQueueService =
        new ValuerecommenderReindexQueueService(cedarConfig.getCacheConfig().getPersistent());
    valuerecommenderExecutorService = new ValuerecommenderReindexExecutorService(cedarConfig,
        valuerecommenderQueueService);
    valuerecommenderExecutorService.init(userService);

    CommandInclusionSubgraphResource.injectUserService(userService);
  }

  @Override
  public void runApp(WorkerServerConfiguration configuration, Environment environment) {

    final IndexResource index = new IndexResource(cedarConfig);
    environment.jersey().register(index);

    final CedarDefaultHealthCheck healthCheck = new CedarDefaultHealthCheck();
    environment.healthChecks().register("message", healthCheck);

    final CommandInclusionSubgraphResource commandInclusionsubgraph = new CommandInclusionSubgraphResource(cedarConfig);
    environment.jersey().register(commandInclusionsubgraph);

    PermissionQueueProcessor searchPermissionProcessor = new PermissionQueueProcessor(permissionQueueService,
        searchPermissionExecutorService);
    environment.lifecycle().manage(searchPermissionProcessor);

    CloneInstancesQueueProcessor cloneInstancesQueueProcessor =
        new CloneInstancesQueueProcessor(cloneInstancesQueueService, cloneInstancesExecutorService);
    environment.lifecycle().manage(cloneInstancesQueueProcessor);

    AppLoggerQueueProcessor appLoggerQueueProcessor = new AppLoggerQueueProcessor(appLoggerQueueService,
        appLoggerExecutorService);
    environment.lifecycle().manage(appLoggerQueueProcessor);

    HistoricalBackfillJob historicalBackfillJob = new HistoricalBackfillJob(logAggregationService);
    environment.lifecycle().manage(historicalBackfillJob);

    LiveAggregatorJob liveAggregatorJob = new LiveAggregatorJob(logAggregationService);
    environment.lifecycle().manage(liveAggregatorJob);

    LogPruneJob logPruneJob = new LogPruneJob(logAggregationService);
    environment.lifecycle().manage(logPruneJob);

    ValuerecommenderReindexQueueProcessor valuerecommenderReindexQueueProcessor =
        new ValuerecommenderReindexQueueProcessor(valuerecommenderQueueService, valuerecommenderExecutorService);
    environment.lifecycle().manage(valuerecommenderReindexQueueProcessor);
  }
}
