package org.metadatacenter.cedar.worker;

import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.config.HibernateConfig;

import java.util.Map;

public class CedarWorkerHibernateBundle extends HibernateBundle<WorkerServerConfiguration> {

  private final CedarConfig cedarConfig;

  protected CedarWorkerHibernateBundle(CedarConfig cedarConfig, Class<?> entity, Class<?>[] entities) {
    super(entity, entities);
    this.cedarConfig = cedarConfig;
  }

  @Override
  public PooledDataSourceFactory getDataSourceFactory(WorkerServerConfiguration workerServerConfiguration) {
    HibernateConfig dbLoggingConfig = cedarConfig.getDBLoggingConfig();
    DataSourceFactory database = new DataSourceFactory();
    database.setUrl(dbLoggingConfig.getUrl());
    database.setUser(dbLoggingConfig.getUser());
    database.setPassword(dbLoggingConfig.getPassword());
    database.setDriverClass(dbLoggingConfig.getDriverClass());
    Map<String, String> properties = database.getProperties();
    properties.putAll(dbLoggingConfig.getProperties());
    return database;
  }
}
