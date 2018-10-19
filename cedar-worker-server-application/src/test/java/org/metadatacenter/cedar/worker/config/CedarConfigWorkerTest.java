package org.metadatacenter.cedar.worker.config;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.config.environment.CedarEnvironmentVariable;
import org.metadatacenter.config.environment.CedarEnvironmentVariableProvider;
import org.metadatacenter.model.SystemComponent;
import org.metadatacenter.util.test.TestUtil;

import java.util.HashMap;
import java.util.Map;

public class CedarConfigWorkerTest {

  @Before
  public void setEnvironment() {
    Map<String, String> env = new HashMap<>();

    env.put(CedarEnvironmentVariable.CEDAR_HOST.getName(), "metadatacenter.orgx");

    env.put(CedarEnvironmentVariable.CEDAR_NET_GATEWAY.getName(), "127.0.0.1");

    env.put(CedarEnvironmentVariable.CEDAR_ADMIN_USER_API_KEY.getName(), "1234");

    env.put(CedarEnvironmentVariable.CEDAR_NEO4J_USER_NAME.getName(), "neo4j");
    env.put(CedarEnvironmentVariable.CEDAR_NEO4J_USER_PASSWORD.getName(), "password");
    env.put(CedarEnvironmentVariable.CEDAR_NEO4J_HOST.getName(), "127.0.0.1");
    env.put(CedarEnvironmentVariable.CEDAR_NEO4J_BOLT_PORT.getName(), "7687");

    env.put(CedarEnvironmentVariable.CEDAR_MONGO_APP_USER_NAME.getName(), "cedarUser");
    env.put(CedarEnvironmentVariable.CEDAR_MONGO_APP_USER_PASSWORD.getName(), "password");
    env.put(CedarEnvironmentVariable.CEDAR_MONGO_HOST.getName(), "localhost");
    env.put(CedarEnvironmentVariable.CEDAR_MONGO_PORT.getName(), "27017");

    env.put(CedarEnvironmentVariable.CEDAR_ELASTICSEARCH_HOST.getName(), "127.0.0.1");
    env.put(CedarEnvironmentVariable.CEDAR_ELASTICSEARCH_TRANSPORT_PORT.getName(), "9300");

    env.put(CedarEnvironmentVariable.CEDAR_REDIS_PERSISTENT_HOST.getName(), "127.0.0.1");
    env.put(CedarEnvironmentVariable.CEDAR_REDIS_PERSISTENT_PORT.getName(), "6379");

    env.put(CedarEnvironmentVariable.CEDAR_LOG_MYSQL_HOST.getName(), "127.0.0.1");
    env.put(CedarEnvironmentVariable.CEDAR_LOG_MYSQL_PORT.getName(), "3306");
    env.put(CedarEnvironmentVariable.CEDAR_LOG_MYSQL_DB.getName(), "cedar_log");
    env.put(CedarEnvironmentVariable.CEDAR_LOG_MYSQL_USER.getName(), "cedar_log_user");
    env.put(CedarEnvironmentVariable.CEDAR_LOG_MYSQL_PASSWORD.getName(), "cedar_log_password");

    env.put(CedarEnvironmentVariable.CEDAR_SUBMISSION_TEMPLATE_ID_1.getName(), "http://template-id-1");

    env.put(CedarEnvironmentVariable.CEDAR_WORKER_HTTP_PORT.getName(), "9011");
    env.put(CedarEnvironmentVariable.CEDAR_WORKER_ADMIN_PORT.getName(), "9111");
    env.put(CedarEnvironmentVariable.CEDAR_WORKER_STOP_PORT.getName(), "9211");

    env.put(CedarEnvironmentVariable.CEDAR_WORKSPACE_HTTP_PORT.getName(), "9008");
    env.put(CedarEnvironmentVariable.CEDAR_TEMPLATE_HTTP_PORT.getName(), "9001");
    env.put(CedarEnvironmentVariable.CEDAR_USER_HTTP_PORT.getName(), "9005");

    TestUtil.setEnv(env);
  }

  @Test
  public void testGetInstance() throws Exception {
    SystemComponent systemComponent = SystemComponent.SERVER_WORKER;
    Map<String, String> environment = CedarEnvironmentVariableProvider.getFor(systemComponent);
    CedarConfig instance = CedarConfig.getInstance(environment);
    Assert.assertNotNull(instance);
  }

}