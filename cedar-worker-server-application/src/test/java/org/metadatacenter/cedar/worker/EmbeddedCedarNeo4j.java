package org.metadatacenter.cedar.worker;

import org.metadatacenter.bridge.CedarDataServices;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.config.environment.CedarEnvironmentVariableProvider;
import org.metadatacenter.model.SystemComponent;
import org.metadatacenter.rest.context.CedarRequestContext;
import org.metadatacenter.rest.context.CedarRequestContextFactory;
import org.metadatacenter.server.logging.AppLogger;
import org.metadatacenter.server.logging.AppLoggerQueueService;
import org.metadatacenter.server.security.model.user.CedarUser;
import org.metadatacenter.util.test.TestAuthUtil;
import org.metadatacenter.util.test.TestUtil;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.util.HashMap;
import java.util.Map;

/**
 * An in-process Neo4j for integration tests, replacing the live graph database. The worker
 * server resolves the admin user and opens graph sessions during startup, so unlike the group
 * server's variant, everything here runs before the DropwizardAppRule: the embedded server is
 * booted on a random port (so it can never collide with, or write into, a real Neo4j), the
 * CEDAR Neo4j environment variables are redirected, and the graph is seeded with the admin user
 * and the global objects through the same session calls provisioning uses.
 */
public final class EmbeddedCedarNeo4j {

  private static Neo4j embedded;

  private EmbeddedCedarNeo4j() {
  }

  public static synchronized void startRedirectAndSeed() {
    if (embedded != null) {
      return;
    }
    embedded = Neo4jBuilders.newInProcessBuilder().withDisabledServer().build();
    Map<String, String> environment = new HashMap<>(System.getenv());
    environment.put("CEDAR_NEO4J_HOST", embedded.boltURI().getHost());
    environment.put("CEDAR_NEO4J_BOLT_PORT", String.valueOf(embedded.boltURI().getPort()));
    TestUtil.setEnv(environment);

    // The application will reuse this singleton instance, built after the redirection
    Map<String, String> sandbox = CedarEnvironmentVariableProvider.getFor(SystemComponent.SERVER_WORKER);
    CedarConfig cedarConfig = CedarConfig.getInstance(sandbox);
    CedarDataServices.initializeNeo4jServices(cedarConfig);
    CedarRequestContextFactory.init(cedarConfig.getLinkedDataUtil());
    // The Neo4j proxies log every query through AppLogger, which the application only
    // initializes at startup; seeding runs before that
    AppLogger.initLoggerQueueService(new AppLoggerQueueService(cedarConfig.getCacheConfig().getPersistent()),
        SystemComponent.SERVER_WORKER);

    try {
      CedarUser admin = TestAuthUtil.getAdminUser(cedarConfig);
      CedarDataServices.getNeoUserService().createUser(admin);
      CedarRequestContext adminContext = CedarRequestContextFactory.fromUser(admin);
      CedarDataServices.getAdminServiceSession(adminContext).ensureGlobalObjectsExists();
    } catch (Exception e) {
      throw new IllegalStateException("Could not seed the embedded Neo4j", e);
    }
  }

}
