package org.metadatacenter.cedar.worker;

import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.ResourceHelpers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.metadatacenter.cedar.worker.resources.CommandInclusionSubgraphResource;
import org.metadatacenter.cedar.util.dw.CedarServerInsightReportResource;
import org.metadatacenter.model.SystemComponent;
import org.metadatacenter.util.test.EmbeddedCedarMySql;
import org.metadatacenter.util.test.EmbeddedCedarNeo4j;
import org.metadatacenter.util.test.RouteSurface;

import java.util.Map;

/**
 * Route safety net: probes every worker command and shared diagnostic endpoint unauthenticated and
 * requires each to answer 401. A 404/405 means the route vanished or changed verb; any other status
 * means an endpoint lost its authentication assertion. The probes need no fixture; the application
 * boots against the same in-process log store and seeded graph as the smoke test, because startup
 * itself opens both.
 */
public class WorkerRoutesRespondTest {

  static {
    // Must run before the test support boots the server, which reads the port env vars. The class
    // installs its own environment rather than copying whatever the previous class left: the test
    // support restores the process environment after every class, so a copy taken here would point
    // the log store at the CI MySQL port, where nothing listens. Ports are assigned by the OS, so
    // they cannot collide with the dev server or another test. Redis goes to a dead port: the
    // worker's queue writes are best-effort, so no live Redis is needed to boot.
    EmbeddedCedarMySql.startAndRedirectEnvironment("CEDAR_LOG_MYSQL", Map.of(
        "CEDAR_WORKER_HTTP_PORT", "0",
        "CEDAR_WORKER_ADMIN_PORT", "0",
        "CEDAR_WORKER_STOP_PORT", "0",
        "CEDAR_REDIS_PERSISTENT_PORT", "1"));
    EmbeddedCedarNeo4j.startRedirectAndSeed(SystemComponent.SERVER_WORKER);
  }

  private static final DropwizardTestSupport<WorkerServerConfiguration> SERVER =
      new DropwizardTestSupport<>(WorkerServerApplication.class, ResourceHelpers.resourceFilePath("test-config.yml"));

  @BeforeAll
  public static void startServer() throws Exception {
    SERVER.before();
  }

  @AfterAll
  public static void stopServer() {
    SERVER.after();
  }

  @Test
  public void everyRouteRejectsAnUnauthenticatedRequest() {
    RouteSurface.assertEveryRouteAnswers(
        "http://localhost:" + SERVER.getLocalPort(),
        RouteSurface.endpoints(
            CommandInclusionSubgraphResource.class,
            CedarServerInsightReportResource.class),
        401);
  }

}
