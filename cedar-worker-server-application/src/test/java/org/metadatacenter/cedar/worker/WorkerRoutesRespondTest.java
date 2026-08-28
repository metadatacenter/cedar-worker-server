package org.metadatacenter.cedar.worker;

import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.ResourceHelpers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.metadatacenter.cedar.worker.resources.CommandInclusionSubgraphResource;
import org.metadatacenter.config.environment.CedarEnvironmentSource;
import org.metadatacenter.cedar.util.dw.CedarServerInsightReportResource;
import org.metadatacenter.util.test.RouteSurface;

import java.util.HashMap;
import java.util.Map;

/**
 * Route safety net: probes every worker command and shared diagnostic endpoint unauthenticated and
 * requires each to answer 401. A 404/405 means the route vanished or changed verb; any other status
 * means an endpoint lost its authentication assertion. No fixtures and no backend are involved.
 */
public class WorkerRoutesRespondTest {

  static {
    // Must run before the test support boots the server, which reads the port env vars. Ports are
    // distinct from the dev server and from every other booting test class. Redis goes to a dead
    // port: the worker's queue writes are best-effort, so no live Redis is needed to boot.
    Map<String, String> environment = new HashMap<>(CedarEnvironmentSource.getAll());
    environment.put("CEDAR_WORKER_HTTP_PORT", "19025");
    environment.put("CEDAR_WORKER_ADMIN_PORT", "19125");
    environment.put("CEDAR_WORKER_STOP_PORT", "19225");
    environment.put("CEDAR_REDIS_PERSISTENT_PORT", "1");
    CedarEnvironmentSource.setOverride(environment);
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
