package org.metadatacenter.cedar.worker;

import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.ResourceHelpers;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.metadatacenter.cedar.util.dw.CedarMicroserviceIndexResource;
import org.metadatacenter.model.SystemComponent;
import org.metadatacenter.util.test.EmbeddedCedarMySql;
import org.metadatacenter.util.test.EmbeddedCedarNeo4j;
import org.metadatacenter.util.test.RouteSurface;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Route safety net: probes every endpoint of every resource the worker registers, its commands and
 * the server report, health check and insight resources the shared bootstrap adds, unauthenticated,
 * and requires each to answer 401. A 404/405 means the route vanished or changed verb; any other
 * status means an endpoint lost its authentication assertion. The probes need no fixture; the
 * application boots against the same in-process log store and seeded graph as the smoke test,
 * because startup itself opens both.
 */
@Tag("integration")
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
    List<Class<?>> resources = registeredResourceClasses();
    Assertions.assertTrue(resources.size() > 1, "No resource classes were found on the booted application: "
        + resources);
    RouteSurface.assertEveryRouteAnswers(
        "http://localhost:" + SERVER.getLocalPort(),
        RouteSurface.endpoints(resources),
        401);
  }

  /**
   * Every resource class the booted application registers, its own and those the shared bootstrap
   * adds, read from Jersey rather than listed by hand. The index resource is left out: it serves the
   * server's public index and asserts no login.
   */
  private static List<Class<?>> registeredResourceClasses() {
    ResourceConfig resourceConfig = SERVER.getEnvironment().jersey().getResourceConfig();
    List<Object> registeredComponents = new ArrayList<>();
    registeredComponents.addAll(resourceConfig.getInstances());
    registeredComponents.addAll(resourceConfig.getSingletons());
    registeredComponents.addAll(resourceConfig.getClasses());
    registeredComponents.addAll(resourceConfig.getResources());
    return RouteSurface.registeredResourceClasses(registeredComponents, "org.metadatacenter").stream()
        .filter(resourceClass -> !CedarMicroserviceIndexResource.class.isAssignableFrom(resourceClass))
        .toList();
  }

}
