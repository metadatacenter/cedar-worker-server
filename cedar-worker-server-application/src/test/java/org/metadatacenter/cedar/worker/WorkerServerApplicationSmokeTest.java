package org.metadatacenter.cedar.worker;

import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.ResourceHelpers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.metadatacenter.model.SystemComponent;
import org.metadatacenter.util.test.EmbeddedCedarMySql;
import org.metadatacenter.util.test.EmbeddedCedarNeo4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

/**
 * Boots the real application through Dropwizard test support. The worker server resolves the
 * admin user and opens graph sessions at startup, so an in-process Neo4j is seeded before the
 * test support runs; MySQL and Redis are still expected, as in the development environment. Beyond
 * configuration rot, this catches startup regressions in the permission and queue executor
 * wiring - the kind that previously surfaced only as a NullPointerException at boot.
 */
public class WorkerServerApplicationSmokeTest {

  static {
    // Must run before the test support: startup itself needs the graph and the admin user.
    // The application log store comes from an in-process MariaDB; the MySQL redirection must
    // precede the Neo4j seeding, which builds the CedarConfig singleton. Redis goes to a dead
    // port: queue writes are best-effort, the consumers take their first contact on background
    // threads, and the polling services borrow a connection per poll. OS-assigned server ports,
    // so the test instance never collides with a running dev server.
    EmbeddedCedarMySql.startAndRedirectEnvironment("CEDAR_LOG_MYSQL", Map.of(
        "CEDAR_WORKER_HTTP_PORT", "0",
        "CEDAR_WORKER_ADMIN_PORT", "0",
        "CEDAR_WORKER_STOP_PORT", "0",
        "CEDAR_REDIS_PERSISTENT_PORT", "1"));
    EmbeddedCedarNeo4j.startRedirectAndSeed(SystemComponent.SERVER_WORKER);
  }

  public static final DropwizardTestSupport<WorkerServerConfiguration> SERVER =
      new DropwizardTestSupport<>(WorkerServerApplication.class, ResourceHelpers.resourceFilePath("test-config.yml"));

  private static final HttpClient CLIENT = HttpClient.newHttpClient();

  @BeforeAll
  public static void oneTimeSetUp() throws Exception {
    SERVER.before();
  }

  @AfterAll
  public static void oneTimeTearDown() {
    SERVER.after();
  }

  @Test
  public void indexIsServed() throws Exception {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:" + SERVER.getLocalPort() + "/"))
        .GET()
        .build();
    HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    Assertions.assertEquals(200, response.statusCode(), response.body());
    Assertions.assertTrue(response.body().contains("name"), response.body());
  }

}
