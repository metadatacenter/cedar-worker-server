package org.metadatacenter.cedar.worker;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.metadatacenter.model.SystemComponent;
import org.metadatacenter.util.test.EmbeddedCedarNeo4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Boots the real application through the Dropwizard test rule. The worker server resolves the
 * admin user and opens graph sessions at startup, so an in-process Neo4j is seeded before the
 * rule runs; MySQL and Redis are still expected, as in the development environment. Beyond
 * configuration rot, this catches startup regressions in the permission and queue executor
 * wiring - the kind that previously surfaced only as a NullPointerException at boot.
 */
public class WorkerServerApplicationSmokeTest {

  static {
    // Must run before the application rule: startup itself needs the graph and the admin user
    EmbeddedCedarNeo4j.startRedirectAndSeed(SystemComponent.SERVER_WORKER);
  }

  @ClassRule
  public static final DropwizardAppRule<WorkerServerConfiguration> SERVER =
      new DropwizardAppRule<>(WorkerServerApplication.class, ResourceHelpers.resourceFilePath("test-config.yml"));

  private static final HttpClient CLIENT = HttpClient.newHttpClient();

  @Test
  public void indexIsServed() throws Exception {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:" + SERVER.getLocalPort() + "/"))
        .GET()
        .build();
    HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    Assert.assertEquals(200, response.statusCode());
    Assert.assertTrue(response.body().contains("name"));
  }

}
