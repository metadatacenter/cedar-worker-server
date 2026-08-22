package org.metadatacenter.cedar.worker;

import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.ResourceHelpers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.config.environment.CedarEnvironmentVariableProvider;
import org.metadatacenter.model.SystemComponent;
import org.metadatacenter.util.test.EmbeddedCedarMySql;
import org.metadatacenter.util.test.EmbeddedCedarNeo4j;
import org.metadatacenter.util.test.PermissionMatrix;
import org.metadatacenter.util.test.TestAuthUtil;

import java.util.Map;

import static org.metadatacenter.util.test.PermissionMatrix.Actor.ANONYMOUS;
import static org.metadatacenter.util.test.PermissionMatrix.Actor.OTHER_USER;
import static org.metadatacenter.util.test.PermissionMatrix.Actor.OWNER;

/**
 * The authorization gate on the worker server's administrative command, asserted as a table so it
 * cannot quietly lose its gate — the worker counterpart to the resource server's matrix.
 *
 * <p>The single admin command, {@code regenerate-inclusion-subgraph}, now authorizes through the shared
 * {@code AdminCommand} policy rather than an inline check. It must refuse an anonymous caller with 401
 * and an authenticated non-admin with 403. Its permission, {@code INCLUSION_SUBGRAPH_RECREATE}, is
 * granted only by the {@code SEARCH_REINDEXER} role, which the seeded test users do not hold, so both
 * are genuinely unprivileged. ADMIN is deliberately never probed: it would pass the gate, and the point
 * is to assert the gate without ever running the subgraph regeneration.
 *
 * <p>The worker resolves the admin user and opens graph sessions at startup, so — like the worker smoke
 * test — an in-process MariaDB (log store) and Neo4j are seeded before the test support boots. The
 * seeded graph also resolves the two test users at request time, which is what makes the 403 assertions
 * meaningful. Ports are alternate so the test never collides with a running dev worker.
 */
public class AdminCommandAuthorizationMatrixTest {

  static {
    EmbeddedCedarMySql.startAndRedirectEnvironment("CEDAR_LOG_MYSQL", Map.of(
        "CEDAR_WORKER_HTTP_PORT", "19075",
        "CEDAR_WORKER_ADMIN_PORT", "19175",
        "CEDAR_WORKER_STOP_PORT", "19275",
        "CEDAR_REDIS_PERSISTENT_PORT", "1"));
    EmbeddedCedarNeo4j.startRedirectAndSeed(SystemComponent.SERVER_WORKER);
  }

  public static final DropwizardTestSupport<WorkerServerConfiguration> SERVER =
      new DropwizardTestSupport<>(WorkerServerApplication.class, ResourceHelpers.resourceFilePath("test-config.yml"));

  private static Map<PermissionMatrix.Actor, String> actors;

  @BeforeAll
  public static void oneTimeSetUp() throws Exception {
    SERVER.before();
    CedarConfig cedarConfig = CedarConfig.getInstance(CedarEnvironmentVariableProvider.getFor(SystemComponent.SERVER_WORKER));
    // OWNER and OTHER_USER are simply the two seeded non-admin users; there is no resource owner for an
    // admin command. Both must be refused. ANONYMOUS carries no Authorization header by contract.
    actors = Map.of(
        OWNER, TestAuthUtil.getTestUser1AuthHeader(cedarConfig),
        OTHER_USER, TestAuthUtil.getTestUser2AuthHeader(cedarConfig));
  }

  @AfterAll
  public static void oneTimeTearDown() {
    SERVER.after();
  }

  @Test
  public void inclusionSubgraphCommandRefusesNonAdmins() {
    PermissionMatrix matrix = new PermissionMatrix("http://localhost:" + SERVER.getLocalPort(), actors);
    matrix.when("POST", "/command/regenerate-inclusion-subgraph")
        .expect(ANONYMOUS, 401)
        .expect(OWNER, 403)
        .expect(OTHER_USER, 403);
    matrix.verify();
  }

}
