package org.metadatacenter.cedar.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.metadatacenter.cedar.worker.resources.CommandInclusionSubgraphResource;
import org.metadatacenter.server.search.util.RegenerateInclusionSubgraphTask;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a request for a regeneration is answered with. A started job is accepted with the job as the
 * body. A request made while a job is in flight is refused in the shared error body, which carries the
 * running job in objects.job, in the representation the accepted answer gives it.
 */
class RegenerationAnswerTest {

  private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

  @Test
  void aRequestDuringARegenerationIsAConflictCarryingTheRunningJob() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    InclusionSubgraphRegenerationManager manager = new InclusionSubgraphRegenerationManager(() -> {
      started.countDown();
      assertTrue(release.await(10, TimeUnit.SECONDS));
      return new RegenerateInclusionSubgraphTask.Outcome(0, List.of());
    });
    manager.start();
    try {
      InclusionSubgraphRegenerationManager.StartResult first = manager.submit();
      assertTrue(started.await(10, TimeUnit.SECONDS));
      URI statusUri = URI.create("/command/regenerate-inclusion-subgraph/" + first.job().getId());

      Response accepted = CommandInclusionSubgraphResource.answer(first);
      assertEquals(202, accepted.getStatus());
      assertSame(first.job(), accepted.getEntity());
      assertEquals(statusUri, accepted.getLocation());

      Response conflict = CommandInclusionSubgraphResource.answer(manager.submit());
      assertEquals(409, conflict.getStatus());
      assertEquals(statusUri, conflict.getLocation());
      assertEquals(MediaType.APPLICATION_JSON_TYPE, conflict.getMediaType());
      JsonNode body = MAPPER.valueToTree(conflict.getEntity());
      assertEquals(409, body.path("statusCode").asInt(), body.toString());
      assertEquals("CONFLICT", body.path("status").asText(), body.toString());
      assertTrue(body.path("errorKey").isNull(), body.toString());
      assertEquals("An inclusion subgraph regeneration is already queued or running, so no second one was "
          + "started.", body.path("message").asText(), body.toString());
      assertEquals(MAPPER.valueToTree(first.job()), body.at("/objects/job"), body.toString());
      assertEquals("RUNNING", body.at("/objects/job/status").asText(), body.toString());
    } finally {
      release.countDown();
      manager.stop();
    }
  }
}
