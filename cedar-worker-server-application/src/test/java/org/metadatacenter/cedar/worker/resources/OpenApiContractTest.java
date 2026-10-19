package org.metadatacenter.cedar.worker.resources;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.metadatacenter.util.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;

import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenApiContractTest {

  private static final String JOB = "#/components/schemas/InclusionSubgraphJob";
  private static final String CEDAR_ERROR = "#/components/schemas/CedarError";

  @Test
  void jobSubmissionAndPollingPublishTheJobSchema() throws IOException {
    JsonNode spec = readSpec();
    assertEquals(JOB, responseSchema(spec, "/command/regenerate-inclusion-subgraph", "post", "202"));
    assertEquals(JOB,
        responseSchema(spec, "/command/regenerate-inclusion-subgraph/{jobId}", "get", "200"));
  }

  /**
   * The operation that starts a regeneration documents every answer it gives, each refusal in the shared
   * error body. A request refused because a job is in flight still names that job, and the document says
   * where.
   */
  @Test
  void jobSubmissionDocumentsEveryAnswer() throws IOException {
    JsonNode responses = readSpec().at("/paths/~1command~1regenerate-inclusion-subgraph/post/responses");
    Set<String> documented = new TreeSet<>();
    responses.fieldNames().forEachRemaining(documented::add);
    assertEquals(new TreeSet<>(Set.of("202", "401", "403", "406", "409", "429", "500", "503")), documented);
    for (String status : documented) {
      if (status.equals("202")) continue;
      assertEquals(CEDAR_ERROR, responses.at("/" + status + "/content/application~1json/schema/$ref").asText(),
          status);
      assertFalse(responses.path(status).path("description").asText().isBlank(), status);
    }
    assertTrue(responses.at("/409/description").asText().contains("objects.job"), responses.at("/409").toString());
    assertTrue(responses.at("/409/headers/Location").isObject(), responses.at("/409").toString());
    assertTrue(responses.at("/202/headers/Location").isObject(), responses.at("/202").toString());
  }

  private static String responseSchema(JsonNode spec, String path, String method, String status) {
    return spec.path("paths").path(path).path(method).path("responses").path(status)
        .path("content").path("application/json").path("schema").path("$ref").asText();
  }

  private static JsonNode readSpec() throws IOException {
    try (InputStream input = OpenApiContractTest.class.getResourceAsStream("/assets/swagger-api/swagger.json")) {
      assertNotNull(input, "generated OpenAPI document");
      return JsonMapper.STRICT_MAPPER.readTree(input);
    }
  }
}
