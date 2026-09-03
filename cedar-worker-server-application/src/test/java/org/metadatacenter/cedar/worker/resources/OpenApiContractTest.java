package org.metadatacenter.cedar.worker.resources;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.metadatacenter.util.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class OpenApiContractTest {

  private static final String JOB = "#/components/schemas/InclusionSubgraphJob";

  @Test
  void jobSubmissionAndPollingPublishTheJobSchema() throws IOException {
    JsonNode spec = readSpec();
    assertEquals(JOB, responseSchema(spec, "/command/regenerate-inclusion-subgraph", "post", "202"));
    assertEquals(JOB, responseSchema(spec, "/command/regenerate-inclusion-subgraph", "post", "409"));
    assertEquals(JOB,
        responseSchema(spec, "/command/regenerate-inclusion-subgraph/{jobId}", "get", "200"));
  }

  private static String responseSchema(JsonNode spec, String path, String method, String status) {
    return spec.path("paths").path(path).path(method).path("responses").path(status)
        .path("content").path("application/json").path("schema").path("$ref").asText();
  }

  private static JsonNode readSpec() throws IOException {
    try (InputStream input = OpenApiContractTest.class.getResourceAsStream("/assets/swagger-api/swagger.json")) {
      assertNotNull(input, "generated OpenAPI document");
      return JsonMapper.MAPPER.readTree(input);
    }
  }
}
