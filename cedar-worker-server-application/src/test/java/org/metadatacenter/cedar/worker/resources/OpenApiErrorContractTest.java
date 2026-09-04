package org.metadatacenter.cedar.worker.resources;

import org.junit.jupiter.api.Test;
import org.metadatacenter.util.test.OpenApiErrorContract;

import java.io.IOException;
import java.io.InputStream;

class OpenApiErrorContractTest {

  @Test
  void errorResponsesPublishTheCommonSchema() throws IOException {
    try (InputStream input = getClass().getResourceAsStream("/assets/swagger-api/swagger.json")) {
      OpenApiErrorContract.assertDocumented(input,
          "POST /command/regenerate-inclusion-subgraph 409",
          "GET /command/regenerate-inclusion-subgraph/{jobId} 404");
    }
  }
}
