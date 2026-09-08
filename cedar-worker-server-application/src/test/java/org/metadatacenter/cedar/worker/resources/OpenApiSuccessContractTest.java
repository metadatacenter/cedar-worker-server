package org.metadatacenter.cedar.worker.resources;

import org.junit.jupiter.api.Test;
import org.metadatacenter.util.test.OpenApiSuccessContract;

import java.io.IOException;
import java.io.InputStream;

/**
 * Holds this service's committed OpenAPI document to the estate's rule that a success payload is
 * described rather than merely acknowledged.
 *
 * <p>Only three of the eleven spec-shipping services verified their committed document at all, and
 * none of them checked the success side. A 2xx with no schema, a dangling reference, an empty schema
 * and a JSON body typed as a bare string all leave a client generator with nothing to work from, and
 * each is cheap to reintroduce by adding a route and forgetting an annotation.</p>
 *
 * <p>The one write listed as bodiless takes no body: the command rebuilds the whole graph.</p>
 */
class OpenApiSuccessContractTest {

  @Test
  void everySuccessPayloadIsDescribed() throws IOException {
    try (InputStream input = getClass().getResourceAsStream("/assets/swagger-api/swagger.json")) {
      OpenApiSuccessContract.assertDescribed(input,
          // regenerateInclusionSubgraph() rebuilds the whole graph and takes no parameters.
          "POST /command/regenerate-inclusion-subgraph");
    }
  }
}
