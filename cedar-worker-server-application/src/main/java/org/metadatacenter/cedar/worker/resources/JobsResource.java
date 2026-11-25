package org.metadatacenter.cedar.worker.resources;

import com.codahale.metrics.annotation.Timed;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.metadatacenter.cedar.worker.InclusionSubgraphRegenerationManager;
import org.metadatacenter.cedar.worker.security.AdminCommand;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.exception.CedarException;
import org.metadatacenter.rest.context.CedarRequestContext;
import org.metadatacenter.util.http.CedarError;
import org.metadatacenter.util.http.CedarResponse;

import java.net.URI;

import static org.metadatacenter.constant.CedarPathParameters.PP_ID;

/** The jobs the worker runs, each an inclusion-subgraph regeneration. */
@Path("/jobs")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "Jobs")
@SecurityRequirement(name = "api_key")
public class JobsResource extends AbstractWorkerResource {

  private final InclusionSubgraphRegenerationManager jobManager;

  public JobsResource(CedarConfig cedarConfig, InclusionSubgraphRegenerationManager jobManager) {
    super(cedarConfig);
    this.jobManager = jobManager;
  }

  /** Where a caller polls one job, whether it started the job or was refused by it. */
  public static URI jobUri(String jobId) {
    return URI.create("/jobs/" + jobId);
  }

  @GET
  @Timed
  @Path("/{id}")
  @Operation(summary = "Get the status of one job",
      description = "Report where a regeneration job has got to. Jobs are held in memory, so an "
          + "identifier from before the last restart is no longer known. Restricted to administrators.")
  @ApiResponses({
      @ApiResponse(responseCode = "200", description = "The job and its current state",
          content = @Content(schema = @Schema(ref = "#/components/schemas/JobStatus"))),
      @ApiResponse(responseCode = "401", content = @Content(schema = @Schema(implementation = CedarError.class)), description = "Unauthorized"),
      @ApiResponse(responseCode = "403", content = @Content(schema = @Schema(implementation = CedarError.class)), description = "The caller is not an administrator"),
      @ApiResponse(responseCode = "404", description = "No job answers to this identifier",
          content = @Content(schema = @Schema(implementation = CedarError.class))),
      @ApiResponse(responseCode = "500", content = @Content(schema = @Schema(implementation = CedarError.class)), description = "Internal server error")
  })
  public Response getJob(
      @Parameter(description = "Job identifier, as returned when the job was started.", required = true)
      @PathParam(PP_ID) String id) throws CedarException {
    CedarRequestContext c = buildRequestContext();
    AdminCommand.REGENERATE_INCLUSION_SUBGRAPH.enforce(c);

    return jobManager.find(id)
        .map(job -> Response.ok(job.toStatus()).build())
        .orElseGet(() -> CedarResponse.notFound()
            .message("No job answers to " + id)
            .parameter("jobId", id)
            .build());
  }
}
