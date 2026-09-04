package org.metadatacenter.cedar.worker.resources;

import com.codahale.metrics.annotation.Timed;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.metadatacenter.util.http.CedarError;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.exception.CedarException;
import org.metadatacenter.cedar.worker.InclusionSubgraphRegenerationManager;
import org.metadatacenter.cedar.worker.security.AdminCommand;
import org.metadatacenter.rest.context.CedarRequestContext;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;

@Path("/command")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "Inclusion subgraph")
@SecurityRequirement(name = "api_key")
public class CommandInclusionSubgraphResource extends AbstractWorkerResource {

  private final InclusionSubgraphRegenerationManager jobManager;

  public CommandInclusionSubgraphResource(CedarConfig cedarConfig,
                                          InclusionSubgraphRegenerationManager jobManager) {
    super(cedarConfig);
    this.jobManager = jobManager;
  }

  @POST
  @Timed
  @Path("/regenerate-inclusion-subgraph")
  @Operation(summary = "Start regenerating the inclusion subgraph",
      description = "Ask for the graph of which artifacts include which to be rebuilt, and return "
          + "the job that will do it. The work is asynchronous, so this answers immediately with a "
          + "job to poll rather than waiting for the rebuild. Only one job runs at a time: asking "
          + "again while one is in flight returns that job rather than starting a second. Restricted "
          + "to administrators.")
  @ApiResponses({
      @ApiResponse(responseCode = "202", description = "A job was started",
          content = @Content(schema = @Schema(ref = "#/components/schemas/InclusionSubgraphJob")),
          headers = @Header(name = "Location", description = "Where to poll this job's status.",
              schema = @Schema(type = "string"))),
      @ApiResponse(responseCode = "409",
          description = "A regeneration is already running; the job returned is that one, not a new one",
          content = @Content(schema = @Schema(ref = "#/components/schemas/InclusionSubgraphJob")),
          headers = @Header(name = "Location", description = "Where to poll the running job's status.",
              schema = @Schema(type = "string"))),
      @ApiResponse(responseCode = "401", content = @Content(schema = @Schema(implementation = CedarError.class)), description = "Unauthorized"),
      @ApiResponse(responseCode = "403", content = @Content(schema = @Schema(implementation = CedarError.class)), description = "The caller is not an administrator"),
      @ApiResponse(responseCode = "500", content = @Content(schema = @Schema(implementation = CedarError.class)), description = "Internal server error")
  })
  public Response regenerateInclusionSubgraph() throws CedarException {
    CedarRequestContext c = buildRequestContext();
    AdminCommand.REGENERATE_INCLUSION_SUBGRAPH.enforce(c);

    InclusionSubgraphRegenerationManager.StartResult result = jobManager.submit();
    URI statusUri = URI.create("/command/regenerate-inclusion-subgraph/" + result.job().getId());
    return result.accepted()
        ? Response.accepted(result.job()).location(statusUri).build()
        : Response.status(Response.Status.CONFLICT).entity(result.job()).location(statusUri).build();
  }

  @GET
  @Timed
  @Path("/regenerate-inclusion-subgraph/{jobId}")
  @Operation(summary = "Get the status of a regeneration job",
      description = "Report where a regeneration job has got to. Jobs are held in memory, so an "
          + "identifier from before the last restart is no longer known. Restricted to administrators.")
  @ApiResponses({
      @ApiResponse(responseCode = "200", description = "The job and its current state",
          content = @Content(schema = @Schema(ref = "#/components/schemas/InclusionSubgraphJob"))),
      @ApiResponse(responseCode = "401", content = @Content(schema = @Schema(implementation = CedarError.class)), description = "Unauthorized"),
      @ApiResponse(responseCode = "403", content = @Content(schema = @Schema(implementation = CedarError.class)), description = "The caller is not an administrator"),
      @ApiResponse(responseCode = "404", description = "No job answers to this identifier; the response has no body"),
      @ApiResponse(responseCode = "500", content = @Content(schema = @Schema(implementation = CedarError.class)), description = "Internal server error")
  })
  public Response regenerationStatus(
      @Parameter(description = "Job identifier, as returned when the job was started.", required = true)
      @PathParam("jobId") String jobId) throws CedarException {
    CedarRequestContext c = buildRequestContext();
    AdminCommand.REGENERATE_INCLUSION_SUBGRAPH.enforce(c);

    return jobManager.find(jobId)
        .map(job -> Response.ok(job).build())
        .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build());
  }
}
