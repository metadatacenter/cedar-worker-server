package org.metadatacenter.cedar.worker.resources;

import com.codahale.metrics.annotation.Timed;
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
  public Response regenerationStatus(@PathParam("jobId") String jobId) throws CedarException {
    CedarRequestContext c = buildRequestContext();
    AdminCommand.REGENERATE_INCLUSION_SUBGRAPH.enforce(c);

    return jobManager.find(jobId)
        .map(job -> Response.ok(job).build())
        .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build());
  }
}
