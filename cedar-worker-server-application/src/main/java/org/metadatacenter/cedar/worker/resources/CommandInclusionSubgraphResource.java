package org.metadatacenter.cedar.worker.resources;

import com.codahale.metrics.annotation.Timed;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.exception.CedarException;
import org.metadatacenter.exception.CedarProcessingException;
import org.metadatacenter.rest.context.CedarRequestContext;
import org.metadatacenter.rest.context.CedarRequestContextFactory;
import org.metadatacenter.server.search.util.RegenerateInclusionSubgraphTask;
import org.metadatacenter.server.security.model.auth.CedarPermission;
import org.metadatacenter.server.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.metadatacenter.rest.assertion.GenericAssertions.LoggedIn;

@Path("/command")
@Produces(MediaType.APPLICATION_JSON)
public class CommandInclusionSubgraphResource extends AbstractWorkerResource {

  private static final Logger log = LoggerFactory.getLogger(CommandInclusionSubgraphResource.class);
  private static UserService userService;

  public CommandInclusionSubgraphResource(CedarConfig cedarConfig) {
    super(cedarConfig);
  }

  public static void injectUserService(UserService us) {
    userService = us;
  }

  @POST
  @Timed
  @Path("/regenerate-inclusion-subgraph")
  public Response regenerateRulesIndex() throws CedarException {
    CedarRequestContext c = buildRequestContext();
    c.must(c.user()).be(LoggedIn);
    c.must(c.user()).have(CedarPermission.INCLUSION_SUBGRAPH_RECREATE);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      RegenerateInclusionSubgraphTask task = new RegenerateInclusionSubgraphTask(cedarConfig);
      try {
        CedarRequestContext cedarAdminRequestContext = CedarRequestContextFactory.fromAdminUser(cedarConfig, userService);
        task.regenerateInclusionSubgraph(cedarAdminRequestContext);
      } catch (CedarProcessingException e) {
        //TODO: handle this, log it separately
        log.error("Error in inclusion subgraph regeneration executor", e);
      }
    });

    return Response.ok().build();
  }


}
