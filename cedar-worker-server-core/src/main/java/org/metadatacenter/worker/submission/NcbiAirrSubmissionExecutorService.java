package org.metadatacenter.worker.submission;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpResponse;
import org.metadatacenter.bridge.CedarDataServices;
import org.metadatacenter.config.CedarConfig;
import org.metadatacenter.exception.CedarProcessingException;
import org.metadatacenter.queue.NcbiAirrSubmissionQueueEvent;
import org.metadatacenter.rest.context.CedarRequestContext;
import org.metadatacenter.rest.context.CedarRequestContextFactory;
import org.metadatacenter.server.service.UserService;
import org.metadatacenter.submission.NcbiAirrSubmission;
import org.metadatacenter.util.http.ProxyUtil;
import org.metadatacenter.util.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NcbiAirrSubmissionExecutorService {

  private static final Logger log = LoggerFactory.getLogger(NcbiAirrSubmissionExecutorService.class);
  private final CedarConfig cedarConfig;
  private CedarRequestContext context;

  public NcbiAirrSubmissionExecutorService(CedarConfig cedarConfig) {
    this.cedarConfig = cedarConfig;
    UserService userService = CedarDataServices.getUserService();
    this.context = CedarRequestContextFactory.fromAdminUser(cedarConfig, userService);
  }

  // Main entry point
  public void handleEvent(NcbiAirrSubmissionQueueEvent event) {
    submit(event.getSubmission());
  }

  private void submit(NcbiAirrSubmission submission) {
    // TODO: read endpoint url from configuration
    String submissionUrl = cedarConfig.getServers().getSubmission().getBase() + "command/submit-to-ncbi";
    log.info("Submission URL: " + submissionUrl);
    ObjectNode submissionRequestBody = JsonMapper.MAPPER.valueToTree(submission);
    String submissionRequestBodyAsString = null;
    try {
      submissionRequestBodyAsString = JsonMapper.MAPPER.writeValueAsString(submissionRequestBody);
      HttpResponse submissionResponse = ProxyUtil.proxyPost(submissionUrl, context, submissionRequestBodyAsString);
      // TODO: check response and send notification to the user
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    } catch (CedarProcessingException e) {
      e.printStackTrace();
    }
  }

}
