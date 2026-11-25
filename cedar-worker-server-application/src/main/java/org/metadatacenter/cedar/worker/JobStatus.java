package org.metadatacenter.cedar.worker;

import java.util.List;

/**
 * One job as {@code GET /jobs/{id}} reports it, in the shape every CEDAR server that runs jobs uses.
 *
 * <p>The worker runs one kind of job, an inclusion-subgraph regeneration. Its jobs carry no claim with
 * a deadline, so {@code deadlineAt} is always empty and {@code overdue} always false.
 * {@code submittedAt} and {@code unreadableArtifacts} belong to this kind alone.
 */
public record JobStatus(String jobId, String kind, String command, State state, String startedAt,
                        String finishedAt, String deadlineAt, boolean overdue, String failure,
                        String submittedAt, List<String> unreadableArtifacts) {

  public static final String INCLUSION_SUBGRAPH = "inclusion-subgraph";

  public enum State {
    QUEUED, RUNNING, COMPLETE, FAILED, ABANDONED, INTERRUPTED
  }

  static JobStatus of(InclusionSubgraphRegenerationManager.Job job) {
    InclusionSubgraphRegenerationManager.Status status = job.getStatus();
    State state = status == InclusionSubgraphRegenerationManager.Status.SUCCEEDED
        ? State.COMPLETE : State.valueOf(status.name());
    return new JobStatus(job.getId(), INCLUSION_SUBGRAPH, "regenerate-inclusion-subgraph", state,
        job.getStartedAt() == null ? null : job.getStartedAt().toString(),
        job.getCompletedAt() == null ? null : job.getCompletedAt().toString(),
        null, false, job.getError(), job.getSubmittedAt().toString(), job.getUnreadableArtifacts());
  }
}
