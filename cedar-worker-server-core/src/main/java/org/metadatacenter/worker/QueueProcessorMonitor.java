package org.metadatacenter.worker;

import java.time.Instant;

public interface QueueProcessorMonitor {

  String getProcessorName();

  boolean isRunning();

  Instant getLastFailureAt();

  Instant getLastSuccessAt();
}
