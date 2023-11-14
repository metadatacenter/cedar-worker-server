package org.metadatacenter.cedar.worker.health;

import com.codahale.metrics.health.HealthCheck;

public class WorkerServerHealthCheck extends HealthCheck {

  public WorkerServerHealthCheck() {
  }

  @Override
  protected Result check() {
    if (2 * 2 == 5) {
      return Result.unhealthy("Unhealthy, because 2 * 2 == 5");
    }
    return Result.healthy();
  }
}
