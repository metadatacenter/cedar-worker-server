package org.metadatacenter.cedar.worker;

import com.codahale.metrics.health.HealthCheck;

public class WorkerDependencyHealthCheck extends HealthCheck {

  @FunctionalInterface
  public interface Probe {
    void verify() throws Exception;
  }

  private final String dependencyName;
  private final Probe probe;

  public WorkerDependencyHealthCheck(String dependencyName, Probe probe) {
    this.dependencyName = dependencyName;
    this.probe = probe;
  }

  @Override
  protected Result check() {
    try {
      probe.verify();
      return Result.healthy(dependencyName + " is reachable");
    } catch (Exception e) {
      return Result.unhealthy(e);
    }
  }
}
