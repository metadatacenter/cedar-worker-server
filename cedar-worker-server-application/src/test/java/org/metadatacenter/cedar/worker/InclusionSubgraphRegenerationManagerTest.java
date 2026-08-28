package org.metadatacenter.cedar.worker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InclusionSubgraphRegenerationManagerTest {

  private InclusionSubgraphRegenerationManager manager;

  @AfterEach
  void stopManager() throws Exception {
    if (manager != null) {
      manager.stop();
    }
  }

  @Test
  void rejectsAnOverlappingRegenerationAndExposesTheRunningJob() throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    manager = new InclusionSubgraphRegenerationManager(() -> {
      started.countDown();
      assertTrue(release.await(10, TimeUnit.SECONDS));
    });
    manager.start();

    InclusionSubgraphRegenerationManager.StartResult first = manager.submit();
    assertTrue(started.await(10, TimeUnit.SECONDS));
    InclusionSubgraphRegenerationManager.StartResult overlapping = manager.submit();

    assertTrue(first.accepted());
    assertFalse(overlapping.accepted());
    assertEquals(first.job().getId(), overlapping.job().getId());
    assertEquals(InclusionSubgraphRegenerationManager.Status.RUNNING, overlapping.job().getStatus());

    release.countDown();
    awaitStatus(first.job(), InclusionSubgraphRegenerationManager.Status.SUCCEEDED);
    assertNotNull(first.job().getCompletedAt());
  }

  @Test
  void recordsAJobFailureForTheStatusEndpoint() throws Exception {
    manager = new InclusionSubgraphRegenerationManager(() -> {
      throw new IllegalStateException("graph unavailable");
    });
    manager.start();

    InclusionSubgraphRegenerationManager.Job job = manager.submit().job();
    awaitStatus(job, InclusionSubgraphRegenerationManager.Status.FAILED);

    assertEquals("graph unavailable", job.getError());
    assertEquals(job, manager.find(job.getId()).orElseThrow());
  }

  private static void awaitStatus(InclusionSubgraphRegenerationManager.Job job,
                                  InclusionSubgraphRegenerationManager.Status expected)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (job.getStatus() != expected && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(expected, job.getStatus());
  }
}
