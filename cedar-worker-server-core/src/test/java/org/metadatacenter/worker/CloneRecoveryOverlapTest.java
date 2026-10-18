package org.metadatacenter.worker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.metadatacenter.id.CedarTemplateId;
import org.metadatacenter.server.queue.util.CloneInstancesQueueService;
import org.metadatacenter.server.queue.util.EmbeddedRedis;
import org.metadatacenter.server.queue.util.QueueTestConfig;
import org.metadatacenter.server.resource.CloneInstancesExecutorService;
import org.metadatacenter.server.resource.CloneInstancesQueueEvent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Two actual processors and real Redis, with a delayed old claimant. */
@Timeout(30)
@Tag("integration")
class CloneRecoveryOverlapTest {
  @Test void aReplacementCompletingARecoveredClaimMustFenceTheOldConsumer() throws Exception {
    try (var redis = EmbeddedRedis.start()) {
      var config = QueueTestConfig.onPort(redis.port());
      var firstQueue = spy(new CloneInstancesQueueService(config));
      var secondQueue = spy(new CloneInstancesQueueService(config));
      var firstClaimed = new CountDownLatch(1);
      var resumeFirst = new CountDownLatch(1);
      var secondCompleted = new CountDownLatch(1);
      var firstFinished = new CountDownLatch(1);
      doAnswer(call -> {
        firstClaimed.countDown();
        assertTrue(resumeFirst.await(10, TimeUnit.SECONDS));
        return call.callRealMethod();
      }).when(firstQueue).beginExecution(anyString());
      doAnswer(call -> {
        Object result = call.callRealMethod(); secondCompleted.countDown(); return result;
      }).when(secondQueue).acknowledge(anyString());
      doAnswer(call -> {
        Object result = call.callRealMethod(); firstFinished.countDown(); return result;
      }).when(firstQueue).acknowledge(anyString());
      doAnswer(call -> {
        Object result = call.callRealMethod(); firstFinished.countDown(); return result;
      }).when(firstQueue).deadLetter(anyString());
      var executions = new AtomicInteger();
      var executor = mock(CloneInstancesExecutorService.class);
      doAnswer(call -> { executions.incrementAndGet(); return null; }).when(executor).handleEvent(any());
      var first = new CloneInstancesQueueProcessor(firstQueue, executor);
      var replacement = new CloneInstancesQueueProcessor(secondQueue, executor);
      firstQueue.enqueueEvent(new CloneInstancesQueueEvent(
          CedarTemplateId.build("https://repo.metadatacenter.orgx/templates/overlap-source"),
          CedarTemplateId.build("https://repo.metadatacenter.orgx/templates/overlap-destination"), "overlap regression"));
      first.start();
      try {
        assertTrue(firstClaimed.await(10, TimeUnit.SECONDS));
        replacement.start();
        assertTrue(secondCompleted.await(10, TimeUnit.SECONDS));
        assertEquals(1, executions.get());
        resumeFirst.countDown();
        assertTrue(firstFinished.await(10, TimeUnit.SECONDS));
        assertEquals(1, executions.get(),
            "resuming the superseded claimant must not repeat the non-idempotent clone handler");
      } finally {
        resumeFirst.countDown();
        first.stop();
        replacement.stop();
      }
    }
  }
}
