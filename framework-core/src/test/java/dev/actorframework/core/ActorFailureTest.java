package dev.actorframework.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * TASK-107a: a throwing actor is logged and stopped, and nothing else in the system is affected.
 */
class ActorFailureTest {

  @Test
  void anUncaughtExceptionStopsOnlyThatActor() {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<String> flaky =
          system.spawn(
              () ->
                  (context, message) -> {
                    throw new RuntimeException("boom");
                  });

      flaky.tell("trigger");

      awaitTerminated(flaky, Duration.ofSeconds(2));
    }
  }

  @Test
  void aSiblingActorKeepsWorkingAfterAnUnrelatedActorFails() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<String> flaky =
          system.spawn(
              () ->
                  (context, message) -> {
                    throw new RuntimeException("boom");
                  });
      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      ActorRef<String> healthy = system.spawn(() -> (context, message) -> received.add(message));

      flaky.tell("trigger");
      awaitTerminated(flaky, Duration.ofSeconds(2));

      healthy.tell("still fine");

      assertEquals("still fine", received.poll(2, TimeUnit.SECONDS));
    }
  }

  @Test
  void postStopRunsEvenAfterAFailure() {
    try (ActorSystem system = ActorSystem.start("test")) {
      AtomicBoolean postStopRan = new AtomicBoolean(false);
      ActorRef<String> flaky =
          system.spawn(
              () ->
                  new Actor<String>() {
                    @Override
                    public void onMessage(ActorContext<String> context, String message) {
                      throw new RuntimeException("boom");
                    }

                    @Override
                    public void postStop(ActorContext<String> context) {
                      postStopRan.set(true);
                    }
                  });

      flaky.tell("trigger");

      awaitTerminated(flaky, Duration.ofSeconds(2));
      assertTrue(postStopRan.get());
    }
  }

  @Test
  void aFailureInPreStartStopsTheActorWithoutProcessingAnyMessage() {
    try (ActorSystem system = ActorSystem.start("test")) {
      AtomicBoolean onMessageCalled = new AtomicBoolean(false);
      ActorRef<String> brokenAtStartup =
          system.spawn(
              () ->
                  new Actor<String>() {
                    @Override
                    public void preStart(ActorContext<String> context) {
                      throw new IllegalStateException("cannot start");
                    }

                    @Override
                    public void onMessage(ActorContext<String> context, String message) {
                      onMessageCalled.set(true);
                    }
                  },
              "broken-at-startup");

      brokenAtStartup.tell("should never be processed");

      awaitTerminated(brokenAtStartup, Duration.ofSeconds(2));
      assertTrue(!onMessageCalled.get());
    }
  }

  private static void awaitTerminated(ActorRef<?> ref, Duration timeout) {
    assertTimeoutPreemptively(
        timeout,
        () -> {
          while (!ref.isTerminated()) {
            Thread.sleep(5);
          }
        });
  }
}
