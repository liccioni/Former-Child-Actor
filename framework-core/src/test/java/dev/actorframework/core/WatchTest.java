package dev.actorframework.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * M6: death watch ({@link ActorContext#watch}/{@link ActorContext#unwatch}). See {@code
 * docs/decisions/ADR-016-death-watch.md} for the full semantics this exercises: exactly-once
 * delivery (including races between a late watch and the target's own concurrent termination),
 * {@code Restart} never firing a watch, cascaded/escalated stops still firing it, and the
 * actor-id-reuse hazard {@code ActorSystem.watch}/{@code unwatch} guard against by ref identity.
 */
class WatchTest {

  @Test
  void watchingALiveActorThatLaterStopsDeliversTheMessageExactlyOnce() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<String> target =
          system.spawn(
              () ->
                  (context, message) -> {
                    throw new RuntimeException("boom");
                  });
      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      spawnWatcher(system, target, "target-died", received);

      target.tell("trigger");

      assertEquals("target-died", received.poll(2, TimeUnit.SECONDS));
      assertNull(received.poll(300, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  void watchingAnAlreadyTerminatedActorDeliversPromptly() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<String> target = system.spawn(() -> (context, message) -> {});
      system.stop(target);
      awaitTerminated(target, Duration.ofSeconds(2));

      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      // Bounded well under a timeout that would also cover "eventually" delivery, to prove this
      // fires immediately rather than by some other, slower path.
      assertTimeoutPreemptively(
          Duration.ofSeconds(1),
          () -> {
            spawnWatcher(system, target, "already-gone", received);
            assertEquals("already-gone", received.take());
          });
    }
  }

  @Test
  void unwatchBeforeTerminationPreventsTheNotification() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<String> target = system.spawn(() -> (context, message) -> {});
      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      CountDownLatch unwatched = new CountDownLatch(1);

      ActorRef<String> watcher =
          system.spawn(
              () ->
                  new Actor<String>() {
                    @Override
                    public void preStart(ActorContext<String> context) {
                      context.watch(target, "target-died");
                    }

                    @Override
                    public void onMessage(ActorContext<String> context, String message) {
                      if ("stop-watching".equals(message)) {
                        context.unwatch(target);
                        unwatched.countDown();
                      } else {
                        received.add(message);
                      }
                    }
                  });

      watcher.tell("stop-watching");
      assertTrue(unwatched.await(2, TimeUnit.SECONDS));

      system.stop(target);
      awaitTerminated(target, Duration.ofSeconds(2));

      assertNull(received.poll(300, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  void restartingAWatchedActorDoesNotFireTheWatch() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<ActorRef<String>> childBox = new LinkedBlockingQueue<>();
      BlockingQueue<String> childReceived = new LinkedBlockingQueue<>();
      system.spawn(
          () ->
              new Actor<Object>() {
                @Override
                public void preStart(ActorContext<Object> context) {
                  childBox.add(
                      context.spawnChild(
                          () ->
                              (childContext, message) -> {
                                if ("boom".equals(message)) {
                                  throw new RuntimeException("boom");
                                }
                                childReceived.add(message);
                              },
                          "child",
                          SupervisorStrategy.restart()));
                }

                @Override
                public void onMessage(ActorContext<Object> context, Object message) {}
              });

      ActorRef<String> child = childBox.poll(2, TimeUnit.SECONDS);
      assertNotNull(child);

      BlockingQueue<String> watcherReceived = new LinkedBlockingQueue<>();
      spawnWatcher(system, child, "child-died", watcherReceived);

      child.tell("boom");
      child.tell("still alive");

      assertEquals("still alive", childReceived.poll(2, TimeUnit.SECONDS));
      assertFalse(child.isTerminated());
      assertNull(watcherReceived.poll(300, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  void aChildStoppedByASupervisionCascadeStillFiresPendingWatches() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<ActorRef<Object>> parentBox = new LinkedBlockingQueue<>();
      BlockingQueue<ActorRef<String>> childBox = new LinkedBlockingQueue<>();

      system.spawn(
          () ->
              new Actor<Object>() {
                @Override
                public void preStart(ActorContext<Object> context) {
                  parentBox.add(
                      context.spawnChild(
                          () ->
                              new Actor<Object>() {
                                @Override
                                public void preStart(ActorContext<Object> parentContext) {
                                  childBox.add(
                                      parentContext.spawnChild(
                                          () ->
                                              (childContext, message) -> {
                                                throw new RuntimeException("boom");
                                              },
                                          "child",
                                          SupervisorStrategy.escalate()));
                                }

                                @Override
                                public void onMessage(ActorContext<Object> c, Object m) {}
                              },
                          "parent",
                          SupervisorStrategy.stop()));
                }

                @Override
                public void onMessage(ActorContext<Object> context, Object message) {}
              });

      ActorRef<Object> parent = parentBox.poll(2, TimeUnit.SECONDS);
      ActorRef<String> child = childBox.poll(2, TimeUnit.SECONDS);
      assertNotNull(parent);
      assertNotNull(child);

      BlockingQueue<String> watcherReceived = new LinkedBlockingQueue<>();
      spawnWatcher(system, parent, "parent-died", watcherReceived);

      child.tell("trigger");

      assertEquals("parent-died", watcherReceived.poll(2, TimeUnit.SECONDS));
    }
  }

  @Test
  void multipleWatchersOfTheSameTargetAreAllNotified() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<String> target = system.spawn(() -> (context, message) -> {});
      BlockingQueue<String> received1 = new LinkedBlockingQueue<>();
      BlockingQueue<String> received2 = new LinkedBlockingQueue<>();
      BlockingQueue<String> received3 = new LinkedBlockingQueue<>();
      spawnWatcher(system, target, "died-1", received1);
      spawnWatcher(system, target, "died-2", received2);
      spawnWatcher(system, target, "died-3", received3);

      system.stop(target);

      assertEquals("died-1", received1.poll(2, TimeUnit.SECONDS));
      assertEquals("died-2", received2.poll(2, TimeUnit.SECONDS));
      assertEquals("died-3", received3.poll(2, TimeUnit.SECONDS));
    }
  }

  @Test
  void reWatchingTheSameTargetReplacesThePendingMessage() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<String> target = system.spawn(() -> (context, message) -> {});
      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      CountDownLatch rewatched = new CountDownLatch(1);

      ActorRef<String> watcher =
          system.spawn(
              () ->
                  new Actor<String>() {
                    @Override
                    public void preStart(ActorContext<String> context) {
                      context.watch(target, "first");
                    }

                    @Override
                    public void onMessage(ActorContext<String> context, String message) {
                      if ("rewatch".equals(message)) {
                        context.watch(target, "second");
                        rewatched.countDown();
                      } else {
                        received.add(message);
                      }
                    }
                  });

      watcher.tell("rewatch");
      assertTrue(rewatched.await(2, TimeUnit.SECONDS));

      system.stop(target);

      assertEquals("second", received.poll(2, TimeUnit.SECONDS));
      assertNull(received.poll(300, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  void watchingYourselfNeverDeliversButTheActorStillTerminatesCleanly() {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<String> selfWatcher =
          system.spawn(
              () ->
                  new Actor<String>() {
                    @Override
                    public void preStart(ActorContext<String> context) {
                      context.watch(context.self(), "self-died");
                    }

                    @Override
                    public void onMessage(ActorContext<String> context, String message) {
                      throw new RuntimeException("boom");
                    }
                  });

      selfWatcher.tell("trigger");

      awaitTerminated(selfWatcher, Duration.ofSeconds(2));
    }
  }

  @Test
  void watchingAnActorWhoseNameIsLaterReusedFiresForTheOriginalNotTheReplacement()
      throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<String> original = system.spawn(() -> (context, message) -> {}, "worker");
      system.stop(original);
      awaitTerminated(original, Duration.ofSeconds(2));

      ActorRef<String> replacement = system.spawn(() -> (context, message) -> {}, "worker");
      assertFalse(replacement.isTerminated());

      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      spawnWatcher(system, original, "original-died", received);

      assertEquals("original-died", received.poll(1, TimeUnit.SECONDS));
      assertFalse(replacement.isTerminated());
    }
  }

  @Test
  void manyConcurrentWatchesRacingTheTargetsOwnTerminationAllDeliverExactlyOnce()
      throws InterruptedException {
    int watcherCount = 300;
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<String> target = system.spawn(() -> (context, message) -> {});
      List<BlockingQueue<Integer>> receivedPerWatcher = new ArrayList<>();
      for (int i = 0; i < watcherCount; i++) {
        receivedPerWatcher.add(new LinkedBlockingQueue<>());
      }

      ExecutorService pool = Executors.newFixedThreadPool(16);
      CountDownLatch allSubmitted = new CountDownLatch(watcherCount);
      try {
        for (int i = 0; i < watcherCount; i++) {
          int index = i;
          BlockingQueue<Integer> queue = receivedPerWatcher.get(i);
          pool.execute(
              () -> {
                system.spawn(
                    () ->
                        new Actor<Integer>() {
                          @Override
                          public void preStart(ActorContext<Integer> context) {
                            context.watch(target, index);
                          }

                          @Override
                          public void onMessage(ActorContext<Integer> context, Integer message) {
                            queue.add(message);
                          }
                        });
                allSubmitted.countDown();
              });
        }
        // Submitted after the registrations, but the fixed pool has far fewer threads than
        // pending tasks, so this genuinely races a good number of the registrations above rather
        // than always running strictly last.
        pool.execute(() -> system.stop(target));

        assertTrue(allSubmitted.await(10, TimeUnit.SECONDS));
      } finally {
        pool.shutdown();
      }

      for (int i = 0; i < watcherCount; i++) {
        BlockingQueue<Integer> queue = receivedPerWatcher.get(i);
        Integer received = queue.poll(5, TimeUnit.SECONDS);
        assertEquals(i, received, "watcher " + i + " should receive its own index exactly once");
        assertNull(
            queue.poll(20, TimeUnit.MILLISECONDS),
            "watcher " + i + " should not be notified twice");
      }
    }
  }

  private static ActorRef<String> spawnWatcher(
      ActorSystem system,
      ActorRef<?> target,
      String onTerminatedMessage,
      BlockingQueue<String> received) {
    return system.spawn(
        () ->
            new Actor<String>() {
              @Override
              public void preStart(ActorContext<String> context) {
                context.watch(target, onTerminatedMessage);
              }

              @Override
              public void onMessage(ActorContext<String> context, String message) {
                received.add(message);
              }
            });
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
