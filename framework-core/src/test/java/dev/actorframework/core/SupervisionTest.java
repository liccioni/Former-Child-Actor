package dev.actorframework.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * TASK-402: configurable supervision strategies and actor hierarchies, superseding the M1 fixed
 * default (TASK-107a).
 */
class SupervisionTest {

  @Test
  void aChildSpawnedFromWithinAnActorGetsAHierarchicalIdAndAParentReference() throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<ActorRef<String>> childRefs = new LinkedBlockingQueue<>();
      BlockingQueue<ActorRef<?>> observedParents = new LinkedBlockingQueue<>();

      ActorRef<Object> parent =
          system.spawn(
              () ->
                  new Actor<Object>() {
                    @Override
                    public void preStart(ActorContext<Object> context) {
                      ActorRef<String> child =
                          context.spawn(
                              () ->
                                  (childContext, message) ->
                                      childContext.parent().ifPresent(observedParents::add),
                              "child");
                      childRefs.add(child);
                    }

                    @Override
                    public void onMessage(ActorContext<Object> context, Object message) {}
                  },
              "parent");

      ActorRef<String> child = poll(childRefs);
      assertEquals("parent/child", child.id());

      child.tell("trigger");
      ActorRef<?> observedParent = poll(observedParents);
      assertEquals(parent.id(), observedParent.id());
    }
  }

  @Test
  void spawningTwoChildrenWithTheSameNameUnderTheSameParentFails() throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<Object> results = new LinkedBlockingQueue<>();

      system.spawn(
          () ->
              new Actor<Object>() {
                @Override
                public void preStart(ActorContext<Object> context) {
                  context.spawn(() -> (c, m) -> {}, "dup");
                  try {
                    context.spawn(() -> (c, m) -> {}, "dup");
                    results.add("no exception");
                  } catch (IllegalArgumentException e) {
                    results.add(e);
                  }
                }

                @Override
                public void onMessage(ActorContext<Object> context, Object message) {}
              },
          "parent");

      Object result = poll(results);
      assertTrue(
          result instanceof IllegalArgumentException, "expected an IllegalArgumentException");
    }
  }

  @Test
  void aFailingChildWithNoSupervisorOverrideIsStoppedByDefaultAndItsParentIsUnaffected()
      throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<ActorRef<String>> childRefs = new LinkedBlockingQueue<>();

      ActorRef<Object> parent =
          system.spawn(
              () ->
                  new Actor<Object>() {
                    @Override
                    public void preStart(ActorContext<Object> context) {
                      ActorRef<String> child =
                          context.spawn(
                              () ->
                                  (c, m) -> {
                                    throw new RuntimeException("boom");
                                  },
                              "flaky");
                      childRefs.add(child);
                    }

                    @Override
                    public void onMessage(ActorContext<Object> context, Object message) {}
                  },
              "parent");

      ActorRef<String> child = poll(childRefs);
      child.tell("trigger");

      awaitTerminated(child, Duration.ofSeconds(2));
      assertTrue(!parent.isTerminated());
    }
  }

  @Test
  void resumeDiscardsThePoisonMessageExactlyOnceAndKeepsTheChildRunning() throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<ActorRef<String>> childRefs = new LinkedBlockingQueue<>();
      BlockingQueue<String> processed = new LinkedBlockingQueue<>();
      AtomicInteger poisonSeen = new AtomicInteger();

      system.spawn(
          () ->
              new Actor<Object>() {
                @Override
                public void preStart(ActorContext<Object> context) {
                  ActorRef<String> child =
                      context.spawn(
                          () ->
                              (c, m) -> {
                                if ("poison".equals(m)) {
                                  poisonSeen.incrementAndGet();
                                  throw new RuntimeException("boom");
                                }
                                processed.add(m);
                              },
                          "child");
                  childRefs.add(child);
                }

                @Override
                public void onMessage(ActorContext<Object> context, Object message) {}

                @Override
                public SupervisorDirective supervisorStrategy(Throwable failure) {
                  return SupervisorDirective.RESUME;
                }
              },
          "parent");

      ActorRef<String> child = poll(childRefs);
      child.tell("poison");
      child.tell("still-going");

      assertEquals("still-going", processed.poll(2, TimeUnit.SECONDS));
      assertEquals(1, poisonSeen.get());
      assertTrue(!child.isTerminated());
    }
  }

  @Test
  void restartReplacesTheActorInstanceAndNeverRedeliversThePoisonMessage() throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<ActorRef<String>> childRefs = new LinkedBlockingQueue<>();
      AtomicInteger preStartCalls = new AtomicInteger();
      AtomicInteger poisonSeen = new AtomicInteger();
      BlockingQueue<String> processed = new LinkedBlockingQueue<>();

      class RestartableActor implements Actor<String> {
        int localState = 0;

        @Override
        public void preStart(ActorContext<String> context) {
          preStartCalls.incrementAndGet();
        }

        @Override
        public void onMessage(ActorContext<String> context, String message) {
          if ("poison".equals(message)) {
            poisonSeen.incrementAndGet();
            throw new RuntimeException("boom");
          }
          localState++;
          processed.add(message + ":" + localState);
        }
      }

      system.spawn(
          () ->
              new Actor<Object>() {
                @Override
                public void preStart(ActorContext<Object> context) {
                  ActorRef<String> child = context.spawn(RestartableActor::new, "child");
                  childRefs.add(child);
                }

                @Override
                public void onMessage(ActorContext<Object> context, Object message) {}

                @Override
                public SupervisorDirective supervisorStrategy(Throwable failure) {
                  return SupervisorDirective.RESTART;
                }
              },
          "parent");

      ActorRef<String> child = poll(childRefs);
      child.tell("a");
      child.tell("poison");
      child.tell("b");

      assertEquals("a:1", processed.poll(2, TimeUnit.SECONDS));
      // localState resets to 0 in the fresh instance, so "b" is again its first message: ":1".
      assertEquals("b:1", processed.poll(2, TimeUnit.SECONDS));
      assertEquals(1, poisonSeen.get());
      assertEquals(2, preStartCalls.get());
      assertTrue(!child.isTerminated());
    }
  }

  @Test
  void explicitStopDirectiveStopsTheChildJustLikeTheDefault() throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<ActorRef<String>> childRefs = new LinkedBlockingQueue<>();

      system.spawn(
          () ->
              new Actor<Object>() {
                @Override
                public void preStart(ActorContext<Object> context) {
                  ActorRef<String> child =
                      context.spawn(
                          () ->
                              (c, m) -> {
                                throw new RuntimeException("boom");
                              },
                          "child");
                  childRefs.add(child);
                }

                @Override
                public void onMessage(ActorContext<Object> context, Object message) {}

                @Override
                public SupervisorDirective supervisorStrategy(Throwable failure) {
                  return SupervisorDirective.STOP;
                }
              },
          "parent");

      ActorRef<String> child = poll(childRefs);
      child.tell("trigger");

      awaitTerminated(child, Duration.ofSeconds(2));
    }
  }

  @Test
  void escalateStopsTheOriginalChildAndAppliesTheGrandparentsDirectiveToTheParent()
      throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<ActorRef<String>> child1Refs = new LinkedBlockingQueue<>();
      BlockingQueue<ActorRef<String>> child2Refs = new LinkedBlockingQueue<>();
      AtomicInteger parentPreStartCalls = new AtomicInteger();

      class EscalatingParent implements Actor<Object> {
        @Override
        public void preStart(ActorContext<Object> context) {
          parentPreStartCalls.incrementAndGet();
          ActorRef<String> child1 =
              context.spawn(
                  () ->
                      (c, m) -> {
                        throw new RuntimeException("boom");
                      },
                  "child1");
          ActorRef<String> child2 = context.spawn(() -> (c, m) -> {}, "child2");
          child1Refs.add(child1);
          child2Refs.add(child2);
        }

        @Override
        public void onMessage(ActorContext<Object> context, Object message) {}

        @Override
        public SupervisorDirective supervisorStrategy(Throwable failure) {
          return SupervisorDirective.ESCALATE;
        }
      }

      system.spawn(
          () ->
              new Actor<Object>() {
                @Override
                public void preStart(ActorContext<Object> context) {
                  context.spawn(EscalatingParent::new, "parent");
                }

                @Override
                public void onMessage(ActorContext<Object> context, Object message) {}

                @Override
                public SupervisorDirective supervisorStrategy(Throwable failure) {
                  return SupervisorDirective.RESTART;
                }
              },
          "grandparent");

      ActorRef<String> child1 = poll(child1Refs);
      ActorRef<String> child2 = poll(child2Refs);

      child1.tell("trigger");

      // Escalation always stops the originally-failing child...
      awaitTerminated(child1, Duration.ofSeconds(2));
      // ...and the grandparent's RESTART for the parent cascades to the parent's other children.
      awaitTerminated(child2, Duration.ofSeconds(2));
      // At least one restart happened (the parent's own preStart may run again more than once if
      // a fresh child of the restarted instance transiently collides with a not-yet-deregistered
      // pre-restart child of the same name — see ADR-008's noted limitation).
      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (parentPreStartCalls.get() < 2) {
              Thread.sleep(5);
            }
          });
    }
  }

  @Test
  void stoppingAParentStopsItsChildAndDoesNotTerminateUntilTheChildDoes() throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<ActorRef<String>> childRefs = new LinkedBlockingQueue<>();
      CountDownLatch childStarted = new CountDownLatch(1);
      CountDownLatch releaseChild = new CountDownLatch(1);
      BlockingQueue<String> observations = new LinkedBlockingQueue<>();

      ActorRef<Object> parent =
          system.spawn(
              () ->
                  new Actor<Object>() {
                    @Override
                    public void preStart(ActorContext<Object> context) {
                      ActorRef<String> child =
                          context.spawn(
                              () ->
                                  new Actor<String>() {
                                    @Override
                                    public void onMessage(
                                        ActorContext<String> context, String message) {
                                      childStarted.countDown();
                                      awaitUninterruptibly(releaseChild);
                                    }

                                    @Override
                                    public void postStop(ActorContext<String> context) {
                                      observations.add("child-stopped");
                                    }
                                  },
                              "child");
                      childRefs.add(child);
                    }

                    @Override
                    public void onMessage(ActorContext<Object> context, Object message) {}

                    @Override
                    public void postStop(ActorContext<Object> context) {
                      observations.add("parent-stopped");
                    }
                  },
              "parent");

      ActorRef<String> child = poll(childRefs);
      child.tell("block-me");
      assertTimeoutPreemptively(Duration.ofSeconds(2), () -> childStarted.await());

      system.stop(parent);
      // The parent cannot finish terminating while its child is still blocked processing.
      assertTrue(!parent.isTerminated());

      releaseChild.countDown();

      assertEquals("child-stopped", observations.poll(2, TimeUnit.SECONDS));
      assertEquals("parent-stopped", observations.poll(2, TimeUnit.SECONDS));
      awaitTerminated(parent, Duration.ofSeconds(2));
      assertTrue(child.isTerminated());
    }
  }

  @Test
  void concurrentChildFailuresAreResolvedOneAtATimeOnTheParentsOwnThread() throws Exception {
    int childCount = 200;
    try (ActorSystem system = ActorSystem.start("test")) {
      CountDownLatch allResolved = new CountDownLatch(childCount);
      AtomicInteger inFlight = new AtomicInteger(0);
      AtomicBoolean concurrentInvocationDetected = new AtomicBoolean(false);

      system.spawn(
          () ->
              new Actor<Object>() {
                @Override
                public void preStart(ActorContext<Object> context) {
                  for (int i = 0; i < childCount; i++) {
                    context.spawn(
                        () ->
                            new Actor<String>() {
                              @Override
                              public void preStart(ActorContext<String> childContext) {
                                throw new RuntimeException("boom");
                              }

                              @Override
                              public void onMessage(ActorContext<String> c, String m) {}
                            });
                  }
                }

                @Override
                public void onMessage(ActorContext<Object> context, Object message) {}

                @Override
                public SupervisorDirective supervisorStrategy(Throwable failure) {
                  // Real concurrent load hits this from up to `childCount` different child
                  // threads; it is safe only because ActorCell resolves one ChildFailure envelope
                  // at a time, on this actor's own dispatch thread (ADR-008 §3).
                  if (inFlight.incrementAndGet() > 1) {
                    concurrentInvocationDetected.set(true);
                  }
                  Thread.onSpinWait();
                  inFlight.decrementAndGet();
                  allResolved.countDown();
                  return SupervisorDirective.RESUME;
                }
              },
          "parent");

      assertTrue(allResolved.await(10, TimeUnit.SECONDS), "all child failures should be resolved");
      assertTrue(!concurrentInvocationDetected.get());
    }
  }

  private static <X> X poll(BlockingQueue<X> queue) throws InterruptedException {
    X value = queue.poll(2, TimeUnit.SECONDS);
    assertNotNull(value, "expected a value within the timeout");
    return value;
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

  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          latch.await();
          return;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
