package dev.actorframework.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * TASK-402: configurable supervision strategies (Resume, Restart, Stop, Escalate) and actor
 * hierarchies. A top-level actor's own behavior (TASK-107a's fixed default) is unchanged and
 * covered by {@link ActorFailureTest}/{@link ActorSystemTest}, which needed no edits for this task
 * — this file covers only actors spawned via {@link ActorContext#spawnChild}.
 */
class SupervisionTest {

  @Test
  void resumeKeepsTheActorAliveAndPreservesStateAcrossTheFailure() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<Integer> observed = new LinkedBlockingQueue<>();
      Spawned<String> spawned =
          spawnChildUnderTopLevelParent(
              system, () -> new CountingActor(observed), SupervisorStrategy.resume());

      spawned.child().tell("count");
      assertEquals(1, observed.poll(2, TimeUnit.SECONDS));

      spawned.child().tell("boom");
      spawned.child().tell("count");

      // A resumed actor keeps its existing instance, so the counter continues from where it left
      // off rather than resetting — the opposite of what restart proves below.
      assertEquals(2, observed.poll(2, TimeUnit.SECONDS));
      assertFalse(spawned.child().isTerminated());
    }
  }

  @Test
  void resumeContinuesProcessingLaterMessagesAfterTheFailure() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      Spawned<String> spawned =
          spawnChildUnderTopLevelParent(
              system,
              () ->
                  (context, message) -> {
                    if ("boom".equals(message)) {
                      throw new RuntimeException("boom");
                    }
                    received.add(message);
                  },
              SupervisorStrategy.resume());

      spawned.child().tell("boom");
      spawned.child().tell("still working");
      spawned.child().tell("and again");

      assertEquals("still working", received.poll(2, TimeUnit.SECONDS));
      assertEquals("and again", received.poll(2, TimeUnit.SECONDS));
      assertFalse(spawned.child().isTerminated());
    }
  }

  @Test
  void resumeOnAPreStartFailureProceedsToTheMessageLoop() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      Spawned<String> spawned =
          spawnChildUnderTopLevelParent(
              system,
              () ->
                  new Actor<String>() {
                    @Override
                    public void preStart(ActorContext<String> context) {
                      throw new IllegalStateException("cannot start");
                    }

                    @Override
                    public void onMessage(ActorContext<String> context, String message) {
                      received.add(message);
                    }
                  },
              SupervisorStrategy.resume());

      spawned.child().tell("hello");

      assertEquals("hello", received.poll(2, TimeUnit.SECONDS));
      assertFalse(spawned.child().isTerminated());
    }
  }

  @Test
  void restartKeepsTheActorAliveButResetsInternalState() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<Integer> observed = new LinkedBlockingQueue<>();
      Spawned<String> spawned =
          spawnChildUnderTopLevelParent(
              system, () -> new CountingActor(observed), SupervisorStrategy.restart());

      spawned.child().tell("count");
      assertEquals(1, observed.poll(2, TimeUnit.SECONDS));

      spawned.child().tell("boom");
      spawned.child().tell("count");

      // A restarted actor gets a fresh instance, so the counter starts over at 1 rather than
      // continuing from 1 to 2 — the opposite of what resume proves above.
      assertEquals(1, observed.poll(2, TimeUnit.SECONDS));
      assertFalse(spawned.child().isTerminated());
    }
  }

  @Test
  void restartInvokesPreRestartAndPostRestartHooks() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      AtomicBoolean preRestartRan = new AtomicBoolean(false);
      AtomicBoolean postRestartRan = new AtomicBoolean(false);
      Spawned<String> spawned =
          spawnChildUnderTopLevelParent(
              system,
              () -> new HookRecordingActor(preRestartRan, postRestartRan),
              SupervisorStrategy.restart());

      spawned.child().tell("trigger");

      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (!postRestartRan.get()) {
              Thread.sleep(5);
            }
          });
      assertTrue(preRestartRan.get());
      assertFalse(spawned.child().isTerminated());
    }
  }

  @Test
  void restartNeverRedeliversTheMessageThatCausedTheFailure() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      List<String> seen = new CopyOnWriteArrayList<>();
      Spawned<String> spawned =
          spawnChildUnderTopLevelParent(
              system, () -> new RecordingThenThrowingActor(seen), SupervisorStrategy.restart());

      spawned.child().tell("boom");
      spawned.child().tell("after");

      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (!seen.contains("after")) {
              Thread.sleep(5);
            }
          });
      assertEquals(1, seen.stream().filter("boom"::equals).count());
      assertFalse(spawned.child().isTerminated());
    }
  }

  @Test
  void restartOnAPreStartFailureRetriesPreStartOnAFreshInstance() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      AtomicInteger attempts = new AtomicInteger();
      Spawned<String> spawned =
          spawnChildUnderTopLevelParent(
              system,
              () -> {
                int attempt = attempts.incrementAndGet();
                return new Actor<String>() {
                  @Override
                  public void preStart(ActorContext<String> context) {
                    if (attempt < 3) {
                      throw new IllegalStateException("still broken on attempt " + attempt);
                    }
                  }

                  @Override
                  public void onMessage(ActorContext<String> context, String message) {
                    received.add(message);
                  }
                };
              },
              SupervisorStrategy.restart());

      spawned.child().tell("hello");

      assertEquals("hello", received.poll(2, TimeUnit.SECONDS));
      assertEquals(3, attempts.get());
      assertFalse(spawned.child().isTerminated());
    }
  }

  @Test
  void stopDirectiveBehavesLikeTheExistingDefault() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      Spawned<String> spawned =
          spawnChildUnderTopLevelParent(
              system,
              () ->
                  (context, message) -> {
                    throw new RuntimeException("boom");
                  },
              SupervisorStrategy.stop());

      spawned.child().tell("trigger");

      awaitTerminated(spawned.child(), Duration.ofSeconds(2));
    }
  }

  @Test
  void stoppingAParentCascadesStopToItsChildren() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      Spawned<String> spawned =
          spawnChildUnderTopLevelParent(
              system, () -> (context, message) -> {}, SupervisorStrategy.stop());

      spawned.parent().tell("boom");

      awaitTerminated(spawned.parent(), Duration.ofSeconds(2));
      awaitTerminated(spawned.child(), Duration.ofSeconds(2));
    }
  }

  @Test
  void stoppingAParentViaActorSystemStopCascadesToItsChildren() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      Spawned<String> spawned =
          spawnChildUnderTopLevelParent(
              system, () -> (context, message) -> {}, SupervisorStrategy.stop());

      system.stop(spawned.parent());

      awaitTerminated(spawned.parent(), Duration.ofSeconds(2));
      awaitTerminated(spawned.child(), Duration.ofSeconds(2));
    }
  }

  @Test
  void escalateStopsTheWholeAncestorChainUpToTheRoot() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<ActorRef<Object>> parentBox = new LinkedBlockingQueue<>();
      BlockingQueue<ActorRef<String>> childBox = new LinkedBlockingQueue<>();

      ActorRef<Object> grandparent =
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

      child.tell("trigger");

      awaitTerminated(child, Duration.ofSeconds(2));
      awaitTerminated(parent, Duration.ofSeconds(2));
      awaitTerminated(grandparent, Duration.ofSeconds(2));
    }
  }

  @Test
  void escalateStopsSiblingSubtreesAtEveryAncestorLevel() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<ActorRef<Object>> parentABox = new LinkedBlockingQueue<>();
      BlockingQueue<ActorRef<Object>> siblingUnderGrandparentBox = new LinkedBlockingQueue<>();
      BlockingQueue<ActorRef<String>> escalatingChildBox = new LinkedBlockingQueue<>();
      BlockingQueue<ActorRef<String>> siblingLeafBox = new LinkedBlockingQueue<>();

      ActorRef<Object> grandparent =
          system.spawn(
              () ->
                  new Actor<Object>() {
                    @Override
                    public void preStart(ActorContext<Object> context) {
                      parentABox.add(
                          context.spawnChild(
                              () ->
                                  new Actor<Object>() {
                                    @Override
                                    public void preStart(ActorContext<Object> parentContext) {
                                      escalatingChildBox.add(
                                          parentContext.spawnChild(
                                              () ->
                                                  (childContext, message) -> {
                                                    throw new RuntimeException("boom");
                                                  },
                                              "escalating-child",
                                              SupervisorStrategy.escalate()));
                                      siblingLeafBox.add(
                                          parentContext.spawnChild(
                                              () -> (siblingContext, message) -> {},
                                              "sibling-leaf",
                                              SupervisorStrategy.stop()));
                                    }

                                    @Override
                                    public void onMessage(ActorContext<Object> c, Object m) {}
                                  },
                              "parent-a",
                              SupervisorStrategy.stop()));
                      siblingUnderGrandparentBox.add(
                          context.spawnChild(
                              () -> (siblingContext, message) -> {},
                              "sibling-under-grandparent",
                              SupervisorStrategy.stop()));
                    }

                    @Override
                    public void onMessage(ActorContext<Object> context, Object message) {}
                  });

      ActorRef<Object> parentA = parentABox.poll(2, TimeUnit.SECONDS);
      ActorRef<Object> siblingUnderGrandparent =
          siblingUnderGrandparentBox.poll(2, TimeUnit.SECONDS);
      ActorRef<String> escalatingChild = escalatingChildBox.poll(2, TimeUnit.SECONDS);
      ActorRef<String> siblingLeaf = siblingLeafBox.poll(2, TimeUnit.SECONDS);
      assertNotNull(parentA);
      assertNotNull(siblingUnderGrandparent);
      assertNotNull(escalatingChild);
      assertNotNull(siblingLeaf);

      escalatingChild.tell("trigger");

      awaitTerminated(escalatingChild, Duration.ofSeconds(2));
      awaitTerminated(siblingLeaf, Duration.ofSeconds(2));
      awaitTerminated(parentA, Duration.ofSeconds(2));
      awaitTerminated(siblingUnderGrandparent, Duration.ofSeconds(2));
      awaitTerminated(grandparent, Duration.ofSeconds(2));
    }
  }

  @Test
  void spawningAChildUnderAStoppingParentDoesNotOrphanIt() throws InterruptedException {
    int iterations = 200;
    try (ActorSystem system = ActorSystem.start("test")) {
      for (int i = 0; i < iterations; i++) {
        CountDownLatch aboutToSpawn = new CountDownLatch(1);
        BlockingQueue<ActorRef<Object>> childBox = new LinkedBlockingQueue<>();

        ActorRef<Object> parent =
            system.spawn(
                () ->
                    (context, message) -> {
                      aboutToSpawn.countDown();
                      try {
                        childBox.add(
                            context.spawnChild(() -> (childContext, childMessage) -> {}, "child"));
                      } catch (IllegalStateException e) {
                        // The parent was already stopping before the child could even be
                        // registered — no orphan risk in this case, nothing more to do.
                      }
                    },
                "parent-" + i);

        parent.tell("spawn");
        assertTrue(aboutToSpawn.await(2, TimeUnit.SECONDS));
        system.stop(parent);

        ActorRef<Object> child = childBox.poll(2, TimeUnit.SECONDS);
        if (child != null) {
          awaitTerminated(child, Duration.ofSeconds(2));
        }
      }
    }
  }

  @Test
  void spawnChildFailsOnceTheSystemIsShuttingDown() throws InterruptedException {
    ActorSystem system = ActorSystem.start("test");
    CountDownLatch parentReady = new CountDownLatch(1);
    BlockingQueue<ActorContext<Object>> contextBox = new LinkedBlockingQueue<>();

    system.spawn(
        () ->
            new Actor<Object>() {
              @Override
              public void preStart(ActorContext<Object> context) {
                contextBox.add(context);
                parentReady.countDown();
              }

              @Override
              public void onMessage(ActorContext<Object> context, Object message) {}
            });

    assertTrue(parentReady.await(2, TimeUnit.SECONDS));
    ActorContext<Object> parentContext = contextBox.poll(2, TimeUnit.SECONDS);
    assertNotNull(parentContext);

    system.shutdown();

    assertThrows(
        IllegalStateException.class,
        () -> parentContext.spawnChild(() -> (context, message) -> {}, "too-late"));

    system.close();
  }

  @Test
  void childIdsAreNamespacedUnderTheirParentsId() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      Spawned<Object> spawned =
          spawnChildUnderTopLevelParent(
              system, () -> (context, message) -> {}, SupervisorStrategy.stop());

      assertEquals(spawned.parent().id() + "/child", spawned.child().id());
    }
  }

  @Test
  void manyChildrenUnderARestartingParentSurviveConcurrentFailuresAndLoad()
      throws InterruptedException {
    int childCount = 10;
    int messagesPerChild = 500;
    try (ActorSystem system = ActorSystem.start("test")) {
      List<BlockingQueue<Integer>> processedPerChild = new ArrayList<>();
      List<ActorRef<Integer>> children = new ArrayList<>();
      BlockingQueue<ActorRef<Integer>> childBox = new LinkedBlockingQueue<>();

      system.spawn(
          () ->
              new Actor<Object>() {
                @Override
                public void preStart(ActorContext<Object> context) {
                  for (int i = 0; i < childCount; i++) {
                    BlockingQueue<Integer> processed = new LinkedBlockingQueue<>();
                    processedPerChild.add(processed);
                    childBox.add(
                        context.spawnChild(
                            () ->
                                (childContext, message) -> {
                                  if (message < 0) {
                                    throw new RuntimeException("poison");
                                  }
                                  processed.add(message);
                                },
                            "child-" + i,
                            SupervisorStrategy.restart()));
                  }
                }

                @Override
                public void onMessage(ActorContext<Object> context, Object message) {}
              });

      for (int i = 0; i < childCount; i++) {
        ActorRef<Integer> child = childBox.poll(2, TimeUnit.SECONDS);
        assertNotNull(child);
        children.add(child);
      }

      ExecutorService senders = Executors.newFixedThreadPool(8);
      CountDownLatch allSent = new CountDownLatch(childCount * messagesPerChild);
      try {
        for (int c = 0; c < childCount; c++) {
          ActorRef<Integer> child = children.get(c);
          for (int m = 0; m < messagesPerChild; m++) {
            int normalMessage = m;
            senders.execute(
                () -> {
                  if (normalMessage % 17 == 0) {
                    child.tell(-1); // poison: dropped by restart, never redelivered
                  }
                  child.tell(normalMessage);
                  allSent.countDown();
                });
          }
        }
        assertTrue(allSent.await(30, TimeUnit.SECONDS), "all messages should be sent");
      } finally {
        senders.shutdown();
      }

      Set<Integer> expected =
          IntStream.range(0, messagesPerChild).boxed().collect(Collectors.toSet());
      for (int c = 0; c < childCount; c++) {
        BlockingQueue<Integer> processed = processedPerChild.get(c);
        assertTimeoutPreemptively(
            Duration.ofSeconds(10),
            () -> {
              while (processed.size() < messagesPerChild) {
                Thread.sleep(5);
              }
            });
        assertFalse(children.get(c).isTerminated());
        assertEquals(expected, Set.copyOf(processed));
      }
    }
  }

  private record Spawned<C>(ActorRef<Object> parent, ActorRef<C> child) {}

  /**
   * Spawns a top-level parent actor whose {@code preStart} spawns exactly one child named {@code
   * "child"} with the given factory and strategy, returning both refs. The parent itself stops
   * (using the top-level, unconfigurable default) if sent the message {@code "boom"} — used by the
   * cascade-to-children tests.
   */
  private static <C> Spawned<C> spawnChildUnderTopLevelParent(
      ActorSystem system, Supplier<Actor<C>> childFactory, SupervisorStrategy childStrategy)
      throws InterruptedException {
    BlockingQueue<ActorRef<C>> childBox = new LinkedBlockingQueue<>();
    ActorRef<Object> parent =
        system.spawn(
            () ->
                new Actor<Object>() {
                  @Override
                  public void preStart(ActorContext<Object> context) {
                    childBox.add(context.spawnChild(childFactory, "child", childStrategy));
                  }

                  @Override
                  public void onMessage(ActorContext<Object> context, Object message) {
                    if ("boom".equals(message)) {
                      throw new RuntimeException("boom");
                    }
                  }
                });
    ActorRef<C> child = childBox.poll(2, TimeUnit.SECONDS);
    assertNotNull(child);
    return new Spawned<>(parent, child);
  }

  /** Increments and reports an instance-owned counter — distinguishes resume from restart. */
  private static final class CountingActor implements Actor<String> {
    private int counter;
    private final BlockingQueue<Integer> observed;

    CountingActor(BlockingQueue<Integer> observed) {
      this.observed = observed;
    }

    @Override
    public void onMessage(ActorContext<String> context, String message) {
      if ("boom".equals(message)) {
        throw new RuntimeException("boom");
      }
      counter++;
      observed.add(counter);
    }
  }

  private static final class HookRecordingActor implements Actor<String> {
    private final AtomicBoolean preRestartRan;
    private final AtomicBoolean postRestartRan;

    HookRecordingActor(AtomicBoolean preRestartRan, AtomicBoolean postRestartRan) {
      this.preRestartRan = preRestartRan;
      this.postRestartRan = postRestartRan;
    }

    @Override
    public void onMessage(ActorContext<String> context, String message) {
      throw new RuntimeException("boom");
    }

    @Override
    public void preRestart(ActorContext<String> context, Throwable failure, String message) {
      preRestartRan.set(true);
    }

    @Override
    public void postRestart(ActorContext<String> context, Throwable failure) {
      postRestartRan.set(true);
    }
  }

  private static final class RecordingThenThrowingActor implements Actor<String> {
    private final List<String> seen;

    RecordingThenThrowingActor(List<String> seen) {
      this.seen = seen;
    }

    @Override
    public void onMessage(ActorContext<String> context, String message) {
      seen.add(message);
      if ("boom".equals(message)) {
        throw new RuntimeException("boom");
      }
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
