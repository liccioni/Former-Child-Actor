package dev.actorframework.benchmarks;

import dev.actorframework.core.ActorRef;
import dev.actorframework.core.ActorSystem;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TASK-307: standing-memory-footprint measurement for actors — deliberately <em>not</em> a JMH
 * benchmark.
 *
 * <p>JMH's timing-based harness (and its bundled GCProfiler) measures allocation <em>rate</em> —
 * bytes allocated per operation — which is the right tool for the throughput/latency questions
 * TASK-301/302 already answered. TASK-307 asks a different question: how much heap does N <em>live,
 * idle</em> actors cost, just standing around? That is a retained-footprint snapshot, not a
 * per-operation rate, so this uses a plain heap-delta measurement instead: force GC, sample {@code
 * Runtime} heap usage, spawn or exercise N actors, sample again, divide the delta by N.
 *
 * <p>This is inherently approximate — {@code Runtime.gc()} is a request, not a guarantee, and JVM
 * heap usage includes JIT-compiled code and class-metadata growth unrelated to actor count.
 * Multiple trials are run and all of them reported, so the noise is visible rather than hidden
 * behind one falsely precise number — the same honesty bar the JMH suite's own confidence intervals
 * hold to.
 *
 * <p>Three scenarios, matching the design document's Section 12:
 *
 * <ul>
 *   <li><b>Empty actor</b> — freshly spawned, no message ever sent: the baseline cost of {@code
 *       ActorCell} plus a never-touched {@code Mailbox} (whose backing {@code ArrayDeque} pre-sizes
 *       to 256 slots at construction, regardless of whether it is ever used).
 *   <li><b>Actor with mailbox after a burst, now idle</b> — every actor already has a mailbox from
 *       birth (there is no "actor without one" to compare against), so the meaningful comparison
 *       is: does a mailbox that has briefly held a backlog leave a permanently larger footprint
 *       once idle again? {@code ArrayDeque} never shrinks its backing array back down after
 *       growing, so the answer is expected to be yes.
 *   <li><b>Actor under sustained load</b> — memory sampled <em>while</em> many actors are being
 *       actively bombarded with messages, not after things settle — the higher-water-mark profile
 *       during real operation, deliberately without a forced GC (which would itself distort an
 *       active workload).
 * </ul>
 *
 * <p>A fourth scenario, added for TASK-306, isolates the specific question the mailbox-bounds
 * decision needs answered: when a mailbox is actually filled to capacity, how much does the chosen
 * capacity itself cost, independent of message payload size? It uses {@link BoundedBlockingMailbox}
 * directly (not full actors) and a single shared sentinel object in every slot, so the measured
 * delta is purely the backing array's cost at each capacity, not per-message allocation (which
 * TASK-307's other scenarios already cover, and which varies by application payload anyway — not
 * something the framework controls).
 *
 * <p>Run via {@code ./gradlew :benchmarks:memoryFootprint}.
 */
public final class MemoryFootprintHarness {

  private static final int ACTOR_COUNT = 10_000;
  private static final int TRIALS = 3;
  private static final int BURST_SIZE = 300; // > the mailbox's initial 256-slot backing array

  private static final int MAILBOXES_FOR_SATURATION = 1_000;
  private static final int[] SATURATION_CAPACITIES = {16, 128, 1024, 8192};
  private static final Object SATURATION_SENTINEL = new Object();

  public static void main(String[] args) throws InterruptedException {
    System.out.println("TASK-307 memory footprint (best-effort, Runtime heap-delta sampling)");
    System.out.println("Actors per trial: " + ACTOR_COUNT + ", trials: " + TRIALS);
    System.out.println();

    report("Empty actor (freshly spawned, no messages ever sent)", emptyActorScenario());
    report("Actor whose mailbox grew from a burst, now idle again", burstThenIdleScenario());
    report("Actor under sustained load (sampled live, no forced GC)", underLoadScenario());

    System.out.println();
    System.out.println(
        "TASK-306: mailbox retained memory when actually filled to capacity (isolated mailbox,"
            + " shared sentinel message, no actor)");
    for (int capacity : SATURATION_CAPACITIES) {
      report("  capacity=" + capacity, mailboxSaturationScenario(capacity));
    }
  }

  private static long[] mailboxSaturationScenario(int capacity) {
    long[] bytesPerMailbox = new long[TRIALS];
    for (int trial = 0; trial < TRIALS; trial++) {
      long before = usedMemoryAfterGc();
      List<BoundedBlockingMailbox<Object>> mailboxes = new ArrayList<>(MAILBOXES_FOR_SATURATION);
      for (int i = 0; i < MAILBOXES_FOR_SATURATION; i++) {
        BoundedBlockingMailbox<Object> mailbox = new BoundedBlockingMailbox<>(capacity);
        for (int m = 0; m < capacity; m++) {
          mailbox.offer(SATURATION_SENTINEL);
        }
        mailboxes.add(mailbox);
      }
      long after = usedMemoryAfterGc();
      bytesPerMailbox[trial] = (after - before) / mailboxes.size();
    }
    return bytesPerMailbox;
  }

  private static long[] emptyActorScenario() throws InterruptedException {
    long[] bytesPerActor = new long[TRIALS];
    for (int trial = 0; trial < TRIALS; trial++) {
      long before = usedMemoryAfterGc();
      try (ActorSystem system = ActorSystem.start("memory-empty-actor")) {
        List<ActorRef<Object>> actors = new ArrayList<>(ACTOR_COUNT);
        for (int i = 0; i < ACTOR_COUNT; i++) {
          ActorRef<Object> actor = system.spawn(() -> (context, message) -> {});
          actors.add(actor);
        }
        long after = usedMemoryAfterGc();
        bytesPerActor[trial] = (after - before) / actors.size();
      }
    }
    return bytesPerActor;
  }

  private static long[] burstThenIdleScenario() throws InterruptedException {
    long[] bytesPerActor = new long[TRIALS];
    for (int trial = 0; trial < TRIALS; trial++) {
      long before = usedMemoryAfterGc();
      try (ActorSystem system = ActorSystem.start("memory-burst-then-idle")) {
        List<CountDownLatch> drainedPerActor = new ArrayList<>(ACTOR_COUNT);
        for (int i = 0; i < ACTOR_COUNT; i++) {
          CountDownLatch drained = new CountDownLatch(BURST_SIZE);
          drainedPerActor.add(drained);
          ActorRef<Runnable> actor = system.spawn(() -> (context, message) -> message.run());
          // Block this actor's own dispatcher thread mid-onMessage (not on mailbox.take()), so
          // the burst below queues up behind it before draining, forcing the backing array to
          // grow past its initial 256 slots.
          actor.tell(() -> sleepQuietly(20));
          for (int m = 0; m < BURST_SIZE; m++) {
            actor.tell(drained::countDown);
          }
        }
        for (CountDownLatch drained : drainedPerActor) {
          drained.await(10, TimeUnit.SECONDS);
        }
        long after = usedMemoryAfterGc();
        bytesPerActor[trial] = (after - before) / drainedPerActor.size();
      }
    }
    return bytesPerActor;
  }

  private static long[] underLoadScenario() throws InterruptedException {
    long[] bytesPerActor = new long[TRIALS];
    int senderThreads = 8;
    for (int trial = 0; trial < TRIALS; trial++) {
      try (ActorSystem system = ActorSystem.start("memory-under-load")) {
        List<ActorRef<Runnable>> actors = new ArrayList<>(ACTOR_COUNT);
        for (int i = 0; i < ACTOR_COUNT; i++) {
          ActorRef<Runnable> actor = system.spawn(() -> (context, message) -> message.run());
          actors.add(actor);
        }
        long before = usedMemoryAfterGc();

        AtomicBoolean keepSending = new AtomicBoolean(true);
        ExecutorService senders = Executors.newFixedThreadPool(senderThreads);
        for (int s = 0; s < senderThreads; s++) {
          int senderIndex = s;
          senders.execute(
              () -> {
                int next = senderIndex;
                while (keepSending.get()) {
                  actors.get(next % actors.size()).tell(() -> {});
                  next += senderThreads;
                }
              });
        }
        sleepQuietly(500); // let load build up to a steady state before sampling
        long duringLoad = currentUsedMemory(); // no forced GC: this is the live-workload profile

        keepSending.set(false);
        senders.shutdown();
        senders.awaitTermination(5, TimeUnit.SECONDS);

        bytesPerActor[trial] = (duringLoad - before) / actors.size();
      }
    }
    return bytesPerActor;
  }

  private static long usedMemoryAfterGc() {
    Runtime runtime = Runtime.getRuntime();
    for (int i = 0; i < 3; i++) {
      runtime.gc();
      sleepQuietly(100);
    }
    return currentUsedMemory();
  }

  private static long currentUsedMemory() {
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void report(String label, long[] bytesPerActor) {
    double average = Arrays.stream(bytesPerActor).average().orElse(0);
    System.out.printf(
        "%-55s samples(bytes/actor)=%s avg=%.0f%n", label, Arrays.toString(bytesPerActor), average);
  }

  private MemoryFootprintHarness() {}
}
