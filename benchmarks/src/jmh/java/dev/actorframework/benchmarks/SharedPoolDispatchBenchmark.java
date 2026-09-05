package dev.actorframework.benchmarks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * TASK-303: the same round-trip throughput measurement as TASK-301's {@code
 * ActorCountScalabilityBenchmark} — same actor counts, same 16 concurrent sender threads, same
 * round-robin send pattern — but against {@link SharedPoolActorCell} (ADR-002's "shared executor"
 * alternative) instead of the real {@code ActorSystem}'s one-virtual-thread-per-actor dispatch
 * strategy. Matching the workload shape exactly is what makes the two directly comparable rather
 * than measuring two different things.
 *
 * <p>The shared pool is a fixed-size platform-thread pool sized to the available processors — the
 * classical shape of this alternative (a small, bounded number of worker threads shared across a
 * potentially much larger number of actors), as opposed to a thread per actor.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
public class SharedPoolDispatchBenchmark {

  private static final int SENDER_THREADS = 16;

  @Param({"1", "10", "100", "1000", "10000"})
  public int actorCount;

  private ExecutorService pool;
  private List<SharedPoolActorCell<CountDownLatch>> actors;
  private AtomicInteger nextActor;

  @Setup(Level.Trial)
  public void setUp() {
    pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    actors = new ArrayList<>(actorCount);
    for (int i = 0; i < actorCount; i++) {
      actors.add(new SharedPoolActorCell<>(pool, CountDownLatch::countDown));
    }
    nextActor = new AtomicInteger();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    pool.shutdown();
  }

  /** One round trip against an actor picked round-robin from the {@code actorCount} live actors. */
  @Benchmark
  @Threads(SENDER_THREADS)
  public void roundTrip() throws InterruptedException {
    SharedPoolActorCell<CountDownLatch> actor =
        actors.get(Math.floorMod(nextActor.getAndIncrement(), actorCount));
    CountDownLatch latch = new CountDownLatch(1);
    actor.tell(latch);
    latch.await();
  }
}
