package dev.actorframework.benchmarks;

import dev.actorframework.core.ActorRef;
import dev.actorframework.core.ActorSystem;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
 * TASK-301: measures how the M1 dispatch strategy (ADR-002 — one dedicated virtual thread per
 * actor) holds up as the number of live actors grows, under a fixed number of concurrent sending
 * threads.
 *
 * <p>ADR-002 names this exact question — "actor count is currently bounded only by how many virtual
 * threads (and their associated mailboxes) the JVM can hold" — as one it defers to real measurement
 * rather than guessing. {@code actorCount} can be widened past the defaults below (e.g. {@code -p
 * actorCount=100000}) to probe further.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
public class ActorCountScalabilityBenchmark {

  private static final int SENDER_THREADS = 16;

  @Param({"1", "10", "100", "1000", "10000"})
  public int actorCount;

  private ActorSystem system;
  private List<ActorRef<CountDownLatch>> actors;
  private AtomicInteger nextActor;

  @Setup(Level.Trial)
  public void setUp() {
    system = ActorSystem.start("actor-count-scalability-benchmark");
    actors = new ArrayList<>(actorCount);
    for (int i = 0; i < actorCount; i++) {
      actors.add(system.spawn(LatchCountingActor::new));
    }
    nextActor = new AtomicInteger();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    system.close();
  }

  /** One round trip against an actor picked round-robin from the {@code actorCount} live actors. */
  @Benchmark
  @Threads(SENDER_THREADS)
  public void roundTrip() throws InterruptedException {
    ActorRef<CountDownLatch> actor =
        actors.get(Math.floorMod(nextActor.getAndIncrement(), actorCount));
    CountDownLatch latch = new CountDownLatch(1);
    actor.tell(latch);
    latch.await();
  }
}
