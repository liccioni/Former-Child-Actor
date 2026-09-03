package dev.actorframework.benchmarks;

import dev.actorframework.core.ActorRef;
import dev.actorframework.core.ActorSystem;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * TASK-301: measures the baseline cost of the M1 dispatch strategy (ADR-002 — one dedicated virtual
 * thread per actor) for a single, already-warm actor: a message travels through the mailbox, onto
 * the actor's dispatcher thread, through {@code onMessage}, and back via a completion signal.
 *
 * <p>This is the number every alternative dispatch strategy considered at TASK-303 has to beat, and
 * the floor that any mailbox-bounds change at TASK-306 is measured against.
 */
@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class DispatchRoundTripBenchmark {

  private ActorSystem system;
  private ActorRef<CountDownLatch> actor;

  @Setup(Level.Trial)
  public void setUp() {
    system = ActorSystem.start("dispatch-round-trip-benchmark");
    actor = system.spawn(LatchCountingActor::new);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    system.close();
  }

  /** How many round trips a single actor sustains per second under continuous load. */
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public void roundTripThroughput() throws InterruptedException {
    roundTrip();
  }

  /** How long a single round trip takes, uncontended. */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void roundTripLatency() throws InterruptedException {
    roundTrip();
  }

  private void roundTrip() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    actor.tell(latch);
    latch.await();
  }
}
