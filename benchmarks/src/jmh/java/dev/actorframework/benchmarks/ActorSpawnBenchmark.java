package dev.actorframework.benchmarks;

import dev.actorframework.core.ActorRef;
import dev.actorframework.core.ActorSystem;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * TASK-308: measures the cost of {@link ActorSystem#spawn} itself.
 *
 * <p>None of TASK-301/302/306/307 isolate this: {@code ActorCountScalabilityBenchmark} (TASK-301)
 * spawns its {@code actorCount} actors once in {@code @Setup}, outside the timed benchmark, and
 * every other benchmark in the suite spawns a single already-warm actor before measuring anything.
 * ADR-002 defers exactly this question — "actor count is currently bounded only by how many virtual
 * threads (and their associated mailboxes) the JVM can hold" — to real measurement rather than
 * guessing; this benchmark supplies the per-spawn-operation half of that answer (TASK-307 already
 * supplies the standing-memory-per-actor half).
 *
 * <p>Each operation pairs a spawn with an immediate stop request, so a long-running measurement
 * doesn't accumulate an unbounded number of live actors and their virtual threads. Requesting a
 * stop before any message is ever sent lets that actor's dispatch loop see a closed, empty mailbox
 * and exit almost as soon as it's scheduled, so the pair's cost is dominated by spawn — allocating
 * an {@code ActorCell} and {@code Mailbox}, registering the actor, and starting its dedicated
 * virtual thread — not by the stop request itself (a single compare-and-set plus a queue close).
 */
@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class ActorSpawnBenchmark {

  private static final int CONTENDED_SPAWNER_THREADS = 16;

  private ActorSystem system;

  @Setup(Level.Trial)
  public void setUp() {
    system = ActorSystem.start("actor-spawn-benchmark");
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    system.close();
  }

  /** Spawns/sec sustained under concurrent spawning. */
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Threads(CONTENDED_SPAWNER_THREADS)
  public void spawnThroughputUnderLoad() {
    spawnAndStop();
  }

  /** Full spawn-latency distribution (p50/p95/p99/p999), uncontended. */
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(1)
  public void spawnLatencyUncontended() {
    spawnAndStop();
  }

  /**
   * Full spawn-latency distribution (p50/p95/p99/p999) under the same contended spawning load as
   * {@link #spawnThroughputUnderLoad}.
   */
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(CONTENDED_SPAWNER_THREADS)
  public void spawnLatencyUnderLoad() {
    spawnAndStop();
  }

  private void spawnAndStop() {
    ActorRef<Object> actor = system.spawn(() -> (context, message) -> {});
    system.stop(actor);
  }
}
