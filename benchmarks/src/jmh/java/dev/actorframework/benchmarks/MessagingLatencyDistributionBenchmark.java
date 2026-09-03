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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * TASK-302: messaging throughput and full latency-distribution measurement for a single actor.
 *
 * <p>TASK-301's {@code ActorCountScalabilityBenchmark} already reports a throughput number for one
 * actor under 16 concurrent senders (its {@code actorCount=1} row), but throughput and simple
 * average latency both hide the tail — exactly where mailbox contention (lock acquisition,
 * block-on-full backpressure, TASK-103a/ADR-003) actually shows up. This class closes that gap:
 * {@link #latencyUnderLoad} uses JMH's {@code SampleTime} mode, which buckets individual invocation
 * timings into percentiles automatically. "p999" in the original design document is the
 * industry-standard shorthand for the 99.9th percentile — JMH's {@code p99.9} bucket in the result
 * table below; JMH reports {@code p50.0}, {@code p90.0}, {@code p95.0}, {@code p99.0}, {@code
 * p99.9}, {@code p99.99}, {@code p99.999}, and {@code p100} by default, which covers
 * p50/p95/p99/p999 directly.
 *
 * <p>{@link #latencyUncontended} is the same measurement with a single sender, as a clean baseline
 * to compare the contended distribution against — contention should widen the tail without
 * necessarily moving the median much, and that gap is itself useful data.
 */
@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class MessagingLatencyDistributionBenchmark {

  private static final int CONTENDED_SENDER_THREADS = 16;

  private ActorSystem system;
  private ActorRef<CountDownLatch> actor;

  @Setup(Level.Trial)
  public void setUp() {
    system = ActorSystem.start("messaging-latency-distribution-benchmark");
    actor = system.spawn(LatchCountingActor::new);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    system.close();
  }

  /** Messages/sec against a single actor under realistic concurrent sender load. */
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Threads(CONTENDED_SENDER_THREADS)
  public void throughputUnderLoad() throws InterruptedException {
    roundTrip();
  }

  /** Full latency distribution (p50/p95/p99/p999) with a single, uncontended sender. */
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(1)
  public void latencyUncontended() throws InterruptedException {
    roundTrip();
  }

  /** Full latency distribution (p50/p95/p99/p999) under the same contended load as throughput. */
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(CONTENDED_SENDER_THREADS)
  public void latencyUnderLoad() throws InterruptedException {
    roundTrip();
  }

  private void roundTrip() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    actor.tell(latch);
    latch.await();
  }
}
