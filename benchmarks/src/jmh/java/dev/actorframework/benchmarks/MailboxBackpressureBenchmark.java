package dev.actorframework.benchmarks;

import java.util.concurrent.TimeUnit;
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
 * TASK-306: does the mailbox's specific capacity matter once a mailbox is actually driven to
 * backpressure? None of TASK-301/302's benchmarks ever did — their busiest case (16 senders against
 * one actor) never approaches the default capacity of 1,024, so they show how the runtime behaves
 * well below the limit, not what the limit itself costs or buys. This benchmark closes that gap
 * directly, against {@link BoundedBlockingMailbox} at several candidate capacities, with a consumer
 * deliberately made the bottleneck so the mailbox is genuinely saturated throughout the measurement
 * — not just occasionally full.
 *
 * <p>Queueing theory predicts: once arrivals exceed a fixed service rate, steady-state
 * <em>acceptance</em> throughput is capped by the consumer's rate regardless of buffer size, and so
 * is the average {@code offer()} blocking latency (roughly "how many senders are already ahead of
 * me in the fair lock's queue, divided by the drain rate") — capacity mainly changes how much
 * bursty demand can be absorbed before backpressure kicks in at all, and how much memory is
 * retained while saturated (TASK-307 already showed the mailbox costs nothing extra for capacity
 * that's never actually used). {@link #acceptanceThroughputUnderSaturation} and {@link
 * #offerLatencyUnderSaturation} test that prediction directly rather than assuming it.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
public class MailboxBackpressureBenchmark {

  /** ~1ms of deliberately slow, fixed-rate processing — guarantees senders outpace the consumer. */
  private static final long CONSUMER_PROCESSING_NANOS = 1_000_000;

  private static final int SENDER_THREADS = 8;

  @Param({"16", "128", "1024", "8192"})
  public int capacity;

  private BoundedBlockingMailbox<Object> mailbox;
  private Thread consumer;
  private volatile boolean stopConsumer;

  @Setup(Level.Trial)
  public void setUp() {
    mailbox = new BoundedBlockingMailbox<>(capacity);
    stopConsumer = false;
    consumer =
        new Thread(
            () -> {
              while (!stopConsumer) {
                Object message = mailbox.take();
                if (message == null) {
                  return;
                }
                busySpin(CONSUMER_PROCESSING_NANOS);
              }
            });
    consumer.setDaemon(true);
    consumer.start();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws InterruptedException {
    stopConsumer = true;
    mailbox.close();
    consumer.join(TimeUnit.SECONDS.toMillis(5));
  }

  /** Messages/sec actually accepted once the mailbox is kept continuously saturated. */
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Threads(SENDER_THREADS)
  public void acceptanceThroughputUnderSaturation() {
    mailbox.offer(new Object());
  }

  /**
   * How long offer() blocks per call under sustained saturation — the backpressure senders feel.
   */
  @Benchmark
  @BenchmarkMode(Mode.SampleTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Threads(SENDER_THREADS)
  public void offerLatencyUnderSaturation() {
    mailbox.offer(new Object());
  }

  private static void busySpin(long nanos) {
    long deadline = System.nanoTime() + nanos;
    while (System.nanoTime() < deadline) {
      Thread.onSpinWait();
    }
  }
}
