package dev.actorframework.testkit;

import dev.actorframework.core.Actor;
import dev.actorframework.core.ActorContext;
import dev.actorframework.core.ActorRef;
import dev.actorframework.core.ActorSystem;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A stand-in actor that records every message it receives, for use from test code (TASK-110: "send
 * a message and assert it was received").
 *
 * <pre>{@code
 * TestProbe<String> probe = TestProbe.create(system);
 * someActor.tell(new Greet(probe.ref()));
 * assertEquals("hello", probe.expectMessage(Duration.ofSeconds(1)));
 * }</pre>
 */
public final class TestProbe<T> {

  private volatile ActorRef<T> ref;
  private final BlockingQueue<T> received = new LinkedBlockingQueue<>();

  private TestProbe() {}

  public static <T> TestProbe<T> create(ActorSystem system) {
    TestProbe<T> probe = new TestProbe<>();
    probe.ref = system.spawn(() -> probe.new RecordingActor());
    return probe;
  }

  public ActorRef<T> ref() {
    return ref;
  }

  /**
   * Waits up to {@code timeout} for the next message and returns it.
   *
   * @throws AssertionError if no message arrives within {@code timeout}
   */
  public T expectMessage(Duration timeout) {
    try {
      T message = received.poll(timeout.toNanos(), TimeUnit.NANOSECONDS);
      if (message == null) {
        throw new AssertionError("Expected a message within " + timeout + " but none arrived");
      }
      return message;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted while waiting for a message", e);
    }
  }

  /** Waits up to {@code timeout} for the next message and asserts it equals {@code expected}. */
  public void expectMessage(T expected, Duration timeout) {
    T actual = expectMessage(timeout);
    if (!expected.equals(actual)) {
      throw new AssertionError("Expected message [" + expected + "] but got [" + actual + "]");
    }
  }

  /** Number of messages received so far but not yet consumed via {@link #expectMessage}. */
  public int receivedCount() {
    return received.size();
  }

  private final class RecordingActor implements Actor<T> {
    @Override
    public void onMessage(ActorContext<T> context, T message) {
      received.add(message);
    }
  }
}
