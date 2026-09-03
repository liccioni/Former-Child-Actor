package dev.actorframework.testkit;

import dev.actorframework.core.ActorRef;
import java.time.Duration;
import java.util.function.BooleanSupplier;

/**
 * Polling assertions for asynchronous actor behavior (TASK-110: "await a condition with a timeout"
 * and "basic failure assertion").
 */
public final class Await {

  private static final Duration POLL_INTERVAL = Duration.ofMillis(10);

  private Await() {}

  /**
   * Polls {@code condition} until it returns {@code true} or {@code timeout} elapses.
   *
   * @throws AssertionError if {@code timeout} elapses before {@code condition} becomes true
   */
  public static void awaitTrue(Duration timeout, String description, BooleanSupplier condition) {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() >= deadlineNanos) {
        throw new AssertionError(description + " (timed out after " + timeout + ")");
      }
      sleepUninterruptibly(POLL_INTERVAL);
    }
  }

  /**
   * Waits up to {@code timeout} for {@code ref}'s actor to report itself terminated — the basic
   * failure/termination assertion TASK-110 calls for, e.g. after TASK-107a's default "log and stop"
   * behavior has run.
   *
   * @throws AssertionError if the actor has not terminated within {@code timeout}
   */
  public static void awaitTerminated(ActorRef<?> ref, Duration timeout) {
    awaitTrue(timeout, "Expected actor '" + ref.id() + "' to have terminated", ref::isTerminated);
  }

  private static void sleepUninterruptibly(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted while awaiting a condition", e);
    }
  }
}
