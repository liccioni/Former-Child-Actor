package dev.actorframework.benchmarks;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * TASK-303: a minimal prototype of ADR-002's "shared executor" alternative — a fixed pool of
 * threads shared across many actors, each actor scheduled onto the pool only when it has work and
 * isn't already scheduled, draining a bounded batch of messages before yielding the thread back to
 * the pool. This is the classical event-driven dispatcher pattern (the shape of, e.g., Akka's
 * default dispatcher), contrasted against ADR-002's one-dedicated-thread-per-actor strategy that
 * blocks on {@code mailbox.take()} for the actor's entire lifetime.
 *
 * <p>Deliberately not wired into {@code framework-core} — this exists purely to produce comparable
 * benchmark data for TASK-303's ADR-gated decision (see {@code AGENTS.md}: the dispatch strategy is
 * not changed ad hoc). It reimplements just enough (a FIFO mailbox and the sequential-processing
 * guarantee) to be a fair comparison point, not a candidate drop-in replacement for {@code
 * ActorCell}.
 *
 * <p>Sequential processing is guaranteed the same way real event-driven dispatchers guarantee it:
 * {@code scheduled} is a compare-and-set gate, so at most one {@link #drain} task per actor is ever
 * queued or running on the shared pool at a time; a message that arrives in the narrow window
 * between a drain finishing its batch and clearing the flag is caught by the
 * re-check-and-reschedule at the end of {@link #drain}.
 */
final class SharedPoolActorCell<T> {

  private static final int MAX_MESSAGES_PER_DRAIN = 16;

  private final ConcurrentLinkedQueue<T> mailbox = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean scheduled = new AtomicBoolean(false);
  private final ExecutorService pool;
  private final Consumer<T> onMessage;

  SharedPoolActorCell(ExecutorService pool, Consumer<T> onMessage) {
    this.pool = pool;
    this.onMessage = onMessage;
  }

  void tell(T message) {
    mailbox.add(message);
    scheduleIfNeeded();
  }

  private void scheduleIfNeeded() {
    if (scheduled.compareAndSet(false, true)) {
      pool.execute(this::drain);
    }
  }

  private void drain() {
    int processed = 0;
    T message;
    while (processed < MAX_MESSAGES_PER_DRAIN && (message = mailbox.poll()) != null) {
      onMessage.accept(message);
      processed++;
    }
    scheduled.set(false);
    if (!mailbox.isEmpty()) {
      scheduleIfNeeded();
    }
  }
}
