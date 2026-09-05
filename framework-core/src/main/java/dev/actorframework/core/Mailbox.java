package dev.actorframework.core;

import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * The FIFO queue of messages waiting to be processed by a single actor (TASK-103).
 *
 * <p><b>Bounds confirmed with benchmark data (TASK-306):</b> the mailbox is bounded with a default
 * capacity and blocks the sending thread when full ("block-on-full"). TASK-103a chose this only so
 * early benchmarks and the Day-1 experiment (Section 18) would be comparable; TASK-306 confirmed
 * both the policy and the specific default capacity against real backpressure data. See {@code
 * docs/decisions/ADR-007-mailbox-bounds-confirmed.md}.
 *
 * <p>Thread safety: {@link #offer} may be called concurrently by many sender threads. {@link #take}
 * is called by exactly one thread — the actor's dedicated dispatcher thread (TASK-105) — which is
 * what gives the runtime its sequential-processing guarantee (TASK-106); this class does not itself
 * enforce single-consumer usage.
 *
 * <p><b>Cross-sender ordering (TASK-201):</b> if one {@link #offer} call returns before another
 * begins, with a real happens-before relationship between the two callers, the first is enqueued
 * before the second. Calls with no such relationship have unspecified relative order. When the
 * mailbox is full, senders already blocked in {@link #offer} are admitted before a sender that
 * arrives later, once space frees up. See {@code
 * docs/decisions/ADR-003-cross-sender-mailbox-ordering.md}.
 *
 * <p>Ownership: a {@code Mailbox} is owned by exactly one {@link ActorCell} for its entire lifetime
 * and is never shared between actors.
 */
final class Mailbox<T> {

  /** Default capacity, confirmed by benchmark data (TASK-306, ADR-007). */
  static final int DEFAULT_CAPACITY = 1024;

  private final ArrayDeque<T> queue;
  private final int capacity;

  // Fair (TASK-201, ADR-003): guarantees that a sender already blocked in offer() is admitted
  // before one that arrives later, once space frees up. A non-fair lock lets a fresh caller
  // barge ahead of an already-signaled waiter still reacquiring the lock.
  private final ReentrantLock lock = new ReentrantLock(true);
  private final Condition notFull = lock.newCondition();
  private final Condition notEmpty = lock.newCondition();
  private boolean closed = false;

  Mailbox() {
    this(DEFAULT_CAPACITY);
  }

  Mailbox(int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be positive: " + capacity);
    }
    this.capacity = capacity;
    this.queue = new ArrayDeque<>(Math.min(capacity, 256));
  }

  /**
   * Enqueues a message, blocking the caller while the mailbox is full.
   *
   * @return {@code true} if the message was enqueued; {@code false} if the mailbox was (or became,
   *     while waiting for space) closed, in which case the message is dropped.
   */
  boolean offer(T message) {
    lock.lock();
    try {
      while (queue.size() >= capacity && !closed) {
        awaitUninterruptibly(notFull);
      }
      if (closed) {
        return false;
      }
      queue.addLast(message);
      notEmpty.signal();
      return true;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Blocks until a message is available or the mailbox is closed and drained.
   *
   * @return the next message, or {@code null} once the mailbox is closed and has no more messages
   *     to deliver.
   */
  T take() {
    lock.lock();
    try {
      while (queue.isEmpty() && !closed) {
        awaitUninterruptibly(notEmpty);
      }
      if (queue.isEmpty()) {
        return null;
      }
      T message = queue.removeFirst();
      notFull.signal();
      return message;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Closes the mailbox: no further messages may be enqueued, any thread blocked in {@link #offer}
   * is released (its message is dropped), and any messages still queued at closing time are
   * discarded (TASK-107: queued-but-unprocessed messages are not delivered). Idempotent.
   */
  void close() {
    close(discarded -> {});
  }

  /**
   * Same as {@link #close()}, but calls {@code onDiscard} for each message still queued at closing
   * time, in FIFO order, before it is discarded. Used by {@link ActorCell} (TASK-402) to resolve
   * any pending child-failure report that would otherwise leave its reporting child blocked forever
   * waiting for a supervisor that is gone.
   */
  void close(Consumer<T> onDiscard) {
    lock.lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      queue.forEach(onDiscard);
      queue.clear();
      notFull.signalAll();
      notEmpty.signalAll();
    } finally {
      lock.unlock();
    }
  }

  private static void awaitUninterruptibly(Condition condition) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          condition.await();
          return;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
