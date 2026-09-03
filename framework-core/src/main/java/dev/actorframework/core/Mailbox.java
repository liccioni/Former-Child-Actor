package dev.actorframework.core;

import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The FIFO queue of messages waiting to be processed by a single actor (TASK-103).
 *
 * <p><b>Provisional bounds decision (TASK-103a):</b> the mailbox is bounded with a generous default
 * capacity and blocks the sending thread when full ("block-on-full"). This is a deliberately
 * provisional default, chosen only so that early benchmarks and the Day-1 experiment (Section 18)
 * are comparable; it is revisited with real measurements at TASK-306 and recorded in {@code
 * docs/decisions/mailbox-bounds-provisional.md}. Do not treat this policy as final.
 *
 * <p>Thread safety: {@link #offer} may be called concurrently by many sender threads. {@link #take}
 * is called by exactly one thread — the actor's dedicated dispatcher thread (TASK-105) — which is
 * what gives the runtime its sequential-processing guarantee (TASK-106); this class does not itself
 * enforce single-consumer usage.
 *
 * <p>Ownership: a {@code Mailbox} is owned by exactly one {@link ActorCell} for its entire lifetime
 * and is never shared between actors.
 */
final class Mailbox<T> {

  /** Provisional default capacity (TASK-103a) — not yet backed by benchmark data. */
  static final int DEFAULT_CAPACITY = 1024;

  private final ArrayDeque<T> queue;
  private final int capacity;
  private final ReentrantLock lock = new ReentrantLock();
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
    lock.lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
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
