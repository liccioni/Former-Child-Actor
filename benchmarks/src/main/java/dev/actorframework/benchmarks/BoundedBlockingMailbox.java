package dev.actorframework.benchmarks;

import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TASK-306: a faithful copy of {@code framework-core}'s package-private {@code Mailbox} — same
 * fair-lock, {@code ArrayDeque}-backed, bounded/block-on-full design — so this benchmark module can
 * compare different capacities directly. The real {@code Mailbox}'s capacity is not part of {@code
 * framework-core}'s public API (only the single hardcoded default is; TASK-306 is about confirming
 * or replacing that one default, not about adding per-actor configurability, which is a separate,
 * undecided question), so there is no way to spawn an actor with a non-default mailbox capacity
 * through the real framework. This class exists purely to produce comparable benchmark data for
 * that decision — deliberately not wired into {@code framework-core}, the same shape as TASK-303's
 * {@code SharedPoolActorCell}.
 *
 * <p>{@code BoundedBlockingMailboxTest} proves this copy behaves identically to the real {@code
 * Mailbox} (FIFO order, blocks while full, capacity enforced) before any benchmark number from it
 * is trusted.
 */
final class BoundedBlockingMailbox<T> {

  private final ArrayDeque<T> queue;
  private final int capacity;
  private final ReentrantLock lock = new ReentrantLock(true);
  private final Condition notFull = lock.newCondition();
  private final Condition notEmpty = lock.newCondition();
  private boolean closed = false;

  BoundedBlockingMailbox(int capacity) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be positive: " + capacity);
    }
    this.capacity = capacity;
    this.queue = new ArrayDeque<>(Math.min(capacity, 256));
  }

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
