package dev.actorframework.core;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns a single actor's state, mailbox and lifecycle, and runs its dispatch loop.
 *
 * <p>Implements the minimal failure-handling default (TASK-107a): an uncaught exception from {@link
 * Actor#preStart} or {@link Actor#onMessage} is logged with the actor's identity and the message
 * being processed, and this actor alone is stopped. No other actor or the {@link ActorSystem} is
 * affected — each {@code ActorCell} runs on its own dedicated thread (see {@link Dispatcher}) and
 * failures never propagate beyond it.
 */
final class ActorCell<T> {

  private static final Logger LOG = System.getLogger(ActorCell.class.getName());

  private final ActorSystem system;
  private final String id;
  private final Actor<T> actor;
  private final Mailbox<T> mailbox = new Mailbox<>();
  private final ActorRefImpl ref = new ActorRefImpl();
  private final ActorContext<T> context = new ActorContextImpl();
  private final AtomicBoolean stopRequested = new AtomicBoolean(false);
  private final AtomicBoolean terminated = new AtomicBoolean(false);

  ActorCell(ActorSystem system, String id, Actor<T> actor) {
    this.system = system;
    this.id = id;
    this.actor = actor;
  }

  String id() {
    return id;
  }

  ActorRef<T> ref() {
    return ref;
  }

  /**
   * Requests that this actor stop. Idempotent. Closes the mailbox immediately, which discards any
   * queued-but-unprocessed messages (TASK-107) and unblocks any sender waiting in {@link
   * ActorRef#tell}; the actor itself stops once it finishes the message it is currently processing,
   * if any.
   */
  void requestStop() {
    if (stopRequested.compareAndSet(false, true)) {
      mailbox.close();
    }
  }

  /** Entry point for this actor's dispatcher thread. */
  void run() {
    boolean started = false;
    try {
      actor.preStart(context);
      started = true;
    } catch (Throwable t) {
      handleFailure(t, null);
    }
    if (started) {
      dispatchLoop();
    }
    finishTermination();
  }

  private void dispatchLoop() {
    while (true) {
      T message = mailbox.take();
      if (message == null) {
        // Mailbox closed (explicit stop) and drained: exit without a failure.
        return;
      }
      try {
        actor.onMessage(context, message);
      } catch (Throwable t) {
        handleFailure(t, message);
        return;
      }
    }
  }

  private void handleFailure(Throwable failure, T message) {
    LOG.log(
        Level.ERROR,
        "Actor '"
            + id
            + "' threw while processing message ["
            + message
            + "]; stopping this actor. Other actors and the ActorSystem are unaffected.",
        failure);
    requestStop();
  }

  private void finishTermination() {
    try {
      actor.postStop(context);
    } catch (Throwable t) {
      LOG.log(Level.WARNING, "Actor '" + id + "' threw from postStop(); ignoring.", t);
    }
    terminated.set(true);
    system.deregister(this);
  }

  private final class ActorRefImpl implements ActorRef<T> {
    @Override
    public void tell(T message) {
      if (terminated.get()) {
        return;
      }
      mailbox.offer(message);
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public boolean isTerminated() {
      return terminated.get();
    }
  }

  private final class ActorContextImpl implements ActorContext<T> {
    @Override
    public ActorRef<T> self() {
      return ref;
    }

    @Override
    public ActorSystem system() {
      return system;
    }
  }
}
