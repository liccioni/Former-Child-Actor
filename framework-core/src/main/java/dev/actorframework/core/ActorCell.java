package dev.actorframework.core;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Owns a single actor's state, mailbox and lifecycle, and runs its dispatch loop.
 *
 * <p>Implements configurable supervision (TASK-402): a failure from {@link Actor#preStart} or
 * {@link Actor#onMessage} is handled by this cell's own {@link SupervisorStrategy}, on this cell's
 * own dispatcher thread. A top-level actor (spawned via {@link ActorSystem#spawn}) always uses
 * {@link SupervisorStrategy#stop()} — log and stop alone, the M1 (TASK-107a) default, unchanged. A
 * child actor (spawned via {@link ActorContext#spawnChild}) uses whatever strategy its parent gave
 * it. See {@code docs/decisions/ADR-008-supervision-strategies-and-hierarchies.md} for full
 * semantics.
 */
final class ActorCell<T> {

  private static final Logger LOG = System.getLogger(ActorCell.class.getName());

  private final ActorSystem system;
  private final String id;
  private final Supplier<Actor<T>> factory;
  private final ActorCell<?> parent;
  private final SupervisorStrategy strategy;
  private Actor<T> actor;
  private final Mailbox<T> mailbox = new Mailbox<>();
  private final ActorRefImpl ref = new ActorRefImpl();
  private final ActorContext<T> context = new ActorContextImpl();
  private final AtomicBoolean stopRequested = new AtomicBoolean(false);
  private final AtomicBoolean terminated = new AtomicBoolean(false);
  private final Set<ActorCell<?>> children = ConcurrentHashMap.newKeySet();

  ActorCell(
      ActorSystem system,
      String id,
      Supplier<Actor<T>> factory,
      ActorCell<?> parent,
      SupervisorStrategy strategy) {
    this.system = system;
    this.id = id;
    this.factory = factory;
    this.parent = parent;
    this.strategy = strategy;
    this.actor = factory.get();
  }

  String id() {
    return id;
  }

  ActorRef<T> ref() {
    return ref;
  }

  ActorCell<?> parent() {
    return parent;
  }

  Set<ActorCell<?>> children() {
    return children;
  }

  boolean isStopRequested() {
    return stopRequested.get();
  }

  /**
   * Requests that this actor stop. Idempotent. Closes the mailbox immediately, which discards any
   * queued-but-unprocessed messages (TASK-107) and unblocks any sender waiting in {@link
   * ActorRef#tell}; the actor itself stops once it finishes the message it is currently processing,
   * if any. Every actor this one supervises (transitively) is stopped the same way (TASK-402).
   */
  void requestStop() {
    if (!stopRequested.compareAndSet(false, true)) {
      return;
    }
    mailbox.close();
    // Iterative, not recursive: a deep hierarchy must not grow the call stack one frame per level.
    Deque<ActorCell<?>> pending = new ArrayDeque<>(children);
    while (!pending.isEmpty()) {
      ActorCell<?> cell = pending.poll();
      if (cell.stopRequested.compareAndSet(false, true)) {
        cell.mailbox.close();
        pending.addAll(cell.children);
      }
    }
  }

  /** Entry point for this actor's dispatcher thread. */
  void run() {
    boolean started;
    try {
      actor.preStart(context);
      started = true;
    } catch (Throwable t) {
      started = handleFailureAndDecideContinue(t, null);
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
        if (!handleFailureAndDecideContinue(t, message)) {
          return;
        }
      }
    }
  }

  /**
   * Applies this cell's {@link SupervisorStrategy} to a failure from {@code preStart} or {@code
   * onMessage}, returning whether the caller should keep processing ({@link Directive#RESUME} or
   * {@link Directive#RESTART}) or not ({@link Directive#STOP} or {@link Directive#ESCALATE}).
   *
   * <p>A {@link Directive#RESTART} that itself fails (the fresh instance's {@code preStart} throws)
   * re-decides the strategy for that new failure and loops rather than recursing, so a
   * persistently-broken {@code preStart} cannot grow the call stack.
   */
  private boolean handleFailureAndDecideContinue(Throwable failure, T message) {
    Throwable currentFailure = failure;
    T currentMessage = message;
    while (true) {
      switch (strategy.decide(currentFailure)) {
        case RESUME -> {
          log(currentFailure, currentMessage, "resuming with existing state");
          return true;
        }
        case RESTART -> {
          log(currentFailure, currentMessage, "restarting");
          Throwable restartFailure = currentFailure;
          T restartMessage = currentMessage;
          safelyRun(() -> actor.preRestart(context, restartFailure, restartMessage), "preRestart");
          actor = factory.get();
          try {
            actor.preStart(context);
            safelyRun(() -> actor.postRestart(context, restartFailure), "postRestart");
            return true;
          } catch (Throwable t) {
            // The fresh instance's own preStart failed: re-decide for this new failure and loop.
            currentFailure = t;
            currentMessage = null;
          }
        }
        case STOP -> {
          log(
              currentFailure,
              currentMessage,
              "stopping" + (children.isEmpty() ? "" : " (and its children)"));
          requestStop();
          return false;
        }
        case ESCALATE -> {
          log(currentFailure, currentMessage, "escalating to its supervisor");
          escalate();
          return false;
        }
      }
    }
  }

  /**
   * Stops this actor's whole ancestor chain up to the root (each ancestor's own {@link
   * #requestStop()} cascades back down to that ancestor's own children, per {@link
   * #requestStop()}), then stops this actor itself — necessary if this actor has no parent (a
   * top-level actor cannot reach this branch today, since its strategy is always {@link
   * SupervisorStrategy#stop()}; kept for when a future strategy makes it reachable), and harmless
   * (idempotent) otherwise.
   */
  private void escalate() {
    ActorCell<?> ancestor = parent;
    while (ancestor != null) {
      ancestor.requestStop();
      ancestor = ancestor.parent;
    }
    requestStop();
  }

  private void log(Throwable failure, T message, String outcome) {
    LOG.log(
        Level.ERROR,
        "Actor '" + id + "' threw while processing message [" + message + "]; " + outcome + ".",
        failure);
  }

  private void finishTermination() {
    safelyRun(() -> actor.postStop(context), "postStop");
    terminated.set(true);
    system.deregister(this);
  }

  private void safelyRun(ThrowingAction action, String hookName) {
    try {
      action.run();
    } catch (Throwable t) {
      LOG.log(Level.WARNING, "Actor '" + id + "' threw from " + hookName + "(); ignoring.", t);
    }
  }

  @FunctionalInterface
  private interface ThrowingAction {
    void run() throws Exception;
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

    @Override
    public <C> ActorRef<C> spawnChild(Supplier<Actor<C>> childFactory, String name) {
      return spawnChild(childFactory, name, SupervisorStrategy.stop());
    }

    @Override
    public <C> ActorRef<C> spawnChild(
        Supplier<Actor<C>> childFactory, String name, SupervisorStrategy childStrategy) {
      return system.spawnChild(ActorCell.this, childFactory, name, childStrategy);
    }
  }
}
