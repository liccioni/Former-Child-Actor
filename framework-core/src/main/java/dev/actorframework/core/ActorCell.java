package dev.actorframework.core;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Owns a single actor's state, mailbox and lifecycle, and runs its dispatch loop.
 *
 * <p>Implements configurable supervision and actor hierarchies (TASK-402): a failure is resolved by
 * asking this actor's parent (if any) for a {@link SupervisorDirective}, resolved on the parent's
 * own dispatch thread by asking its {@link Actor#supervisorStrategy}. An actor with no parent
 * always resolves its own failure to {@link SupervisorDirective#STOP}, matching the fixed M1
 * default (TASK-107a). See {@code
 * docs/decisions/ADR-008-supervision-strategies-and-actor-hierarchies.md}.
 */
final class ActorCell<T> {

  private static final Logger LOG = System.getLogger(ActorCell.class.getName());

  private final ActorSystem system;
  private final String id;
  private final Supplier<Actor<T>> factory;
  private final ActorCell<?> parent;
  private Actor<T> actor;
  private final Mailbox<Envelope<T>> mailbox = new Mailbox<>();
  private final ActorRefImpl ref = new ActorRefImpl();
  private final ActorContext<T> context = new ActorContextImpl();
  private final Set<ActorCell<?>> children = ConcurrentHashMap.newKeySet();
  private final AtomicBoolean stopRequested = new AtomicBoolean(false);
  private final AtomicBoolean terminated = new AtomicBoolean(false);
  private final CompletableFuture<Void> terminationSignal = new CompletableFuture<>();

  ActorCell(ActorSystem system, String id, Supplier<Actor<T>> factory, ActorCell<?> parent) {
    this.system = system;
    this.id = id;
    this.factory = factory;
    this.parent = parent;
    this.actor = factory.get();
  }

  String id() {
    return id;
  }

  ActorRef<T> ref() {
    return ref;
  }

  /**
   * Registers {@code child} as a child of this actor. If this actor's own stop was already
   * requested — racing a child just spawned from within its last in-flight message — the child is
   * immediately asked to stop too, so no child can outlive a parent that is already stopping.
   */
  void addChild(ActorCell<?> child) {
    children.add(child);
    if (stopRequested.get()) {
      child.requestStop();
    }
  }

  private void removeChild(ActorCell<?> child) {
    children.remove(child);
  }

  /**
   * Requests that this actor stop. Idempotent. Closes the mailbox immediately, which discards any
   * queued-but-unprocessed messages (TASK-107) and unblocks any sender waiting in {@link
   * ActorRef#tell}; the actor itself stops once it finishes the message it is currently processing,
   * if any. Cascades to every child of this actor (TASK-402): stopping a parent stops its whole
   * subtree.
   */
  void requestStop() {
    if (stopRequested.compareAndSet(false, true)) {
      mailbox.close(this::resolveDiscardedEnvelope);
      for (ActorCell<?> child : children) {
        child.requestStop();
      }
    }
  }

  /** Blocks until this actor (and, transitively, its whole subtree) has finished terminating. */
  void awaitTermination() {
    joinUninterruptibly(terminationSignal);
  }

  /** Entry point for this actor's dispatcher thread. */
  void run() {
    if (attemptStart()) {
      dispatchLoop();
    }
    awaitChildrenTerminated();
    finishTermination();
  }

  /** Runs {@code preStart} once; on failure, resolves and applies a directive for it. */
  private boolean attemptStart() {
    try {
      actor.preStart(context);
      return true;
    } catch (Throwable t) {
      return applyDirective(reportFailureUp(t, null));
    }
  }

  private void dispatchLoop() {
    while (true) {
      Envelope<T> envelope = mailbox.take();
      if (envelope == null) {
        // Mailbox closed (explicit stop, or a resolved failure) and drained.
        return;
      }
      if (envelope instanceof Envelope.ChildFailure<T> childFailure) {
        resolveChildFailure(childFailure);
        continue;
      }
      T message = ((Envelope.Message<T>) envelope).value();
      try {
        actor.onMessage(context, message);
      } catch (Throwable t) {
        if (!applyDirective(reportFailureUp(t, message))) {
          return;
        }
      }
    }
  }

  /**
   * Asks this actor's parent (if any) how to proceed after {@code failure}, blocking this actor's
   * own thread until resolved. An actor with no parent — or whose parent's mailbox is already
   * closed — always resolves to {@link SupervisorDirective#STOP}: there is no supervisor to ask.
   */
  private SupervisorDirective reportFailureUp(Throwable failure, T message) {
    SupervisorDirective directive;
    if (parent == null) {
      directive = SupervisorDirective.STOP;
    } else {
      CompletableFuture<SupervisorDirective> decision = new CompletableFuture<>();
      directive =
          parent.deliverChildFailure(this, failure, decision)
              ? joinUninterruptibly(decision)
              : SupervisorDirective.STOP;
    }
    logDirective(directive, failure, message);
    return directive;
  }

  private void logDirective(SupervisorDirective directive, Throwable failure, T message) {
    String outcome =
        switch (directive) {
          case RESUME -> "resuming (message discarded)";
          case RESTART -> "restarting";
          case STOP -> "stopping";
          case ESCALATE -> "stopping (escalated to its supervisor)";
        };
    LOG.log(
        Level.WARNING,
        "Actor '" + id + "' threw while processing [" + message + "]; " + outcome + ".",
        failure);
  }

  /**
   * Applies a directive already resolved for this actor's own failure. Returns {@code false} if
   * this actor is now stopping (and must not enter, or continue, its dispatch loop).
   *
   * <p>{@code RESTART} replaces the actor instance and calls {@code preStart} on it; if that itself
   * throws, the resulting failure is resolved and applied the same way, iteratively (not
   * recursively, so a persistently-failing {@code preStart} that always resolves to {@code RESTART}
   * loops in place rather than growing the call stack — see {@code
   * docs/decisions/ADR-008-supervision-strategies-and-actor-hierarchies.md}).
   */
  private boolean applyDirective(SupervisorDirective directive) {
    SupervisorDirective current = directive;
    while (true) {
      switch (current) {
        case RESUME -> {
          return true;
        }
        case RESTART -> {
          replaceActorInstance();
          try {
            actor.preStart(context);
            return true;
          } catch (Throwable t) {
            current = reportFailureUp(t, null);
          }
        }
        case STOP, ESCALATE -> {
          requestStop();
          return false;
        }
      }
    }
  }

  private void replaceActorInstance() {
    // Fire-and-forget: do not wait for children to finish stopping. Waiting here can deadlock —
    // see docs/decisions/ADR-008-supervision-strategies-and-actor-hierarchies.md §3.
    for (ActorCell<?> child : children) {
      child.requestStop();
    }
    actor = factory.get();
  }

  /** Delivers a child's failure report into this actor's own mailbox, to be resolved in turn. */
  private boolean deliverChildFailure(
      ActorCell<?> child, Throwable failure, CompletableFuture<SupervisorDirective> decision) {
    return mailbox.offer(new Envelope.ChildFailure<>(child, failure, decision));
  }

  /** Runs on this actor's own dispatch thread: resolves a child's failure report. */
  private void resolveChildFailure(Envelope.ChildFailure<T> childFailure) {
    SupervisorDirective directive;
    try {
      directive = actor.supervisorStrategy(childFailure.failure());
    } catch (Throwable t) {
      LOG.log(
          Level.WARNING,
          "Actor '"
              + id
              + "' threw from supervisorStrategy(); defaulting to STOP for '"
              + childFailure.child().id()
              + "'.",
          t);
      directive = SupervisorDirective.STOP;
    }
    childFailure
        .decision()
        .complete(directive == SupervisorDirective.ESCALATE ? SupervisorDirective.STOP : directive);
    if (directive == SupervisorDirective.ESCALATE) {
      applyDirective(reportFailureUp(childFailure.failure(), null));
    }
  }

  /**
   * Called for each envelope still queued when this actor's mailbox closes. A queued {@link
   * Envelope.ChildFailure} would otherwise leave its reporting child blocked forever waiting for a
   * supervisor that is now gone; resolve it to {@code STOP} instead.
   */
  private void resolveDiscardedEnvelope(Envelope<T> envelope) {
    if (envelope instanceof Envelope.ChildFailure<T> childFailure) {
      childFailure.decision().complete(SupervisorDirective.STOP);
    }
  }

  private void awaitChildrenTerminated() {
    for (ActorCell<?> child : children) {
      child.awaitTermination();
    }
  }

  private void finishTermination() {
    try {
      actor.postStop(context);
    } catch (Throwable t) {
      LOG.log(Level.WARNING, "Actor '" + id + "' threw from postStop(); ignoring.", t);
    }
    terminated.set(true);
    terminationSignal.complete(null);
    if (parent != null) {
      parent.removeChild(this);
    }
    system.deregister(this);
  }

  private static <V> V joinUninterruptibly(CompletableFuture<V> future) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return future.get();
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (ExecutionException e) {
          throw new AssertionError("should never complete exceptionally", e);
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private final class ActorRefImpl implements ActorRef<T> {
    @Override
    public void tell(T message) {
      if (terminated.get()) {
        return;
      }
      mailbox.offer(new Envelope.Message<>(message));
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
    public Optional<ActorRef<?>> parent() {
      return parent == null ? Optional.empty() : Optional.of(parent.ref());
    }

    @Override
    public <C> ActorRef<C> spawn(Supplier<Actor<C>> factory, String name) {
      return system.spawnChild(ActorCell.this, factory, name);
    }

    @Override
    public <C> ActorRef<C> spawn(Supplier<Actor<C>> factory) {
      return system.spawnChild(ActorCell.this, factory, null);
    }
  }
}
