package dev.actorframework.core;

import java.util.function.Supplier;

/**
 * Context made available to an {@link Actor} while it is starting, processing a message, or
 * stopping.
 *
 * <p>Actor hierarchies (TASK-402): an actor may spawn children of its own via {@link #spawnChild},
 * becoming their supervisor. A child's {@link SupervisorStrategy} decides what happens when it
 * fails; see {@code docs/decisions/ADR-008-supervision-strategies-and-hierarchies.md} for full
 * semantics, including how a stop or an {@link Directive#ESCALATE} decision propagates through the
 * hierarchy.
 *
 * <p>Death watch (M6): an actor may watch any other actor in the same {@link ActorSystem} — not
 * only its own children — via {@link #watch}, to be notified when it terminates. See {@code
 * docs/decisions/ADR-016-death-watch.md} for full semantics.
 */
public interface ActorContext<T> {

  /** The {@link ActorRef} of the actor this context belongs to. */
  ActorRef<T> self();

  /** The {@link ActorSystem} this actor runs in. */
  ActorSystem system();

  /**
   * Spawns a child actor supervised by this actor, using {@link SupervisorStrategy#stop()} — i.e.
   * the child behaves exactly like a top-level actor (TASK-107a's default) unless a different
   * strategy is asked for via {@link #spawnChild(Supplier, String, SupervisorStrategy)}.
   *
   * @throws IllegalArgumentException if this actor already has a child with this name
   * @throws IllegalStateException if this actor (or its {@link ActorSystem}) is stopping
   */
  <C> ActorRef<C> spawnChild(Supplier<Actor<C>> factory, String name);

  /**
   * Spawns a child actor supervised by this actor with an explicit {@link SupervisorStrategy},
   * consulted whenever the child fails.
   *
   * @throws IllegalArgumentException if this actor already has a child with this name
   * @throws IllegalStateException if this actor (or its {@link ActorSystem}) is stopping
   */
  <C> ActorRef<C> spawnChild(Supplier<Actor<C>> factory, String name, SupervisorStrategy strategy);

  /**
   * Watches {@code target}: enqueues {@code onTerminated} to this actor's own mailbox, exactly
   * once, the moment {@code target} terminates for any reason (an explicit stop, an uncaught
   * failure whose {@link SupervisorStrategy} decides {@link Directive#STOP} or {@link
   * Directive#ESCALATE}, or a supervision/shutdown cascade reaching it) — delivered like any other
   * message, processed by a later {@link Actor#onMessage} call, never re-entrantly. A {@link
   * Directive#RESTART} never fires this: the actor's identity (its {@link ActorRef}) survives a
   * restart untouched.
   *
   * <p>If {@code target} has already terminated — or never resolves to a live actor in this system
   * at all — by the time this is called, {@code onTerminated} is still enqueued, just as promptly
   * as any other {@link ActorRef#tell}. Watching the same target again before the first watch has
   * fired replaces the pending message rather than queuing a second delivery.
   *
   * <p>Delivery reuses {@link ActorRef#tell}'s existing contract: if this actor itself has already
   * stopped by the time delivery would happen, {@code onTerminated} is silently dropped, same as
   * any other message to a terminated ref. In particular, an actor watching itself never receives
   * the notification — its own mailbox is already closed by the time it finishes terminating — but
   * still terminates cleanly.
   *
   * @see #unwatch
   */
  void watch(ActorRef<?> target, T onTerminated);

  /**
   * Cancels a previously registered {@link #watch}. A no-op if this actor is not currently watching
   * {@code target}, or if the watch already fired (delivery already in flight cannot be recalled).
   */
  void unwatch(ActorRef<?> target);
}
