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
}
