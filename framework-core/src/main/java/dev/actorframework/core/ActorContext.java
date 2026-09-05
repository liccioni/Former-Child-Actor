package dev.actorframework.core;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Context made available to an {@link Actor} while it is starting, processing a message, or
 * stopping.
 *
 * <p>Exposes a reference to itself, to the owning system, to its parent (if any), and lets it spawn
 * children of its own (TASK-402) — see {@code
 * docs/decisions/ADR-008-supervision-strategies-and-actor-hierarchies.md}.
 */
public interface ActorContext<T> {

  /** The {@link ActorRef} of the actor this context belongs to. */
  ActorRef<T> self();

  /** The {@link ActorSystem} this actor runs in. */
  ActorSystem system();

  /**
   * This actor's supervisor, if any (TASK-402) — the actor that spawned it via {@link #spawn}.
   * Empty for an actor spawned directly via {@link ActorSystem#spawn}.
   */
  Optional<ActorRef<?>> parent();

  /**
   * Spawns a new child actor of this actor, with the given name (TASK-402). The child's id is this
   * actor's id, followed by {@code /}, followed by {@code name}; this actor becomes the child's
   * supervisor (see {@link Actor#supervisorStrategy}). The child is stopped automatically if this
   * actor stops or restarts, or if the owning system shuts down.
   *
   * @throws IllegalArgumentException if this actor already has a child with this name
   * @throws IllegalStateException if the owning system is shutting down
   */
  <C> ActorRef<C> spawn(Supplier<Actor<C>> factory, String name);

  /**
   * Spawns a new child actor of this actor with an automatically generated name. See {@link
   * #spawn(Supplier, String)}.
   */
  <C> ActorRef<C> spawn(Supplier<Actor<C>> factory);
}
