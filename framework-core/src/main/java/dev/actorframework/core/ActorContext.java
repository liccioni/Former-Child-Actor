package dev.actorframework.core;

/**
 * Context made available to an {@link Actor} while it is starting, processing a message, or
 * stopping.
 *
 * <p>Kept deliberately small for M1: a reference to itself and to the owning system. Actor
 * hierarchies (parent/child spawning from within a running actor) are introduced in M4 (TASK-403)
 * and are not part of this context yet.
 */
public interface ActorContext<T> {

  /** The {@link ActorRef} of the actor this context belongs to. */
  ActorRef<T> self();

  /** The {@link ActorSystem} this actor runs in. */
  ActorSystem system();
}
