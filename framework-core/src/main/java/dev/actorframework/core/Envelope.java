package dev.actorframework.core;

import java.util.concurrent.CompletableFuture;

/**
 * Internal mailbox payload for an {@link ActorCell}: either a real message sent via {@link
 * ActorRef#tell}, or a system signal reporting that one of this actor's children failed (TASK-402).
 * Folding the signal into the same mailbox as user messages means it wakes a dispatch thread parked
 * in {@link Mailbox#take()} exactly like a real message would, and is resolved in the same FIFO
 * order — see {@code docs/decisions/ADR-008-supervision-strategies-and-actor-hierarchies.md}.
 */
sealed interface Envelope<T> {

  record Message<T>(T value) implements Envelope<T> {}

  record ChildFailure<T>(
      ActorCell<?> child, Throwable failure, CompletableFuture<SupervisorDirective> decision)
      implements Envelope<T> {}
}
