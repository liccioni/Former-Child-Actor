package dev.actorframework.core;

/**
 * A stable, thread-safe reference used to send messages to an actor.
 *
 * <p>An {@code ActorRef} never exposes the actor's internal state directly; the only way to
 * interact with an actor is by sending it a message (TASK-102).
 *
 * <h2>Termination semantics (TASK-107)</h2>
 *
 * <p>Once the actor behind this reference has stopped (whether by explicit stop request or by an
 * uncaught exception, see {@link Actor}), the reference itself remains valid but every subsequent
 * {@link #tell} is silently dropped rather than delivered or throwing. Messages that were already
 * queued but not yet processed at the moment the actor stopped are discarded, not delivered. {@link
 * #isTerminated()} lets callers observe this.
 */
public interface ActorRef<T> {

  /**
   * Sends a message to this actor's mailbox.
   *
   * <p>Delivery is asynchronous and fire-and-forget: this method does not wait for the message to
   * be processed. If the actor has already terminated, the message is silently dropped. If the
   * mailbox is full, the calling thread blocks until space is available or the actor terminates
   * (see the provisional mailbox-bounds decision in {@code docs/decisions/}).
   */
  void tell(T message);

  /** A stable identifier for this actor, unique within its {@link ActorSystem}. */
  String id();

  /** Whether the actor behind this reference has stopped. */
  boolean isTerminated();
}
