package dev.actorframework.core;

/**
 * A stateful unit of behavior that processes messages of type {@code T} sequentially.
 *
 * <p>An actor never has two invocations of {@link #onMessage} running concurrently for the same
 * instance; the runtime guarantees at most one in-flight message per actor (TASK-106). Actor state
 * may therefore be held in plain (non-volatile, non-synchronized) fields — see the Java Memory
 * Model review in {@code docs/decisions/ADR-005-jmm-review-sequential-processing.md} (TASK-207) for
 * why this holds.
 *
 * <p><b>Do not mutate a message after sending it.</b> The mailbox guarantees that state a sender
 * established <em>before</em> calling {@link ActorRef#tell} is visible inside {@link #onMessage}
 * when that message is processed; it makes no guarantee about further mutation of a message object
 * after {@code tell()} returns. Treat a message as handed off, not shared, once sent — prefer
 * immutable messages (records are a natural fit).
 */
public interface Actor<T> {

  /**
   * Called once before the first message is processed.
   *
   * <p>A failure thrown here is treated the same as a failure thrown from {@link #onMessage}
   * (TASK-107a): it is logged and the actor is stopped without processing any message.
   */
  default void preStart(ActorContext<T> context) throws Exception {}

  /**
   * Processes a single message. Invocations are strictly sequential per actor instance.
   *
   * <p>An uncaught exception here stops this actor and this actor alone (TASK-107a); it never
   * affects sibling actors or the {@link ActorSystem}.
   */
  void onMessage(ActorContext<T> context, T message) throws Exception;

  /**
   * Called once after the actor has stopped, whether due to a normal stop request or an uncaught
   * exception. Best-effort: an exception thrown here is logged and otherwise ignored, since the
   * actor is already terminating.
   */
  default void postStop(ActorContext<T> context) throws Exception {}

  /**
   * Decides what happens to a child of this actor that failed with {@code failure} (TASK-402).
   *
   * <p>Called on this actor's own dispatch thread — in strict turn with its own messages, never
   * concurrently with {@link #onMessage} — whenever a child spawned via {@link ActorContext#spawn}
   * throws from its {@link #preStart} or {@link #onMessage}. This makes it safe to read and write
   * this actor's own fields here, the same as anywhere else in this actor (see {@code
   * docs/decisions/ADR-005-jmm-review-sequential-processing.md} and {@code
   * docs/decisions/ADR-008-supervision-strategies-and-actor-hierarchies.md}).
   *
   * <p>Defaults to {@link SupervisorDirective#STOP}, matching the fixed M1 default (TASK-107a) for
   * any actor that does not override this. An actor with no parent of its own (spawned via {@link
   * ActorSystem#spawn}) always resolves its <em>own</em> failure to {@code STOP} — this method is
   * only ever consulted about a failing child, never about this actor itself.
   */
  default SupervisorDirective supervisorStrategy(Throwable failure) {
    return SupervisorDirective.STOP;
  }
}
