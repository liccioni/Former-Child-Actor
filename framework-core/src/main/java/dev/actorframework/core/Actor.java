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
   * Called once before the first message is processed, and again after a {@link Directive#RESTART}
   * (on the fresh instance, before it takes over).
   *
   * <p>A failure thrown here is handled the same way a failure from {@link #onMessage} is
   * (TASK-402: by this actor's {@link SupervisorStrategy}) — for a top-level actor, or whenever the
   * resulting directive is {@link Directive#STOP} or {@link Directive#ESCALATE}, that means:
   * logged, and the actor stops without processing any message.
   */
  default void preStart(ActorContext<T> context) throws Exception {}

  /**
   * Processes a single message. Invocations are strictly sequential per actor instance.
   *
   * <p>An uncaught exception here is handled by this actor's {@link SupervisorStrategy} (TASK-402);
   * it never affects sibling actors or the {@link ActorSystem} directly (only an {@link
   * Directive#ESCALATE} decision propagates further, and only up this actor's own ancestor chain).
   */
  void onMessage(ActorContext<T> context, T message) throws Exception;

  /**
   * Called once after the actor has stopped, whether due to a normal stop request or an uncaught
   * exception. Best-effort: an exception thrown here is logged and otherwise ignored, since the
   * actor is already terminating.
   */
  default void postStop(ActorContext<T> context) throws Exception {}

  /**
   * Called on the old instance just before a {@link Directive#RESTART} replaces it with a fresh
   * one, given the failure that triggered the restart and the message being processed when it threw
   * ({@code null} if the failure was in {@link #preStart}). Best-effort: an exception thrown here
   * is logged and otherwise ignored — the old instance is being discarded regardless.
   */
  default void preRestart(ActorContext<T> context, Throwable failure, T message) throws Exception {}

  /**
   * Called on the fresh instance right after a {@link Directive#RESTART}, once its own {@link
   * #preStart} has succeeded, given the failure that triggered the restart. Best-effort: an
   * exception thrown here is logged and otherwise ignored.
   */
  default void postRestart(ActorContext<T> context, Throwable failure) throws Exception {}
}
