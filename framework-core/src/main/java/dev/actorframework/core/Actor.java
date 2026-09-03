package dev.actorframework.core;

/**
 * A stateful unit of behavior that processes messages of type {@code T} sequentially.
 *
 * <p>An actor never has two invocations of {@link #onMessage} running concurrently for the same
 * instance; the runtime guarantees at most one in-flight message per actor (TASK-106). Actor state
 * may therefore be held in plain (non-volatile, non-synchronized) fields.
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
}
