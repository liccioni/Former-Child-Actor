package dev.actorframework.core;

/**
 * Converts a message of type {@code T} to and from bytes for durable journaling (TASK-601).
 *
 * <p>{@link JournalStore}/{@link Journal} operate on raw bytes only and know nothing about {@code
 * T} — this is the only place serialization is decided, supplied by whoever opts an actor into
 * persistence via {@link ActorSystem#spawn(java.util.function.Supplier, String, MessageCodec)}. See
 * {@code docs/decisions/ADR-016-durable-actor-journal-and-recovery.md}.
 */
public interface MessageCodec<T> {

  /** Encodes a message to bytes to be appended to a {@link Journal}. */
  byte[] encode(T message);

  /**
   * Decodes bytes previously produced by {@link #encode} back into a message.
   *
   * <p>Thrown exceptions are treated as a corrupt/undecodable record: a fatal recovery error that
   * stops the actor immediately, without consulting its {@link SupervisorStrategy} (TASK-601).
   */
  T decode(byte[] bytes);
}
