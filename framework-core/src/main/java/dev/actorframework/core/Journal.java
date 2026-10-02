package dev.actorframework.core;

import java.util.List;

/**
 * The durable, append-only record of one actor's journaled messages, in the order they were
 * appended (TASK-601).
 *
 * <p><b>Ownership:</b> a {@code Journal} instance is opened once (via {@link
 * JournalStore#open(String)}) and touched only by its actor's own dispatcher thread for the rest of
 * its lifetime — {@link #append} happens inline in the actor's dispatch loop, {@link #readAll}
 * happens once, before the live loop starts. No internal synchronization is required or provided,
 * the same ownership discipline as {@code Mailbox}'s actor-owned state.
 *
 * <p>{@link #append} and {@link #readAll} declare no checked exceptions; an implementation wraps
 * any I/O failure in {@link java.io.UncheckedIOException}. Such a failure is treated as fatal to
 * the owning actor, not arbitrated by its {@link SupervisorStrategy} — see {@code
 * docs/decisions/ADR-016-durable-actor-journal-and-recovery.md}.
 */
public interface Journal extends AutoCloseable {

  /** Durably appends one record. */
  void append(byte[] record);

  /** Returns every record appended so far, in append order. */
  List<byte[]> readAll();

  /** Releases this journal's resources (e.g. its open file handle). A no-op by default. */
  @Override
  default void close() {}
}
