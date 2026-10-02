package dev.actorframework.core;

import java.nio.file.Path;

/**
 * A pluggable, per-actor-system source of {@link Journal}s, keyed by actor id (TASK-601).
 *
 * <p>One {@code JournalStore} configures a whole {@link ActorSystem} (via {@link
 * ActorSystem#start(String, JournalStore)}); an actor opts into it by being spawned through {@link
 * ActorSystem#spawn(java.util.function.Supplier, String, MessageCodec)}. See {@code
 * docs/decisions/ADR-016-durable-actor-journal-and-recovery.md}.
 */
public interface JournalStore extends AutoCloseable {

  /**
   * Opens the {@link Journal} for the given actor id, creating it if it does not yet exist. The
   * returned {@code Journal} is owned by that actor's dispatcher thread for the rest of its
   * lifetime — see {@link Journal}.
   */
  Journal open(String actorId);

  /** Releases any resources held by this store as a whole. A no-op by default. */
  @Override
  default void close() {}

  /**
   * The local, file-backed default {@link JournalStore}: one length-prefixed record file per actor
   * id under {@code root}, which is always explicit (no implicit default directory, matching {@code
   * docs/architecture.md} §8's "explicit behavior" principle).
   */
  static JournalStore fileBacked(Path root) {
    return new FileBackedJournalStore(root);
  }
}
