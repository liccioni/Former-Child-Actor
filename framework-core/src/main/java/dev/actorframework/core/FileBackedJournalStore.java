package dev.actorframework.core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The local file-backed default {@link JournalStore} (TASK-601): one file per actor id under {@code
 * root}, named by sanitizing the id ({@code "/" -> "_"}, since ids may contain {@code "/"} for
 * child namespacing). Sanitization collisions (e.g. {@code "a/b"} and {@code "a_b"}) are an
 * accepted limitation, documented not solved — see {@code
 * docs/decisions/ADR-016-durable-actor-journal-and-recovery.md}.
 */
final class FileBackedJournalStore implements JournalStore {

  private final Path root;

  FileBackedJournalStore(Path root) {
    this.root = root;
  }

  @Override
  public Journal open(String actorId) {
    try {
      Files.createDirectories(root);
      Path file = root.resolve(sanitize(actorId));
      return new FileBackedJournal(new RandomAccessFile(file.toFile(), "rw"));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String sanitize(String actorId) {
    return actorId.replace('/', '_');
  }
}
