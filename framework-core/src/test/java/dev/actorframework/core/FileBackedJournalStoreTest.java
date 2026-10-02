package dev.actorframework.core;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** TASK-601: the local file-backed default {@link JournalStore}/{@link Journal}. */
class FileBackedJournalStoreTest {

  @Test
  void appendedRecordsSurviveCloseAndReopen(@TempDir Path tempDir) {
    JournalStore store = JournalStore.fileBacked(tempDir);
    try (Journal journal = store.open("actor-1")) {
      journal.append("first".getBytes(UTF_8));
      journal.append("second".getBytes(UTF_8));
    }

    try (Journal reopened = store.open("actor-1")) {
      List<byte[]> records = reopened.readAll();
      assertEquals(2, records.size());
      assertEquals("first", new String(records.get(0), UTF_8));
      assertEquals("second", new String(records.get(1), UTF_8));
    }
  }

  @Test
  void actorIdsContainingSlashesAreSanitizedIntoSafeFilenames(@TempDir Path tempDir) {
    JournalStore store = JournalStore.fileBacked(tempDir);
    try (Journal journal = store.open("parent/child")) {
      journal.append("payload".getBytes(UTF_8));
    }

    assertTrue(Files.exists(tempDir.resolve("parent_child")));
  }

  @Test
  void concurrentActorsGetIsolatedFiles(@TempDir Path tempDir) throws InterruptedException {
    JournalStore store = JournalStore.fileBacked(tempDir);
    int actorCount = 8;
    int messagesPerActor = 50;
    CountDownLatch done = new CountDownLatch(actorCount);
    for (int i = 0; i < actorCount; i++) {
      int actorIndex = i;
      Thread writer =
          new Thread(
              () -> {
                try (Journal journal = store.open("actor-" + actorIndex)) {
                  for (int m = 0; m < messagesPerActor; m++) {
                    journal.append(("actor-" + actorIndex + "-msg-" + m).getBytes(UTF_8));
                  }
                } finally {
                  done.countDown();
                }
              });
      writer.start();
    }
    assertTrue(done.await(10, TimeUnit.SECONDS));

    for (int i = 0; i < actorCount; i++) {
      try (Journal journal = store.open("actor-" + i)) {
        List<byte[]> records = journal.readAll();
        assertEquals(messagesPerActor, records.size());
        for (int m = 0; m < messagesPerActor; m++) {
          assertEquals("actor-" + i + "-msg-" + m, new String(records.get(m), UTF_8));
        }
      }
    }
  }
}
