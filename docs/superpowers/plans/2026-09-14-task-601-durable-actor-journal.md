# TASK-601: Durable Actor Journal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in, per-actor durable persistence to `framework-core`: a pluggable
byte-level `JournalStore`/`Journal` abstraction, a `MessageCodec<T>` for message
serialization, a local file-backed default store, and `ActorCell`/`ActorSystem` wiring
that replays a persistent actor's journal via `onMessage` on spawn before falling
through to live mailbox processing, with write-ahead durability.

**Architecture:** One unified `ActorCell.dispatchLoop()` drains a journal-replay
iterator first, then the mailbox — no separate replay code path, so all four
`SupervisorStrategy` directives and the existing poison-message guarantee (ADR-004)
apply unchanged to both live and replayed messages. Persistence is opt-in per actor via
a new `ActorSystem.spawn(factory, name, codec)` overload; existing `spawn` overloads and
`spawnChild` are untouched. A local file-backed `JournalStore` stores one
length-prefixed record file per actor id under a configured root directory.

**Tech Stack:** Java 25, Gradle (Kotlin DSL), JUnit 5 (`@TempDir` for real-file tests),
`java.io.RandomAccessFile` for the file-backed journal (no external dependency).

**Spec:** `docs/superpowers/specs/2026-09-08-task-601-durable-actor-journal-design.md`

## Global Constraints

* `framework-core` has zero dependency on any other module — everything in this plan
  lives inside `framework-core`.
* Every public type/method gets a Javadoc comment referencing the TASK/ADR that explains
  *why*, per `docs/coding-standards.md`.
* Package-private, flat package (`dev.actorframework.core`) — no `.internal`
  subpackage, matching existing `ActorCell`/`Mailbox`/`Dispatcher`.
* Non-persistent actors (existing `spawn(factory)` / `spawn(factory, name)` /
  `spawnChild`) must be byte-for-byte unaffected — `ActorFailureTest`,
  `ActorSystemTest`, `SupervisionTest`, `AskTest` need zero edits. This is the
  regression signal, verified in Task 4.
* `JournalStore`/`Journal` operate on raw bytes only; `append`/`readAll`/`close` are
  unchecked (no `throws` clause) — any I/O failure is wrapped in
  `java.io.UncheckedIOException`.
* No internal synchronization in `Journal` or its file-backed implementation — a given
  `Journal` instance is touched only by its actor's own dispatcher thread, same
  ownership discipline as `Mailbox`'s actor-owned state.
* Concurrency guarantees (here: isolated per-actor files under concurrent spawns) are
  tested with real concurrent load, per `AGENTS.md`.
* This repo enforces `google-java-format` via Spotless, and `spotlessCheck` is wired into
  each module's `check` task, which `build` depends on — a file with the wrong import
  order or spacing fails `./gradlew build` even if the code is otherwise correct. Run
  `./gradlew spotlessApply` after creating or editing any file, before running tests or
  `./gradlew build`.
* Run `./gradlew build` before considering any task done.

---

## Design recap (from the spec — read in full before starting)

```java
public interface MessageCodec<T> {
  byte[] encode(T message);
  T decode(byte[] bytes);
}

public interface JournalStore extends AutoCloseable {
  Journal open(String actorId);
  default void close() {}
}

public interface Journal extends AutoCloseable {
  void append(byte[] record);
  List<byte[]> readAll();
  default void close() {}
}
```

Unified dispatch loop (`ActorCell`):

```java
private void dispatchLoop() {
  Iterator<T> recovery = journal != null ? replayIterator() : Collections.emptyIterator();
  while (true) {
    T message;
    boolean fromJournal = recovery.hasNext();
    if (fromJournal) {
      message = recovery.next();
    } else {
      message = mailbox.take();
      if (message == null) return;
      if (journal != null) {
        journal.append(codec.encode(message)); // write-ahead: before onMessage runs
      }
    }
    try {
      actor.onMessage(context, message);
    } catch (Throwable t) {
      if (!handleFailureAndDecideContinue(t, message)) return;
    }
  }
}
```

`handleFailureAndDecideContinue` needs no changes — this is what makes the ADR-004
(poison message) and ADR-008 (`Restart`) reconciliation fall out for free. A corrupt
record (`codec.decode()` throwing) or an I/O failure (`append`/`readAll` throwing) is a
**fatal recovery error**, not an actor failure: logged, actor stops immediately,
`SupervisorStrategy` is never consulted.

---

## File structure

| File | Responsibility |
|---|---|
| `framework-core/src/main/java/dev/actorframework/core/MessageCodec.java` | New public interface: message ↔ bytes |
| `framework-core/src/main/java/dev/actorframework/core/JournalStore.java` | New public interface: per-actor `Journal` factory + `fileBacked(Path)` static factory |
| `framework-core/src/main/java/dev/actorframework/core/Journal.java` | New public interface: append/readAll/close on raw bytes |
| `framework-core/src/main/java/dev/actorframework/core/FileBackedJournalStore.java` | New package-private: opens one `RandomAccessFile`-backed `Journal` per sanitized actor id under a root directory |
| `framework-core/src/main/java/dev/actorframework/core/FileBackedJournal.java` | New package-private: length-prefixed record file, backed by one `RandomAccessFile` |
| `framework-core/src/main/java/dev/actorframework/core/ActorCell.java` | Modify: `journal`/`codec` fields, unified `dispatchLoop`, journal close in `finishTermination` |
| `framework-core/src/main/java/dev/actorframework/core/ActorSystem.java` | Modify: `store` field, `start(name, store)`, persistent `spawn` overload, `close()` closes the store |
| `framework-core/src/test/java/dev/actorframework/core/FileBackedJournalStoreTest.java` | New: file-backed store behavior (round trip, sanitization, concurrent isolation) |
| `framework-core/src/test/java/dev/actorframework/core/PersistentActorTest.java` | New: `ActorCell`/`ActorSystem`-level persistence behavior (replay, write-ahead, poison-on-replay, restart-during-replay, fatal recovery errors, no-store error) |
| `docs/decisions/ADR-016-durable-actor-journal-and-recovery.md` | New ADR |
| `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md` | Modify: add "Resolved at TASK-601" addendum |
| `docs/decisions/ADR-008-supervision-strategies-and-hierarchies.md` | Modify: add "Resolved at TASK-601" addendum |
| `docs/architecture.md` | Modify: new §10 "Persistence (M6)"; current-milestone line bumped to M6 |

---

## Task 1: `MessageCodec`/`JournalStore`/`Journal` interfaces and the file-backed default store

**Files:**
- Create: `framework-core/src/main/java/dev/actorframework/core/MessageCodec.java`
- Create: `framework-core/src/main/java/dev/actorframework/core/JournalStore.java`
- Create: `framework-core/src/main/java/dev/actorframework/core/Journal.java`
- Create: `framework-core/src/main/java/dev/actorframework/core/FileBackedJournalStore.java`
- Create: `framework-core/src/main/java/dev/actorframework/core/FileBackedJournal.java`
- Test: `framework-core/src/test/java/dev/actorframework/core/FileBackedJournalStoreTest.java`

**Interfaces:**
- Produces: `MessageCodec<T>` (`byte[] encode(T)`, `T decode(byte[])`); `JournalStore`
  (`Journal open(String actorId)`, `void close()`, `static JournalStore fileBacked(Path root)`);
  `Journal` (`void append(byte[])`, `List<byte[]> readAll()`, `void close()`). These three
  types are consumed by Task 2 (`ActorCell`) and Task 3 (`ActorSystem`).

- [ ] **Step 1: Write the failing test — round trip through the file-backed store**

Create `framework-core/src/test/java/dev/actorframework/core/FileBackedJournalStoreTest.java`:

```java
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :framework-core:test --tests FileBackedJournalStoreTest`
Expected: compilation FAILURE — `JournalStore`, `Journal` do not exist yet.

- [ ] **Step 3: Create the three interfaces**

Create `framework-core/src/main/java/dev/actorframework/core/MessageCodec.java`:

```java
package dev.actorframework.core;

/**
 * Converts a message of type {@code T} to and from bytes for durable journaling (TASK-601).
 *
 * <p>{@link JournalStore}/{@link Journal} operate on raw bytes only and know nothing about
 * {@code T} — this is the only place serialization is decided, supplied by whoever opts an actor
 * into persistence via {@link ActorSystem#spawn(java.util.function.Supplier, String,
 * MessageCodec)}. See {@code docs/decisions/ADR-016-durable-actor-journal-and-recovery.md}.
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
```

Create `framework-core/src/main/java/dev/actorframework/core/JournalStore.java`:

```java
package dev.actorframework.core;

import java.nio.file.Path;

/**
 * A pluggable, per-actor-system source of {@link Journal}s, keyed by actor id (TASK-601).
 *
 * <p>One {@code JournalStore} configures a whole {@link ActorSystem} (via {@link
 * ActorSystem#start(String, JournalStore)}); an actor opts into it by being spawned through
 * {@link ActorSystem#spawn(java.util.function.Supplier, String, MessageCodec)}. See {@code
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
   * The local, file-backed default {@link JournalStore}: one length-prefixed record file per
   * actor id under {@code root}, which is always explicit (no implicit default directory,
   * matching {@code docs/architecture.md} §8's "explicit behavior" principle).
   */
  static JournalStore fileBacked(Path root) {
    return new FileBackedJournalStore(root);
  }
}
```

Create `framework-core/src/main/java/dev/actorframework/core/Journal.java`:

```java
package dev.actorframework.core;

import java.util.List;

/**
 * The durable, append-only record of one actor's journaled messages, in the order they were
 * appended (TASK-601).
 *
 * <p><b>Ownership:</b> a {@code Journal} instance is opened once (via {@link
 * JournalStore#open(String)}) and touched only by its actor's own dispatcher thread for the rest
 * of its lifetime — {@link #append} happens inline in the actor's dispatch loop, {@link #readAll}
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
```

- [ ] **Step 4: Implement the file-backed store**

Create `framework-core/src/main/java/dev/actorframework/core/FileBackedJournalStore.java`:

```java
package dev.actorframework.core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The local file-backed default {@link JournalStore} (TASK-601): one file per actor id under
 * {@code root}, named by sanitizing the id ({@code "/" -> "_"}, since ids may contain {@code "/"}
 * for child namespacing). Sanitization collisions (e.g. {@code "a/b"} and {@code "a_b"}) are an
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
```

Create `framework-core/src/main/java/dev/actorframework/core/FileBackedJournal.java`:

```java
package dev.actorframework.core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link Journal} backed by one {@link RandomAccessFile}, kept open for the actor's entire
 * lifetime (TASK-601). Format: sequential records, each a 4-byte big-endian length header
 * followed by that many payload bytes, repeated to EOF — minimal, streamable, no external
 * dependency.
 */
final class FileBackedJournal implements Journal {

  private final RandomAccessFile file;

  FileBackedJournal(RandomAccessFile file) {
    this.file = file;
  }

  @Override
  public void append(byte[] record) {
    try {
      file.seek(file.length());
      file.writeInt(record.length);
      file.write(record);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public List<byte[]> readAll() {
    try {
      List<byte[]> records = new ArrayList<>();
      file.seek(0);
      long length = file.length();
      while (file.getFilePointer() < length) {
        int recordLength = file.readInt();
        byte[] record = new byte[recordLength];
        file.readFully(record);
        records.add(record);
      }
      return records;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() {
    try {
      file.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `./gradlew :framework-core:spotlessApply :framework-core:test --tests FileBackedJournalStoreTest`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add framework-core/src/main/java/dev/actorframework/core/MessageCodec.java \
        framework-core/src/main/java/dev/actorframework/core/JournalStore.java \
        framework-core/src/main/java/dev/actorframework/core/Journal.java \
        framework-core/src/main/java/dev/actorframework/core/FileBackedJournalStore.java \
        framework-core/src/main/java/dev/actorframework/core/FileBackedJournal.java \
        framework-core/src/test/java/dev/actorframework/core/FileBackedJournalStoreTest.java
git commit -m "TASK-601: add MessageCodec/JournalStore/Journal and the file-backed default store"
```

---

## Task 2: `ActorCell` — unified dispatch loop with journal replay and write-ahead journaling

**Files:**
- Modify: `framework-core/src/main/java/dev/actorframework/core/ActorCell.java`
- Test: `framework-core/src/test/java/dev/actorframework/core/PersistentActorTest.java` (created
  here, extended in Task 3)

**Interfaces:**
- Consumes: `Journal` (`append(byte[])`, `readAll(): List<byte[]>`), `MessageCodec<T>`
  (`encode(T): byte[]`, `decode(byte[]): T`) from Task 1.
- Produces: `ActorCell(ActorSystem system, String id, Supplier<Actor<T>> factory, ActorCell<?>
  parent, SupervisorStrategy strategy, Journal journal, MessageCodec<T> codec)` — the new
  7-argument package-private constructor Task 3's `ActorSystem` calls (passing `null, null` for
  non-persistent actors, and a real `Journal`/`MessageCodec<T>` for persistent ones).

This task modifies `ActorCell` directly and proves it via tests that construct `ActorCell`
directly (package-private, same-package access per `docs/coding-standards.md`), bypassing
`ActorSystem.spawn` — this is the only way to test the `Restart`-during-replay and
fatal-recovery-error paths in isolation, and to prove write-ahead ordering deterministically with
a fake in-memory `Journal`.

- [ ] **Step 1: Write the failing tests**

Create `framework-core/src/test/java/dev/actorframework/core/PersistentActorTest.java`:

```java
package dev.actorframework.core;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * TASK-601: durable per-actor journaling and recovery on restart. Persistence-facing
 * {@link ActorSystem} API (the real end-to-end file-backed round trip, the no-store error, and
 * confirmation that non-persistent actors are untouched) is covered in the second half of this
 * file, added in Task 3.
 */
class PersistentActorTest {

  @Test
  void aLiveMessageIsJournaledBeforeItsOnMessageRuns() throws InterruptedException {
    RecordingJournal journal = new RecordingJournal();
    StringCodec codec = new StringCodec();
    CountDownLatch onMessageEntered = new CountDownLatch(1);
    CountDownLatch proceed = new CountDownLatch(1);
    AtomicInteger recordsSeenInsideOnMessage = new AtomicInteger(-1);

    try (ActorSystem system = ActorSystem.start("test")) {
      ActorCell<String> cell =
          new ActorCell<>(
              system,
              "blocking-actor",
              () ->
                  (context, message) -> {
                    recordsSeenInsideOnMessage.set(journal.size());
                    onMessageEntered.countDown();
                    proceed.await();
                  },
              null,
              SupervisorStrategy.stop(),
              journal,
              codec);
      Thread dispatcherThread = new Thread(cell::run);
      dispatcherThread.start();

      cell.ref().tell("hello");
      assertTrue(onMessageEntered.await(2, TimeUnit.SECONDS));
      assertEquals(1, recordsSeenInsideOnMessage.get());

      proceed.countDown();
      dispatcherThread.join(Duration.ofSeconds(2).toMillis());
    }
  }

  @Test
  void aPoisonJournaledMessageHaltsRecoveryAndFailsIdenticallyOnEveryRetry()
      throws InterruptedException {
    StringCodec codec = new StringCodec();
    RecordingJournal journal = new RecordingJournal(codec.encode("boom"));

    for (int attempt = 0; attempt < 3; attempt++) {
      try (ActorSystem system = ActorSystem.start("test")) {
        ActorCell<String> cell =
            new ActorCell<>(
                system,
                "poison-actor",
                () ->
                    (context, message) -> {
                      throw new RuntimeException("boom");
                    },
                null,
                SupervisorStrategy.stop(),
                journal,
                codec);
        Thread dispatcherThread = new Thread(cell::run);
        dispatcherThread.start();

        awaitTerminated(cell.ref(), Duration.ofSeconds(2));
        dispatcherThread.join(Duration.ofSeconds(2).toMillis());
      }
    }
  }

  @Test
  void manyPoisonActorsRecoveringConcurrentlyEachFailIdenticallyWithoutCrossInterference()
      throws InterruptedException {
    int actorCount = 20;
    StringCodec codec = new StringCodec();
    Map<String, RecordingJournal> journals = new ConcurrentHashMap<>();
    for (int i = 0; i < actorCount; i++) {
      journals.put("poison-" + i, new RecordingJournal(codec.encode("boom-" + i)));
    }

    try (ActorSystem system = ActorSystem.start("test")) {
      List<ActorCell<String>> cells = new CopyOnWriteArrayList<>();
      List<Thread> threads = new CopyOnWriteArrayList<>();
      for (int i = 0; i < actorCount; i++) {
        String id = "poison-" + i;
        ActorCell<String> cell =
            new ActorCell<>(
                system,
                id,
                () ->
                    (context, message) -> {
                      throw new RuntimeException("boom");
                    },
                null,
                SupervisorStrategy.stop(),
                journals.get(id),
                codec);
        cells.add(cell);
        Thread thread = new Thread(cell::run);
        threads.add(thread);
        thread.start();
      }

      for (ActorCell<String> cell : cells) {
        awaitTerminated(cell.ref(), Duration.ofSeconds(5));
      }
      for (Thread thread : threads) {
        thread.join(Duration.ofSeconds(2).toMillis());
      }
    }
  }

  @Test
  void restartDuringReplayResumesAtTheNextRecordWithoutRedeliveringTheFailingOne()
      throws InterruptedException {
    StringCodec codec = new StringCodec();
    RecordingJournal journal =
        new RecordingJournal(codec.encode("boom"), codec.encode("after"));
    List<String> seen = new CopyOnWriteArrayList<>();

    try (ActorSystem system = ActorSystem.start("test")) {
      ActorCell<String> cell =
          new ActorCell<>(
              system,
              "restart-during-replay",
              () ->
                  (context, message) -> {
                    if ("boom".equals(message)) {
                      throw new RuntimeException("boom");
                    }
                    seen.add(message);
                  },
              null,
              SupervisorStrategy.restart(),
              journal,
              codec);
      Thread dispatcherThread = new Thread(cell::run);
      dispatcherThread.start();

      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (!seen.contains("after")) {
              Thread.sleep(5);
            }
          });
      assertEquals(1, seen.stream().filter("boom"::equals).count());
      assertFalse(cell.ref().isTerminated());

      cell.requestStop();
      dispatcherThread.join(Duration.ofSeconds(2).toMillis());
    }
  }

  @Test
  void aCorruptJournalRecordStopsTheActorWithoutConsultingSupervisorStrategy()
      throws InterruptedException {
    AtomicBoolean strategyConsulted = new AtomicBoolean(false);
    SupervisorStrategy spyStrategy =
        failure -> {
          strategyConsulted.set(true);
          return Directive.STOP;
        };
    RecordingJournal journal = new RecordingJournal("not-a-valid-record".getBytes(UTF_8));
    MessageCodec<String> alwaysFailingDecode =
        new MessageCodec<>() {
          @Override
          public byte[] encode(String message) {
            return message.getBytes(UTF_8);
          }

          @Override
          public String decode(byte[] bytes) {
            throw new IllegalArgumentException("corrupt record");
          }
        };

    try (ActorSystem system = ActorSystem.start("test")) {
      ActorCell<String> cell =
          new ActorCell<>(
              system,
              "corrupt-record",
              () -> (context, message) -> {},
              null,
              spyStrategy,
              journal,
              alwaysFailingDecode);
      Thread dispatcherThread = new Thread(cell::run);
      dispatcherThread.start();

      awaitTerminated(cell.ref(), Duration.ofSeconds(2));
      dispatcherThread.join(Duration.ofSeconds(2).toMillis());
      assertFalse(strategyConsulted.get());
    }
  }

  @Test
  void anIoFailureDuringRecoveryStopsTheActorWithoutConsultingSupervisorStrategy()
      throws InterruptedException {
    AtomicBoolean strategyConsulted = new AtomicBoolean(false);
    SupervisorStrategy spyStrategy =
        failure -> {
          strategyConsulted.set(true);
          return Directive.STOP;
        };
    Journal explodingJournal =
        new Journal() {
          @Override
          public void append(byte[] record) {}

          @Override
          public List<byte[]> readAll() {
            throw new java.io.UncheckedIOException(new java.io.IOException("disk full"));
          }
        };
    StringCodec codec = new StringCodec();

    try (ActorSystem system = ActorSystem.start("test")) {
      ActorCell<String> cell =
          new ActorCell<>(
              system,
              "io-failure",
              () -> (context, message) -> {},
              null,
              spyStrategy,
              explodingJournal,
              codec);
      Thread dispatcherThread = new Thread(cell::run);
      dispatcherThread.start();

      awaitTerminated(cell.ref(), Duration.ofSeconds(2));
      dispatcherThread.join(Duration.ofSeconds(2).toMillis());
      assertFalse(strategyConsulted.get());
    }
  }

  private static void awaitTerminated(ActorRef<?> ref, Duration timeout) {
    assertTimeoutPreemptively(
        timeout,
        () -> {
          while (!ref.isTerminated()) {
            Thread.sleep(5);
          }
        });
  }

  /** Reusable {@link MessageCodec} for {@code String} messages, UTF-8 encoded. */
  static final class StringCodec implements MessageCodec<String> {
    @Override
    public byte[] encode(String message) {
      return message.getBytes(UTF_8);
    }

    @Override
    public String decode(byte[] bytes) {
      return new String(bytes, UTF_8);
    }
  }

  /** An in-memory, thread-unsafe-by-design {@link Journal} fake — see {@link Journal}'s own
   * single-owner-thread contract, which every test above respects. */
  static final class RecordingJournal implements Journal {
    private final List<byte[]> records = new CopyOnWriteArrayList<>();

    RecordingJournal(byte[]... seed) {
      records.addAll(List.of(seed));
    }

    @Override
    public void append(byte[] record) {
      records.add(record);
    }

    @Override
    public List<byte[]> readAll() {
      return List.copyOf(records);
    }

    int size() {
      return records.size();
    }
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :framework-core:test --tests PersistentActorTest`
Expected: compilation FAILURE — `ActorCell`'s constructor does not yet accept `Journal`/
`MessageCodec` arguments.

- [ ] **Step 3: Modify `ActorCell` — fields, constructor, unified dispatch loop, termination
      cleanup**

Edit `framework-core/src/main/java/dev/actorframework/core/ActorCell.java`.

Add imports (after the existing `java.util.ArrayDeque` import block):

```java
import java.util.Collections;
import java.util.Iterator;
```

Add two fields, right after `private final SupervisorStrategy strategy;`:

```java
  private final Journal journal;
  private final MessageCodec<T> codec;
```

Replace the constructor:

```java
  ActorCell(
      ActorSystem system,
      String id,
      Supplier<Actor<T>> factory,
      ActorCell<?> parent,
      SupervisorStrategy strategy) {
    this.system = system;
    this.id = id;
    this.factory = factory;
    this.parent = parent;
    this.strategy = strategy;
    this.actor = factory.get();
  }
```

with:

```java
  ActorCell(
      ActorSystem system,
      String id,
      Supplier<Actor<T>> factory,
      ActorCell<?> parent,
      SupervisorStrategy strategy,
      Journal journal,
      MessageCodec<T> codec) {
    this.system = system;
    this.id = id;
    this.factory = factory;
    this.parent = parent;
    this.strategy = strategy;
    this.journal = journal;
    this.codec = codec;
    this.actor = factory.get();
  }
```

Replace `dispatchLoop()`:

```java
  private void dispatchLoop() {
    while (true) {
      T message = mailbox.take();
      if (message == null) {
        // Mailbox closed (explicit stop) and drained: exit without a failure.
        return;
      }
      try {
        actor.onMessage(context, message);
      } catch (Throwable t) {
        if (!handleFailureAndDecideContinue(t, message)) {
          return;
        }
      }
    }
  }
```

with:

```java
  /**
   * Drains a journal-replay iterator first (TASK-601), then falls through to the mailbox — one
   * loop, not two, so every {@link SupervisorStrategy} directive and the poison-message guarantee
   * (ADR-004) apply unchanged whether a message came from replay or is live. A corrupt record or
   * an I/O failure while recovering or journaling is a fatal recovery error, not an actor failure:
   * logged and the actor stops immediately, without consulting {@link #strategy}. See {@code
   * docs/decisions/ADR-016-durable-actor-journal-and-recovery.md}.
   */
  private void dispatchLoop() {
    Iterator<T> recovery;
    try {
      recovery = journal != null ? replayIterator() : Collections.emptyIterator();
    } catch (RuntimeException e) {
      logRecoveryFailure(e);
      return;
    }
    while (true) {
      T message;
      try {
        message = nextMessage(recovery);
      } catch (RuntimeException e) {
        logRecoveryFailure(e);
        return;
      }
      if (message == null) {
        // Mailbox closed (explicit stop) and drained: exit without a failure.
        return;
      }
      try {
        actor.onMessage(context, message);
      } catch (Throwable t) {
        if (!handleFailureAndDecideContinue(t, message)) {
          return;
        }
      }
    }
  }

  /**
   * Returns the next replayed record if any remain, otherwise the next live mailbox message
   * (journaling it write-ahead — before {@code onMessage} runs — if this actor is persistent).
   * {@code null} means the mailbox is closed and drained.
   */
  private T nextMessage(Iterator<T> recovery) {
    if (recovery.hasNext()) {
      return recovery.next();
    }
    T message = mailbox.take();
    if (message != null && journal != null) {
      journal.append(codec.encode(message));
    }
    return message;
  }

  /** Reads this actor's whole journal once and decodes it lazily, one record per {@code next()}. */
  private Iterator<T> replayIterator() {
    Iterator<byte[]> raw = journal.readAll().iterator();
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return raw.hasNext();
      }

      @Override
      public T next() {
        return codec.decode(raw.next());
      }
    };
  }

  private void logRecoveryFailure(RuntimeException failure) {
    LOG.log(
        Level.ERROR,
        "Actor '" + id + "' failed to recover or journal a message; stopping.",
        failure);
  }
```

Modify `finishTermination()`:

```java
  private void finishTermination() {
    safelyRun(() -> actor.postStop(context), "postStop");
    terminated.set(true);
    system.deregister(this);
  }
```

to:

```java
  private void finishTermination() {
    safelyRun(() -> actor.postStop(context), "postStop");
    if (journal != null) {
      safelyRun(journal::close, "journal close");
    }
    terminated.set(true);
    system.deregister(this);
  }
```

- [ ] **Step 4: Update `ActorSystem`'s two existing `ActorCell` call sites to compile**

`ActorSystem` still has the old 5-argument `ActorCell` constructor calls, which no longer compile.
Edit `framework-core/src/main/java/dev/actorframework/core/ActorSystem.java`:

In `spawn(Supplier<Actor<T>> factory, String name)`, replace:

```java
    ActorCell<T> cell = new ActorCell<>(this, name, factory, null, SupervisorStrategy.stop());
```

with:

```java
    ActorCell<T> cell =
        new ActorCell<>(this, name, factory, null, SupervisorStrategy.stop(), null, null);
```

In `spawnChild(...)`, replace:

```java
    ActorCell<C> cell = new ActorCell<>(this, id, factory, parentCell, strategy);
```

with:

```java
    ActorCell<C> cell = new ActorCell<>(this, id, factory, parentCell, strategy, null, null);
```

(Task 3 adds the new persistent `spawn` overload that passes real `Journal`/`MessageCodec`
arguments — this step only keeps the existing two call sites compiling against the new
constructor shape.)

- [ ] **Step 5: Run the tests to verify they pass**

Run: `./gradlew :framework-core:spotlessApply :framework-core:test --tests PersistentActorTest --tests ActorFailureTest --tests ActorSystemTest --tests SupervisionTest --tests AskTest`
Expected: PASS — the new `PersistentActorTest` tests pass, and the four pre-existing suites are
unaffected (confirming Task 2 changed no non-persistent behavior).

- [ ] **Step 6: Commit**

```bash
git add framework-core/src/main/java/dev/actorframework/core/ActorCell.java \
        framework-core/src/main/java/dev/actorframework/core/ActorSystem.java \
        framework-core/src/test/java/dev/actorframework/core/PersistentActorTest.java
git commit -m "TASK-601: unify ActorCell's dispatch loop with journal replay and write-ahead journaling"
```

---

## Task 3: `ActorSystem` — configured `JournalStore`, persistent `spawn` overload, real end-to-end recovery

**Files:**
- Modify: `framework-core/src/main/java/dev/actorframework/core/ActorSystem.java`
- Modify: `framework-core/src/test/java/dev/actorframework/core/PersistentActorTest.java`
  (append the end-to-end tests below to the existing class from Task 2)

**Interfaces:**
- Consumes: `JournalStore`/`Journal`/`MessageCodec<T>` (Task 1); `ActorCell`'s 7-arg constructor
  (Task 2).
- Produces: `ActorSystem.start(String name, JournalStore store)`; `ActorSystem.spawn(factory, name,
  codec)` taking `Supplier<Actor<T>> factory, String name, MessageCodec<T> codec`.

- [ ] **Step 1: Write the failing end-to-end tests**

Append these tests (and the one new helper actor class) to
`framework-core/src/test/java/dev/actorframework/core/PersistentActorTest.java`, just before its
final closing `}`:

```java

  @Test
  void aPersistentActorsStateSurvivesASimulatedProcessRestart(@TempDir Path tempDir)
      throws InterruptedException {
    StringCodec codec = new StringCodec();
    List<String> firstRunEvents = new CopyOnWriteArrayList<>();

    try (ActorSystem systemA = ActorSystem.start("test", JournalStore.fileBacked(tempDir))) {
      ActorRef<String> counter = systemA.spawn(() -> new RecordingActor(firstRunEvents), "counter", codec);
      counter.tell("a");
      counter.tell("b");
      counter.tell("c");
      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (firstRunEvents.size() < 3) {
              Thread.sleep(5);
            }
          });
    }

    List<String> secondRunEvents = new CopyOnWriteArrayList<>();
    try (ActorSystem systemB = ActorSystem.start("test", JournalStore.fileBacked(tempDir))) {
      systemB.spawn(() -> new RecordingActor(secondRunEvents), "counter", codec);
      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (secondRunEvents.size() < 3) {
              Thread.sleep(5);
            }
          });
      assertEquals(List.of("a", "b", "c"), secondRunEvents);
    }
  }

  @Test
  void spawningAPersistentActorWithoutAConfiguredStoreThrows() {
    try (ActorSystem system = ActorSystem.start("test")) {
      assertThrows(
          IllegalStateException.class,
          () -> system.spawn(() -> (context, message) -> {}, "no-store", new StringCodec()));
    }
  }

  @Test
  void nonPersistentSpawnOverloadsAreUnaffectedByAConfiguredStore(@TempDir Path tempDir)
      throws InterruptedException, IOException {
    try (ActorSystem system = ActorSystem.start("test", JournalStore.fileBacked(tempDir))) {
      List<String> received = new CopyOnWriteArrayList<>();
      ActorRef<String> plain = system.spawn(() -> (context, message) -> received.add(message));

      plain.tell("hello");

      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (received.isEmpty()) {
              Thread.sleep(5);
            }
          });
      assertEquals(List.of("hello"), received);
      try (Stream<Path> entries = Files.list(tempDir)) {
        assertTrue(entries.findAny().isEmpty());
      }
    }
  }

  /** Records every message it processes, in order — used to observe replayed history. */
  private static final class RecordingActor implements Actor<String> {
    private final List<String> events;

    RecordingActor(List<String> events) {
      this.events = events;
    }

    @Override
    public void onMessage(ActorContext<String> context, String message) {
      events.add(message);
    }
  }
```

Add the missing imports at the top of the file (alongside the existing ones):

```java
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :framework-core:test --tests PersistentActorTest`
Expected: compilation FAILURE — `ActorSystem.start(String, JournalStore)` and the persistent
`spawn` overload do not exist yet.

- [ ] **Step 3: Modify `ActorSystem`**

Edit `framework-core/src/main/java/dev/actorframework/core/ActorSystem.java`.

Add a field, next to the other fields:

```java
  private final JournalStore store;
```

Replace the private constructor and `start()`/`start(String)`:

```java
  private ActorSystem(String name) {
    this.name = name;
    this.dispatcher = new Dispatcher();
  }

  public static ActorSystem start() {
    return start("actor-system");
  }

  public static ActorSystem start(String name) {
    return new ActorSystem(name);
  }
```

with:

```java
  private ActorSystem(String name, JournalStore store) {
    this.name = name;
    this.store = store;
    this.dispatcher = new Dispatcher();
  }

  public static ActorSystem start() {
    return start("actor-system");
  }

  public static ActorSystem start(String name) {
    return new ActorSystem(name, null);
  }

  /**
   * Starts a system configured with {@code store} for persistent actors (TASK-601): an actor
   * opts into journaling by being spawned via {@link #spawn(Supplier, String, MessageCodec)}.
   * {@link #spawn(Supplier)}/{@link #spawn(Supplier, String)} are unaffected — non-persistent
   * actors never touch {@code store}. See {@code
   * docs/decisions/ADR-016-durable-actor-journal-and-recovery.md}.
   *
   * @throws IllegalArgumentException if {@code store} is {@code null}
   */
  public static ActorSystem start(String name, JournalStore store) {
    if (store == null) {
      throw new IllegalArgumentException("store must not be null");
    }
    return new ActorSystem(name, store);
  }
```

Add the persistent `spawn` overload right after the existing `spawn(Supplier<Actor<T>> factory,
String name)` method:

```java
  /**
   * Spawns a new persistent top-level actor with the given name (TASK-601): passing {@code codec}
   * opts it into journaling via this system's configured {@link JournalStore}. On spawn, the
   * actor's journal (if any records exist) is replayed via {@code onMessage} before any live
   * message is processed, reconstructing its history from previous runs. Every live message is
   * durably appended to the journal <em>before</em> {@code onMessage} runs (write-ahead), so it
   * survives a process restart even if processing was interrupted mid-message. Existing {@link
   * #spawn(Supplier)}/{@link #spawn(Supplier, String)} overloads are unaffected — an actor spawned
   * through them never touches a journal. See {@code
   * docs/decisions/ADR-016-durable-actor-journal-and-recovery.md}.
   *
   * @throws IllegalStateException if this system has no configured {@link JournalStore}, or is
   *     shutting down
   * @throws IllegalArgumentException if an actor with this name already exists
   */
  public <T> ActorRef<T> spawn(Supplier<Actor<T>> factory, String name, MessageCodec<T> codec) {
    if (store == null) {
      throw new IllegalStateException(
          "Cannot spawn persistent actor '"
              + name
              + "': ActorSystem '"
              + this.name
              + "' has no configured JournalStore");
    }
    if (shuttingDown.get()) {
      throw new IllegalStateException(
          "Cannot spawn actor '" + name + "': ActorSystem '" + this.name + "' is shutting down");
    }
    Journal journal = store.open(name);
    ActorCell<T> cell =
        new ActorCell<>(this, name, factory, null, SupervisorStrategy.stop(), journal, codec);
    if (actors.putIfAbsent(name, cell) != null) {
      journal.close();
      throw new IllegalArgumentException("An actor named '" + name + "' already exists");
    }
    dispatcher.execute(cell::run);
    return cell.ref();
  }
```

Modify `close()`:

```java
  @Override
  public void close() {
    shutdown();
    dispatcher.close();
  }
```

to:

```java
  @Override
  public void close() {
    shutdown();
    dispatcher.close();
    if (store != null) {
      store.close();
    }
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :framework-core:spotlessApply :framework-core:test --tests PersistentActorTest`
Expected: PASS (all `PersistentActorTest` tests, including the three added in this task).

- [ ] **Step 5: Run the full existing suite to confirm nothing else regressed**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL — every module compiles (`-Xlint:all -Werror`), every existing test
(`ActorFailureTest`, `ActorSystemTest`, `SupervisionTest`, `AskTest`, `MailboxTest`,
`SafePublicationTest`, `framework-testkit`'s tests, the two examples) still passes unmodified —
this is the regression signal that non-persistent actors are untouched.

- [ ] **Step 6: Commit**

```bash
git add framework-core/src/main/java/dev/actorframework/core/ActorSystem.java \
        framework-core/src/test/java/dev/actorframework/core/PersistentActorTest.java
git commit -m "TASK-601: ActorSystem.start(name, store) and the persistent spawn(factory, name, codec) overload"
```

---

## Task 4: ADR-016, ADR-004/ADR-008 addenda, `docs/architecture.md` §10 and milestone bump

**Files:**
- Create: `docs/decisions/ADR-016-durable-actor-journal-and-recovery.md`
- Modify: `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md`
- Modify: `docs/decisions/ADR-008-supervision-strategies-and-hierarchies.md`
- Modify: `docs/architecture.md`

**Interfaces:** None — documentation only, no code or test changes. `README.md`'s stated
milestone line is deliberately **not** touched here, matching the established project pattern:
M5's own ADR/architecture updates ("M5: ask() request-response messaging (ADR-015)", #39) landed
before the README bump ("Bump README's stated milestone to M5", #40), which was its own follow-up
PR after M5 was fully merged.

- [ ] **Step 1: Write ADR-016**

Create `docs/decisions/ADR-016-durable-actor-journal-and-recovery.md`:

```markdown
# ADR-016: Durable per-actor message journal and recovery on restart (TASK-601, M6)

* Status: Accepted
* Written during: M6 (TASK-601)
* Builds on: ADR-004 (poison-message semantics — reconciles journal replay with it), ADR-008
  (supervision strategies — reconciles `Restart` with replay)

## Context

M1–M5 hold all actor state in plain in-memory fields (ADR-005 §1); nothing survives a process
restart. This task designs and implements a write-ahead journal per actor — persisted messages
replayed on `ActorSystem.spawn(...)` to reconstruct an actor's history — with a pluggable storage
abstraction and a local file-backed default (local-first, `AGENTS.md` design principle #3). It
must explicitly reconcile with ADR-004's "poison message... never redelivered" guarantee and
ADR-008's `Restart` directive (a restarted actor must not replay the message that caused the
failure it's recovering from).

## Decision

### New interfaces (`framework-core`)

```java
public interface MessageCodec<T> {
  byte[] encode(T message);
  T decode(byte[] bytes);
}

public interface JournalStore extends AutoCloseable {
  Journal open(String actorId);
  default void close() {}
}

public interface Journal extends AutoCloseable {
  void append(byte[] record);
  List<byte[]> readAll();
  default void close() {}
}
```

`JournalStore`/`Journal` operate on raw bytes only — they know nothing about `T`. `MessageCodec<T>`
is the only place serialization is decided, supplied by whoever opts an actor into persistence.
This keeps the storage abstraction pluggable (file-backed today, nothing stops a future DB-backed
store) without coupling it to a serialization library, matching "small core" and "pluggable
infrastructure" (`docs/architecture.md` §7/§8, e.g. `ask()` reusing `tell()`/`Mailbox` unchanged
rather than inventing new machinery).

### API surface (opt-in, non-persistent actors unchanged)

* `ActorSystem.start(String name, JournalStore store)` configures one store for the whole system.
  Existing `start()`/`start(name)` configure none.
* `ActorSystem.spawn(Supplier<Actor<T>> factory, String name, MessageCodec<T> codec)` is a new
  overload; passing a codec is what opts an actor into journaling. Throws `IllegalStateException`
  if the system has no configured `JournalStore`. Existing `spawn(factory)`/`spawn(factory, name)`
  are byte-for-byte unchanged — a non-persistent actor never touches the journal, mirroring how a
  top-level actor never touches `SupervisorStrategy` beyond the fixed `stop()` default (ADR-008
  precedent: complexity only when explicitly asked for).
* `JournalStore.fileBacked(Path root)` is the concrete local file-backed default. `root` is always
  explicit; no implicit default directory (`docs/architecture.md` §8 "explicit behavior").
* Child actors (`ActorContext.spawnChild`) are not extended with a persistent overload in this
  task — no concrete need yet; adding one later is a small, additive change to `ActorContext` and
  `ActorSystem.spawnChild` following the same shape as top-level `spawn`.

### `ActorCell` integration: one loop, not two

Replay and live processing are a single loop: `dispatchLoop()` drains a journal-replay iterator
first, then falls through to the mailbox, journaling each live message *before* `onMessage` runs
(write-ahead). `handleFailureAndDecideContinue` (and therefore all four supervisor directives)
needed **no changes** — this is what makes the ADR-004/ADR-008 reconciliation fall out for free
instead of needing new logic:

* **Write-ahead, not write-after**: a live message is journaled before `onMessage` runs, so a
  message that completes processing is always durably recoverable even if the process is killed
  mid-processing — the in-memory effect is lost with the process, but replay reconstructs it from
  scratch on the next spawn.
* **Poison message, live**: unchanged from today — the same catch block, the same strategy
  consultation, regardless of persistence.
* **Poison message, on replay**: if a journaled message throws again during replay (a
  deterministic actor reproducing the same failure), it is handled by the *same* strategy on the
  *same* thread via the *same* code path. Under `stop()` this halts recovery at that record.
  **Accepted limitation, stated explicitly**: a truly poison journaled message fails identically
  on every future recovery attempt — nothing removes it from the journal. TASK-602's snapshotting
  is the future lever for bounding or working around it; this task does not invent a fix.
* **`Restart` directive**: orthogonal to the journal. `Restart` replaces `actor` and re-runs
  `preStart`; the loop moves on to the next record/message regardless of how the previous
  iteration's failure was handled — the failing record is consumed exactly once from the recovery
  iterator, the same way `Mailbox.take()` already consumes a message exactly once before
  `onMessage` runs. No journal-specific `Restart` logic is needed.
* **Corrupt/undecodable record**: `codec.decode()` throwing is a fatal recovery error, not an
  actor failure — the actor never saw a message, so `SupervisorStrategy` is not consulted. It is
  logged and the actor stops immediately.

### File-backed default `JournalStore`

* One file per actor id, at `root.resolve(sanitize(actorId))`. Actor ids may contain `/` (child
  namespacing); the id is sanitized into a safe filename (`/` → `_`) rather than creating nested
  directories. Sanitization collisions (e.g. `"a/b"` and `"a_b"`) are an accepted limitation,
  documented not solved.
* Format: sequential length-prefixed records — a 4-byte big-endian length header followed by that
  many payload bytes, repeated to EOF. Minimal, streamable, no external dependency.
* `open(actorId)` creates the root directory and file if absent, and opens the file in read/write
  mode, kept open for the actor's entire lifetime — closed on actor termination, alongside
  `postStop`.
* `readAll()` reads the file from the start, once, at replay time, before any live append happens
  on that same open handle.
* **No internal synchronization.** A given `Journal` instance is only ever touched by its actor's
  own dispatcher thread — append happens inline in `dispatchLoop`, `readAll` happens once before
  the live loop starts. Same reasoning as `Mailbox`'s actor-owned state (ADR-005), stated
  explicitly so it is not accidentally reused across threads later.
* I/O failures (disk full, permission error) on `append`/`open` propagate as unchecked exceptions
  (wrapping `IOException`), treated like the corrupt-record case: fatal to that actor, not
  `SupervisorStrategy`-arbitrated, since it is an environment problem, not actor logic.
* No automatic cleanup or deletion. A persistent actor's journal outlives an explicit
  `ActorSystem.stop()`/termination — respawning a new actor under the same id later replays the
  old history. Persistence is keyed by actor id, not by process lifetime, the same way `ask()`'s
  reply channel and every other actor identity concept in this codebase is keyed by id.

## What this deliberately does not cover

* **Snapshotting / bounding replay time** (TASK-602, explicitly the next task).
* **Compaction or deletion of journaled records.**
* **Exactly-once side-effect semantics for replayed messages** — an actor's `onMessage` must
  tolerate at-least-once redelivery of its own side effects if it opts into persistence, the same
  accepted tradeoff every event-sourced actor framework makes.
* **A codec implementation beyond the interface** — callers supply their own, same as they supply
  their own `Actor<T>`.
* **Per-actor journal stores** — one store per `ActorSystem`, matching one `Dispatcher` per system
  today; no concrete need for mixed backends within one system exists yet.
* **A separate command/event split** (a new `applyEvent` method distinct from `onMessage`).
  Rejected: doubles the `Actor` API surface for a guarantee this project has no concrete need for
  yet — replay-via-`onMessage` is simpler and matches "small core."
* **A marker interface (`PersistentActor<T>`) instead of a spawn overload.** Rejected: couples
  persistence to the actor's static type rather than to how it's spawned; a spawn-time argument is
  more consistent with `spawnChild`'s existing "extra argument decides the behavior" precedent.
* **Write-after (journal only a successfully-processed message)** instead of write-ahead.
  Rejected: silently loses a message's effect forever if the process crashes between a successful
  `onMessage` and the journal write — worse than the accepted "poison record replays forever"
  tradeoff, and contradicts "write-ahead" as named in the task.
* **An implicit default journal directory** for `ActorSystem.start(name)`. Rejected: silent disk
  I/O a caller didn't ask for contradicts `docs/architecture.md` §8's "explicit behavior"
  principle.

## Consequences

* `framework-core`: new public `MessageCodec`, `JournalStore`, `Journal` interfaces; new
  package-private `FileBackedJournalStore`/`FileBackedJournal`; `ActorSystem` gains
  `start(name, store)` and `spawn(factory, name, codec)`; `ActorCell` gains `journal`/`codec`
  fields, a unified `dispatchLoop`, and journal cleanup in `finishTermination`.
* No change to `Mailbox`, `Dispatcher`, `SupervisorStrategy`, or any existing public signature —
  purely additive. `ActorFailureTest`, `ActorSystemTest`, `SupervisionTest`, `AskTest` needed no
  edits, the regression signal that nothing existing changed behavior.
* `docs/decisions/ADR-004-...md` and `docs/decisions/ADR-008-...md` each carry a short "Resolved
  at TASK-601" addendum. `docs/architecture.md` bumped to M6 and gains a §10 "Persistence"
  section.
* New tests: `FileBackedJournalStoreTest` (round trip, id sanitization, concurrent isolated files
  under real load) and `PersistentActorTest` (write-ahead ordering, poison-on-replay halting and
  repeating identically — including under concurrent load — `Restart`-during-replay resuming at
  the next record, a corrupt record and an I/O failure each stopping the actor without consulting
  `SupervisorStrategy`, a real file-backed process-restart round trip, the no-store error, and
  confirmation that non-persistent `spawn` overloads are untouched).
* Any future change to what "poison record" or "write-ahead" means for the journal, or to the
  replay-via-`onMessage` design, must engage with this ADR explicitly, per `AGENTS.md`'s ADR-gate
  list.
```

- [ ] **Step 2: Add ADR-004's "Resolved at TASK-601" addendum**

Edit `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md`. After the existing
`## Resolved at M5 (ADR-015)` section (its last line ends `...See docs/decisions/ADR-015-ask-pattern.md for the full design.`), append:

```markdown

## Resolved at TASK-601 (ADR-016)

TASK-601 added a durable per-actor journal that replays messages via `onMessage` on spawn — a
second path, besides live mailbox delivery, that this ADR's "processed at most once, never
redelivered" guarantee had to be checked against. **It holds unchanged.** A journaled message
throwing again during replay is handled by the exact same `SupervisorStrategy` consultation as a
live failure, on the same code path; under `Restart` the failing record is consumed exactly once
from the replay iterator before `onMessage` runs, the same dequeue-before-processing discipline
`Mailbox.take()` already established for live messages (see this ADR's own "Resolved at
TASK-402" section, above). A truly poison journaled message is a new, explicitly accepted
limitation this ADR did not previously need to name: it fails identically on every future recovery
attempt, since nothing removes it from the journal — TASK-602's snapshotting is the intended future
lever. See `docs/decisions/ADR-016-durable-actor-journal-and-recovery.md` for the full design.
```

- [ ] **Step 3: Add ADR-008's "Resolved at TASK-601" addendum**

Edit `docs/decisions/ADR-008-supervision-strategies-and-hierarchies.md`. Append at the end of the
file:

```markdown

## Resolved at TASK-601 (ADR-016)

TASK-601 added journal replay via `onMessage`, a second source of messages besides the mailbox
that every `SupervisorStrategy` directive had to be checked against. **No change was needed.**
`Restart` replaces the actor instance and re-runs `preStart` exactly as it already did for a live
failure; the dispatch loop then moves on to the next replayed record or mailbox message regardless
of which directive the previous failure resolved to — the failing record is never redelivered,
mirroring how a live poison message is never redelivered (ADR-004's own addendum, above). See
`docs/decisions/ADR-016-durable-actor-journal-and-recovery.md` for the full design, including the
one genuinely new case supervision now has to reckon with: a *journaled* poison message under
`stop()`, which halts recovery at that record identically on every future attempt — an accepted
limitation, not a supervision change.
```

- [ ] **Step 4: Update `docs/architecture.md`**

Edit `docs/architecture.md`.

Replace the status line:

```markdown
Status: living document, updated as milestones land. Current milestone: **M5 — Ask Pattern**.
```

with:

```markdown
Status: living document, updated as milestones land. Current milestone: **M6 — Persistence**.
```

Insert a new `## 10. Persistence (M6)` section immediately before the `## Roadmap` section (i.e.
right after §9's last paragraph, which currently ends `...only act on it from onMessage.`):

```markdown

## 10. Persistence (M6)

An actor opts into durable, per-actor journaling by being spawned through
`ActorSystem.spawn(factory, name, codec)` — a `MessageCodec<T>` argument is what makes the actor
persistent — on a system started with `ActorSystem.start(name, store)`. Existing `spawn(factory)`/
`spawn(factory, name)` are unaffected; a non-persistent actor never touches a journal.

On spawn, a persistent actor's journal (if it has one from a previous run) is replayed via
`onMessage` before any live message is processed, reconstructing its history. Every live message
is durably appended to the journal *before* `onMessage` runs (write-ahead): it survives a process
restart even if processing is interrupted mid-message. Replay and live processing are one
dispatch loop, not two, so every `SupervisorStrategy` directive (§7) and the poison-message
guarantee (§7, ADR-004) apply identically to a replayed message as to a live one — including the
accepted limitation that a truly poison *journaled* message halts recovery identically on every
future attempt, since nothing removes it from the journal (TASK-602's future snapshotting is the
intended lever for bounding this).

`JournalStore`/`Journal` are a pluggable, byte-level storage abstraction (`JournalStore.open`
returns a `Journal` of `append`/`readAll`); `MessageCodec<T>` is the only place serialization is
decided. `JournalStore.fileBacked(Path root)` is the local, file-backed default: one
length-prefixed record file per actor id under an explicit `root` — no implicit default directory
(§8's "explicit behavior"). See `docs/decisions/ADR-016-durable-actor-journal-and-recovery.md`
for the full design.
```

- [ ] **Step 5: Run the full build one final time**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL. (Docs changes don't affect compilation, but this confirms the branch
as a whole — code from Tasks 1-3 plus docs from Task 4 — is green together.)

- [ ] **Step 6: Commit**

```bash
git add docs/decisions/ADR-016-durable-actor-journal-and-recovery.md \
        docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md \
        docs/decisions/ADR-008-supervision-strategies-and-hierarchies.md \
        docs/architecture.md
git commit -m "TASK-601: add ADR-016, reconcile ADR-004/ADR-008, document M6 persistence in architecture.md"
```

---

## After this plan lands

Open a PR per `AGENTS.md`'s git workflow (feature branch already checked out:
`task-601-durable-actor-journal`) — do not push to `main` directly. This closes GitHub issue #42.
TASK-602 (state snapshotting to bound journal replay time, GitHub issue #43) is the natural next
task once this merges.
