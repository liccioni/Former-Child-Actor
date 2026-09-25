package dev.actorframework.core;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * TASK-601: durable per-actor journaling and recovery on restart. Persistence-facing {@link
 * ActorSystem} API (the real end-to-end file-backed round trip, the no-store error, and
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
      cell.requestStop();
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
    RecordingJournal journal = new RecordingJournal(codec.encode("boom"), codec.encode("after"));
    List<String> seen = new CopyOnWriteArrayList<>();
    AtomicInteger boomAttempts = new AtomicInteger();

    try (ActorSystem system = ActorSystem.start("test")) {
      ActorCell<String> cell =
          new ActorCell<>(
              system,
              "restart-during-replay",
              () ->
                  (context, message) -> {
                    if ("boom".equals(message)) {
                      boomAttempts.incrementAndGet();
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
      assertEquals(1, boomAttempts.get());
      assertEquals(List.of("after"), seen);
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

  @Test
  void aLiveMessageAppendFailureStopsTheActorWithoutConsultingSupervisorStrategy()
      throws InterruptedException {
    AtomicBoolean strategyConsulted = new AtomicBoolean(false);
    SupervisorStrategy spyStrategy =
        failure -> {
          strategyConsulted.set(true);
          return Directive.STOP;
        };
    AtomicBoolean onMessageCalled = new AtomicBoolean(false);
    Journal explodingOnAppendJournal =
        new Journal() {
          @Override
          public void append(byte[] record) {
            throw new java.io.UncheckedIOException(new java.io.IOException("disk full"));
          }

          @Override
          public List<byte[]> readAll() {
            return List.of();
          }
        };
    StringCodec codec = new StringCodec();

    try (ActorSystem system = ActorSystem.start("test")) {
      ActorCell<String> cell =
          new ActorCell<>(
              system,
              "append-failure",
              () -> (context, message) -> onMessageCalled.set(true),
              null,
              spyStrategy,
              explodingOnAppendJournal,
              codec);
      Thread dispatcherThread = new Thread(cell::run);
      dispatcherThread.start();

      cell.ref().tell("hello");

      awaitTerminated(cell.ref(), Duration.ofSeconds(2));
      dispatcherThread.join(Duration.ofSeconds(2).toMillis());
      assertFalse(strategyConsulted.get());
      assertFalse(onMessageCalled.get());
    }
  }

  /**
   * Regression test for #47: a journal failure must stop the actor the same way every other exit
   * path does — closing its mailbox (releasing senders blocked on a full one) and cascading the
   * stop to its children — not just log and return.
   */
  @Test
  void aJournalFailureReleasesBlockedSendersAndStopsChildren() throws InterruptedException {
    CountDownLatch appendEntered = new CountDownLatch(1);
    CountDownLatch failAppend = new CountDownLatch(1);
    Journal blockThenFailJournal =
        new Journal() {
          @Override
          public void append(byte[] record) {
            appendEntered.countDown();
            try {
              failAppend.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            throw new java.io.UncheckedIOException(new java.io.IOException("disk full"));
          }

          @Override
          public List<byte[]> readAll() {
            return List.of();
          }
        };
    AtomicReference<ActorRef<String>> child = new AtomicReference<>();

    try (ActorSystem system = ActorSystem.start("test", actorId -> blockThenFailJournal)) {
      ActorRef<String> parent =
          system.spawn(
              () ->
                  new Actor<String>() {
                    @Override
                    public void preStart(ActorContext<String> context) {
                      child.set(context.spawnChild(() -> (ctx, message) -> {}, "child"));
                    }

                    @Override
                    public void onMessage(ActorContext<String> context, String message) {}
                  },
              "failing-journal",
              new StringCodec());

      parent.tell("first");
      assertTrue(appendEntered.await(2, TimeUnit.SECONDS));
      // The dispatcher is now stuck inside append(); fill the mailbox, then block one more sender.
      Thread sender =
          new Thread(
              () -> {
                for (int i = 0; i <= Mailbox.DEFAULT_CAPACITY; i++) {
                  parent.tell("m" + i);
                }
              });
      sender.setDaemon(true);
      sender.start();
      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (sender.getState() != Thread.State.WAITING) {
              Thread.sleep(5);
            }
          });

      failAppend.countDown();

      awaitTerminated(parent, Duration.ofSeconds(2));
      sender.join(Duration.ofSeconds(2).toMillis());
      assertFalse(sender.isAlive(), "sender blocked on the full mailbox was never released");
      awaitTerminated(child.get(), Duration.ofSeconds(2));
    }
  }

  /**
   * Regression test for #49: a stop requested mid-replay must take effect after the record being
   * processed, not after the whole journal has been replayed.
   */
  @Test
  void aStopRequestedDuringReplayTakesEffectAfterTheCurrentRecord() throws InterruptedException {
    StringCodec codec = new StringCodec();
    int recordCount = 100;
    byte[][] seed = new byte[recordCount][];
    for (int i = 0; i < recordCount; i++) {
      seed[i] = codec.encode("r" + i);
    }
    RecordingJournal journal = new RecordingJournal(seed);
    CountDownLatch firstRecordEntered = new CountDownLatch(1);
    CountDownLatch proceed = new CountDownLatch(1);
    AtomicInteger processed = new AtomicInteger();

    try (ActorSystem system = ActorSystem.start("test")) {
      ActorCell<String> cell =
          new ActorCell<>(
              system,
              "long-replay",
              () ->
                  (context, message) -> {
                    processed.incrementAndGet();
                    firstRecordEntered.countDown();
                    proceed.await();
                  },
              null,
              SupervisorStrategy.stop(),
              journal,
              codec);
      Thread dispatcherThread = new Thread(cell::run);
      dispatcherThread.start();

      assertTrue(firstRecordEntered.await(2, TimeUnit.SECONDS));
      cell.requestStop();
      proceed.countDown();

      awaitTerminated(cell.ref(), Duration.ofSeconds(2));
      dispatcherThread.join(Duration.ofSeconds(2).toMillis());
      assertEquals(1, processed.get());
    }
  }

  /**
   * Regression test for #52: a journal opened for a persistent spawn must be closed if the actor
   * can't be constructed (its factory throws), not leaked.
   */
  @Test
  void aThrowingFactoryDoesNotLeakTheOpenedJournal() {
    AtomicBoolean journalClosed = new AtomicBoolean(false);
    Journal trackingJournal =
        new Journal() {
          @Override
          public void append(byte[] record) {}

          @Override
          public List<byte[]> readAll() {
            return List.of();
          }

          @Override
          public void close() {
            journalClosed.set(true);
          }
        };

    try (ActorSystem system = ActorSystem.start("test", actorId -> trackingJournal)) {
      assertThrows(
          IllegalStateException.class,
          () ->
              system.spawn(
                  () -> {
                    throw new IllegalStateException("factory failed");
                  },
                  "throwing-factory",
                  new StringCodec()));
      assertTrue(journalClosed.get());
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

  /**
   * An in-memory, thread-unsafe-by-design {@link Journal} fake — see {@link Journal}'s own
   * single-owner-thread contract, which every test above respects.
   */
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

  @Test
  void aPersistentActorsStateSurvivesASimulatedProcessRestart(@TempDir Path tempDir)
      throws InterruptedException {
    StringCodec codec = new StringCodec();
    List<String> firstRunEvents = new CopyOnWriteArrayList<>();

    try (ActorSystem systemA = ActorSystem.start("test", JournalStore.fileBacked(tempDir))) {
      ActorRef<String> counter =
          systemA.spawn(() -> new RecordingActor(firstRunEvents), "counter", codec);
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
}
