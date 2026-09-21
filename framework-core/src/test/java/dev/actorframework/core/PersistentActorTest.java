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
}
