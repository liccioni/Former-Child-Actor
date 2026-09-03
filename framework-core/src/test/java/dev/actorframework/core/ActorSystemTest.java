package dev.actorframework.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ActorSystemTest {

  @Test
  void deliversAMessageToASpawnedActor() throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<String> received = new LinkedBlockingQueue<>();
      ActorRef<String> ref = system.spawn(() -> (context, message) -> received.add(message));

      ref.tell("hello");

      assertEquals("hello", received.poll(2, TimeUnit.SECONDS));
    }
  }

  @Test
  void spawnedActorsGetStableUniqueIds() {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<String> a = system.spawn(() -> (context, message) -> {}, "a");
      ActorRef<String> b = system.spawn(() -> (context, message) -> {});

      assertEquals("a", a.id());
      assertNotEquals(a.id(), b.id());
      assertEquals(a.id(), a.id());
    }
  }

  @Test
  void spawningWithADuplicateNameFails() {
    try (ActorSystem system = ActorSystem.start("test")) {
      system.spawn(() -> (context, message) -> {}, "dup");

      assertThrows(
          IllegalArgumentException.class,
          () -> system.spawn(() -> (context, message) -> {}, "dup"));
    }
  }

  @Test
  void atMostOneMessageIsProcessedAtATimePerActor() throws InterruptedException {
    int messageCount = 2_000;
    try (ActorSystem system = ActorSystem.start("test")) {
      AtomicInteger inFlight = new AtomicInteger(0);
      List<Integer> maxObservedInFlight = new CopyOnWriteArrayList<>();
      CountDownLatch allProcessed = new CountDownLatch(messageCount);

      ActorRef<Integer> ref =
          system.spawn(
              () ->
                  (context, message) -> {
                    int concurrent = inFlight.incrementAndGet();
                    maxObservedInFlight.add(concurrent);
                    if (concurrent > 1) {
                      throw new AssertionError(
                          "Two messages were processed concurrently: " + concurrent);
                    }
                    // Yield to give a real overlap a chance to manifest if the guarantee were
                    // broken.
                    Thread.onSpinWait();
                    inFlight.decrementAndGet();
                    allProcessed.countDown();
                  });

      ExecutorService senders = Executors.newFixedThreadPool(8);
      try {
        for (int i = 0; i < messageCount; i++) {
          int message = i;
          senders.execute(() -> ref.tell(message));
        }
        assertTrue(allProcessed.await(10, TimeUnit.SECONDS), "all messages should be processed");
      } finally {
        senders.shutdown();
      }

      assertTrue(maxObservedInFlight.stream().allMatch(n -> n == 1));
    }
  }

  @Test
  void stoppingAnActorDiscardsItsQueuedMessages() {
    try (ActorSystem system = ActorSystem.start("test")) {
      CountDownLatch firstMessageStarted = new CountDownLatch(1);
      CountDownLatch releaseFirstMessage = new CountDownLatch(1);
      BlockingQueue<String> processed = new LinkedBlockingQueue<>();

      ActorRef<String> ref =
          system.spawn(
              () ->
                  (context, message) -> {
                    if ("block-me".equals(message)) {
                      firstMessageStarted.countDown();
                      awaitUninterruptibly(releaseFirstMessage);
                    }
                    processed.add(message);
                  });

      ref.tell("block-me");
      assertTimeoutPreemptively(Duration.ofSeconds(2), () -> firstMessageStarted.await());
      ref.tell("never-delivered-1");
      ref.tell("never-delivered-2");

      system.stop(ref);
      releaseFirstMessage.countDown();

      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (!ref.isTerminated()) {
              Thread.sleep(5);
            }
          });
      assertEquals(List.of("block-me"), List.copyOf(processed));
    }
  }

  @Test
  void tellAfterTerminationIsSilentlyDropped() {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<String> processed = new LinkedBlockingQueue<>();
      ActorRef<String> ref = system.spawn(() -> (context, message) -> processed.add(message));

      system.stop(ref);
      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (!ref.isTerminated()) {
              Thread.sleep(5);
            }
          });

      ref.tell("too-late");

      assertTrue(processed.isEmpty());
    }
  }

  @Test
  void cannotSpawnOnceTheSystemIsShuttingDown() {
    ActorSystem system = ActorSystem.start("test");
    system.shutdown();

    assertThrows(IllegalStateException.class, () -> system.spawn(() -> (context, message) -> {}));

    system.close();
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          latch.await();
          return;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
