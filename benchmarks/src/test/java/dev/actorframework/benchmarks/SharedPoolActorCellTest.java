package dev.actorframework.benchmarks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Before trusting benchmark numbers from {@link SharedPoolActorCell}, this proves it actually gives
 * the same two guarantees {@code ActorCell} does (TASK-106): every message delivered exactly once,
 * and never two messages processed concurrently — despite many actors sharing a small, fixed thread
 * pool instead of each owning a dedicated thread.
 */
class SharedPoolActorCellTest {

  @Test
  void deliversEveryMessageExactlyOnceUnderConcurrentSenders() throws InterruptedException {
    int messageCount = 20_000;
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      List<Integer> received = new CopyOnWriteArrayList<>();
      CountDownLatch allProcessed = new CountDownLatch(messageCount);
      SharedPoolActorCell<Integer> actor =
          new SharedPoolActorCell<>(
              pool,
              message -> {
                received.add(message);
                allProcessed.countDown();
              });

      ExecutorService senders = Executors.newFixedThreadPool(8);
      try {
        for (int i = 0; i < messageCount; i++) {
          int message = i;
          senders.execute(() -> actor.tell(message));
        }
        assertTrue(allProcessed.await(10, TimeUnit.SECONDS), "all messages should be processed");
      } finally {
        senders.shutdown();
      }

      assertEquals(messageCount, received.size(), "no message should be lost or duplicated");
    } finally {
      pool.shutdown();
    }
  }

  @Test
  void neverProcessesTwoMessagesConcurrentlyAcrossSharedPoolThreads() throws InterruptedException {
    int messageCount = 20_000;
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      AtomicInteger inFlight = new AtomicInteger(0);
      AtomicInteger concurrencyViolations = new AtomicInteger(0);
      CountDownLatch allProcessed = new CountDownLatch(messageCount);
      SharedPoolActorCell<Integer> actor =
          new SharedPoolActorCell<>(
              pool,
              message -> {
                if (inFlight.incrementAndGet() > 1) {
                  concurrencyViolations.incrementAndGet();
                }
                Thread.onSpinWait();
                inFlight.decrementAndGet();
                allProcessed.countDown();
              });

      ExecutorService senders = Executors.newFixedThreadPool(8);
      try {
        for (int i = 0; i < messageCount; i++) {
          senders.execute(() -> actor.tell(1));
        }
        assertTrue(allProcessed.await(10, TimeUnit.SECONDS), "all messages should be processed");
      } finally {
        senders.shutdown();
      }

      assertEquals(0, concurrencyViolations.get());
    } finally {
      pool.shutdown();
    }
  }
}
