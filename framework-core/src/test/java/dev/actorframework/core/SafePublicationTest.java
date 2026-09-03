package dev.actorframework.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * TASK-207 (ADR-005): the mailbox's lock gives a happens-before edge from a sender's {@code tell()}
 * to the receiving actor's {@code onMessage} — so a plain (non-volatile, non-atomic) field write a
 * sender makes <em>before</em> calling {@code tell()} must be visible to the actor's dispatcher
 * thread, even though it is always a different thread than the sender. These tests guard that edge
 * directly, as a regression check against a future change to {@code Mailbox}'s synchronization
 * strategy that might silently drop it.
 *
 * <p>Each message carries a freshly allocated {@link Holder}, mutated exactly once by the sender
 * and never touched again — reusing a single shared cell across iterations would race against the
 * actor's own (much slower) consumption and produce false failures unrelated to visibility, so this
 * deliberately avoids that shape.
 */
class SafePublicationTest {

  /** A plain mutable holder: the field under test is written via ordinary assignment. */
  private static final class Holder {
    int mutableValue;
  }

  private record Publish(Holder holder, int expected) {}

  @Test
  void stateWrittenBeforeTellIsVisibleInOnMessage() throws InterruptedException {
    int iterations = 5_000;
    try (ActorSystem system = ActorSystem.start("test")) {
      AtomicInteger staleReads = new AtomicInteger(0);
      CountDownLatch allProcessed = new CountDownLatch(iterations);

      ActorRef<Publish> ref =
          system.spawn(
              () ->
                  (context, message) -> {
                    if (message.holder().mutableValue != message.expected()) {
                      staleReads.incrementAndGet();
                    }
                    allProcessed.countDown();
                  });

      for (int i = 0; i < iterations; i++) {
        Holder holder = new Holder();
        holder.mutableValue = i;
        ref.tell(new Publish(holder, i));
      }

      assertTrue(allProcessed.await(10, TimeUnit.SECONDS), "all messages should be processed");
      assertEquals(
          0,
          staleReads.get(),
          "actor observed a stale/reordered value of a plain field written before tell()");
    }
  }

  @Test
  void stateWrittenBeforeTellFromMultipleSenderThreadsIsVisible() throws InterruptedException {
    int sendersCount = 8;
    int perSender = 2_000;
    try (ActorSystem system = ActorSystem.start("test")) {
      AtomicInteger staleReads = new AtomicInteger(0);
      CountDownLatch allProcessed = new CountDownLatch(sendersCount * perSender);

      ActorRef<Publish> ref =
          system.spawn(
              () ->
                  (context, message) -> {
                    if (message.holder().mutableValue != message.expected()) {
                      staleReads.incrementAndGet();
                    }
                    allProcessed.countDown();
                  });

      ExecutorService senders = Executors.newFixedThreadPool(sendersCount);
      try {
        for (int s = 0; s < sendersCount; s++) {
          senders.execute(
              () -> {
                for (int i = 0; i < perSender; i++) {
                  Holder holder = new Holder();
                  holder.mutableValue = i;
                  ref.tell(new Publish(holder, i));
                }
              });
        }
        assertTrue(allProcessed.await(10, TimeUnit.SECONDS), "all messages should be processed");
      } finally {
        senders.shutdown();
      }

      assertEquals(
          0,
          staleReads.get(),
          "actor observed a stale/reordered value of a plain field written before tell()");
    }
  }
}
