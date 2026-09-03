package dev.actorframework.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class MailboxTest {

  @Test
  void deliversMessagesInFifoOrder() {
    Mailbox<Integer> mailbox = new Mailbox<>(16);
    for (int i = 0; i < 10; i++) {
      assertTrue(mailbox.offer(i));
    }
    for (int i = 0; i < 10; i++) {
      assertEquals(i, mailbox.take());
    }
  }

  @Test
  void offerBlocksWhileFullAndUnblocksOnceSpaceFrees() throws InterruptedException {
    Mailbox<Integer> mailbox = new Mailbox<>(1);
    assertTrue(mailbox.offer(1));

    AtomicBoolean secondOfferReturned = new AtomicBoolean(false);
    CountDownLatch threadStarted = new CountDownLatch(1);
    Thread sender =
        new Thread(
            () -> {
              threadStarted.countDown();
              mailbox.offer(2);
              secondOfferReturned.set(true);
            });
    sender.start();
    threadStarted.await();

    // Give the sender a chance to actually block; it must not have returned yet.
    Thread.sleep(100);
    assertFalse(secondOfferReturned.get(), "offer() must block while the mailbox is full");

    assertEquals(1, mailbox.take());
    sender.join(Duration.ofSeconds(2).toMillis());
    assertTrue(secondOfferReturned.get(), "offer() must unblock once space is available");
    assertEquals(2, mailbox.take());
  }

  @Test
  void takeBlocksUntilMessageArrives() {
    Mailbox<String> mailbox = new Mailbox<>(4);
    Thread producer =
        new Thread(
            () -> {
              try {
                Thread.sleep(50);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              mailbox.offer("hello");
            });
    producer.start();

    assertTimeoutPreemptively(Duration.ofSeconds(2), () -> assertEquals("hello", mailbox.take()));
  }

  @Test
  void closeDiscardsQueuedMessagesAndUnblocksTake() {
    Mailbox<String> mailbox = new Mailbox<>(4);
    mailbox.offer("queued-but-never-delivered");

    mailbox.close();

    assertNull(mailbox.take(), "take() must return null once the mailbox is closed and drained");
  }

  @Test
  void offerAfterCloseIsDroppedNotEnqueued() {
    Mailbox<String> mailbox = new Mailbox<>(4);
    mailbox.close();

    boolean enqueued = mailbox.offer("late message");

    assertFalse(enqueued);
  }

  @Test
  void closeUnblocksASenderWaitingOnAFullMailbox() throws InterruptedException {
    Mailbox<Integer> mailbox = new Mailbox<>(1);
    assertTrue(mailbox.offer(1));

    AtomicBoolean offerReturned = new AtomicBoolean(false);
    CountDownLatch threadStarted = new CountDownLatch(1);
    Thread sender =
        new Thread(
            () -> {
              threadStarted.countDown();
              boolean enqueued = mailbox.offer(2);
              assertFalse(enqueued, "a sender unblocked by close() must see its message dropped");
              offerReturned.set(true);
            });
    sender.start();
    threadStarted.await();
    Thread.sleep(100);

    mailbox.close();

    sender.join(Duration.ofSeconds(2).toMillis());
    assertTrue(offerReturned.get(), "close() must unblock a sender stuck on a full mailbox");
  }

  @Test
  void rejectsNonPositiveCapacity() {
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class, () -> new Mailbox<String>(0));
  }

  // TASK-201 (ADR-003): cross-sender ordering guarantees.

  @Test
  void enqueuesInHappensBeforeOrderAcrossSenders() throws InterruptedException {
    Mailbox<String> mailbox = new Mailbox<>(16);
    CountDownLatch firstReturned = new CountDownLatch(1);

    Thread sender1 =
        new Thread(
            () -> {
              mailbox.offer("first");
              firstReturned.countDown();
            });
    sender1.start();
    // sender1's tell() has returned before sender2's begins: a real happens-before relationship.
    firstReturned.await();

    Thread sender2 = new Thread(() -> mailbox.offer("second"));
    sender2.start();

    sender1.join(Duration.ofSeconds(2).toMillis());
    sender2.join(Duration.ofSeconds(2).toMillis());

    assertEquals("first", mailbox.take());
    assertEquals("second", mailbox.take());
  }

  @Test
  void admitsASenderThatIsAlreadyBlockedBeforeALaterArrival() throws InterruptedException {
    for (int iteration = 0; iteration < 30; iteration++) {
      Mailbox<String> mailbox = new Mailbox<>(1);
      assertTrue(mailbox.offer("seed"));

      List<String> admissionOrder = new CopyOnWriteArrayList<>();
      CountDownLatch earlyStarted = new CountDownLatch(1);
      Thread early =
          new Thread(
              () -> {
                earlyStarted.countDown();
                mailbox.offer("early");
                admissionOrder.add("early");
              });
      early.start();
      earlyStarted.await();
      waitUntilBlocked(early);

      // 'late' is pre-warmed and hot-spinning on a plain volatile read, so it can attempt the
      // lock within nanoseconds of 'go' flipping — unlike 'early', which needs a real OS-level
      // unpark-and-reschedule to reacquire the lock after being signaled. Without that head
      // start, thread startup latency alone lets 'early' win even with a non-fair lock, masking
      // the barging bug this test exists to catch.
      AtomicBoolean go = new AtomicBoolean(false);
      CountDownLatch lateSpinning = new CountDownLatch(1);
      Thread late =
          new Thread(
              () -> {
                lateSpinning.countDown();
                while (!go.get()) {
                  Thread.onSpinWait();
                }
                mailbox.offer("late");
                admissionOrder.add("late");
              });
      late.start();
      lateSpinning.await();

      assertEquals("seed", mailbox.take()); // frees the only slot; wakes 'early'
      go.set(true); // release the already-spinning 'late' to race 'early' for that same slot

      // Blocks until whichever of early/late wins the race enqueues, then frees the slot again
      // for the loser.
      assertTimeoutPreemptively(Duration.ofSeconds(2), mailbox::take);

      early.join(Duration.ofSeconds(2).toMillis());
      late.join(Duration.ofSeconds(2).toMillis());

      assertEquals(
          List.of("early", "late"),
          admissionOrder,
          "a sender already blocked must be admitted before one that arrives later (iteration "
              + iteration
              + ")");
    }
  }

  private static void waitUntilBlocked(Thread thread) {
    long deadlineNanos = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (thread.getState() != Thread.State.WAITING
        && thread.getState() != Thread.State.TIMED_WAITING) {
      if (System.nanoTime() > deadlineNanos) {
        throw new AssertionError("thread did not block in time: " + thread.getState());
      }
      Thread.onSpinWait();
    }
  }
}
