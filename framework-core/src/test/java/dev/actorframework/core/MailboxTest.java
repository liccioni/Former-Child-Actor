package dev.actorframework.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
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
}
