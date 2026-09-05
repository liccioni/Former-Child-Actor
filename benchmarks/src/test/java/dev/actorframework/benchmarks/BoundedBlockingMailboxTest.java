package dev.actorframework.benchmarks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * TASK-306: before trusting any benchmark number from {@link BoundedBlockingMailbox}, this proves
 * it behaves the same as {@code framework-core}'s real {@code Mailbox} on the properties this
 * benchmark's conclusions depend on: FIFO delivery, and blocking (not dropping or rejecting) while
 * full until space frees or the mailbox closes.
 */
class BoundedBlockingMailboxTest {

  @Test
  void deliversMessagesInFifoOrder() {
    BoundedBlockingMailbox<Integer> mailbox = new BoundedBlockingMailbox<>(16);
    for (int i = 0; i < 10; i++) {
      assertTrue(mailbox.offer(i));
    }
    for (int i = 0; i < 10; i++) {
      assertEquals(i, mailbox.take());
    }
  }

  @Test
  void offerBlocksWhileFullAndUnblocksOnceSpaceFrees() throws InterruptedException {
    BoundedBlockingMailbox<Integer> mailbox = new BoundedBlockingMailbox<>(1);
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

    Thread.sleep(100);
    assertFalse(secondOfferReturned.get(), "offer() must block while the mailbox is full");

    assertEquals(1, mailbox.take());
    sender.join(Duration.ofSeconds(2).toMillis());
    assertTrue(secondOfferReturned.get(), "offer() must unblock once space is available");
    assertEquals(2, mailbox.take());
  }

  @Test
  void closeUnblocksASenderWaitingOnAFullMailbox() throws InterruptedException {
    BoundedBlockingMailbox<Integer> mailbox = new BoundedBlockingMailbox<>(1);
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
  void neverExceedsItsConfiguredCapacity() throws InterruptedException {
    int capacity = 8;
    BoundedBlockingMailbox<Integer> mailbox = new BoundedBlockingMailbox<>(capacity);
    for (int i = 0; i < capacity; i++) {
      assertTrue(mailbox.offer(i));
    }

    AtomicBoolean overflowOfferReturned = new AtomicBoolean(false);
    Thread sender =
        new Thread(
            () -> {
              mailbox.offer(capacity);
              overflowOfferReturned.set(true);
            });
    sender.start();

    Thread.sleep(100);
    assertFalse(overflowOfferReturned.get(), "offer() must block once capacity is reached");

    mailbox.take();
    sender.join(Duration.ofSeconds(2).toMillis());
    assertTrue(overflowOfferReturned.get());
  }
}
