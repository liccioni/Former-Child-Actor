package dev.actorframework.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/**
 * M5: {@link ActorSystem#ask}. See {@code docs/decisions/ADR-015-ask-pattern.md} for why a
 * terminated-target check is the only fast-fail path ({@link AskFailedException}) and every other
 * way a reply might never come surfaces uniformly as a {@link TimeoutException}.
 */
class AskTest {

  @Test
  void askResolvesWithTheTargetsReply() throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<Echo> echo =
          system.spawn(() -> (context, message) -> message.replyTo().tell(message.value()));

      CompletionStage<String> reply =
          system.ask(echo, replyTo -> new Echo("hello", replyTo), Duration.ofSeconds(2));

      assertEquals("hello", reply.toCompletableFuture().get(2, TimeUnit.SECONDS));
    }
  }

  @Test
  void askResolvesWithTheTargetsReplyEvenThoughPostStopAlwaysAttemptsToFailItAfterward()
      throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<Echo> echo =
          system.spawn(() -> (context, message) -> message.replyTo().tell(message.value()));

      CompletionStage<String> reply =
          system.ask(echo, replyTo -> new Echo("hello", replyTo), Duration.ofSeconds(2));

      assertEquals("hello", reply.toCompletableFuture().get(2, TimeUnit.SECONDS));
      // The reply actor's postStop always tries to fail the future on its way out; give it a
      // moment to run and confirm the already-successful result is unharmed (completeExceptionally
      // on an already-completed future is a documented no-op).
      Thread.sleep(100);
      assertEquals("hello", reply.toCompletableFuture().get(2, TimeUnit.SECONDS));
    }
  }

  @Test
  void askTimesOutWhenTheTargetNeverReplies() {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<Echo> blackHole = system.spawn(() -> (context, message) -> {});

      CompletionStage<String> reply =
          system.ask(blackHole, replyTo -> new Echo("hello", replyTo), Duration.ofMillis(200));

      Throwable error =
          assertTimeoutPreemptively(
              Duration.ofSeconds(2),
              () -> reply.handle((result, err) -> err).toCompletableFuture().get());
      assertTrue(error instanceof TimeoutException, "expected TimeoutException but got " + error);
    }
  }

  @Test
  void askFailsFastWithoutWaitingTheFullTimeoutWhenTheTargetIsAlreadyTerminated() throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<Echo> target = system.spawn(() -> (context, message) -> {});
      system.stop(target);
      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (!target.isTerminated()) {
              Thread.sleep(5);
            }
          });

      CompletionStage<String> reply =
          system.ask(target, replyTo -> new Echo("hello", replyTo), Duration.ofSeconds(10));

      // Bounded well under the ask's own 10s timeout: this only passes if the failure was
      // immediate, not "eventually timed out".
      Throwable error =
          assertTimeoutPreemptively(
              Duration.ofSeconds(1),
              () -> reply.handle((result, err) -> err).toCompletableFuture().get());
      assertTrue(
          error instanceof AskFailedException, "expected AskFailedException but got " + error);
    }
  }

  @Test
  void concurrentAsksToTheSameActorEachResolveWithTheirOwnCorrectReply() throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<Echo> echo =
          system.spawn(() -> (context, message) -> message.replyTo().tell(message.value()));

      int askCount = 200;
      ExecutorService askers = Executors.newFixedThreadPool(16);
      CopyOnWriteArrayList<CompletableFuture<Void>> verifications = new CopyOnWriteArrayList<>();
      try {
        CountDownLatch allSubmitted = new CountDownLatch(askCount);
        for (int i = 0; i < askCount; i++) {
          String expected = "msg-" + i;
          askers.execute(
              () -> {
                CompletionStage<String> reply =
                    system.ask(echo, replyTo -> new Echo(expected, replyTo), Duration.ofSeconds(5));
                verifications.add(
                    reply
                        .toCompletableFuture()
                        .thenAccept(actual -> assertEquals(expected, actual)));
                allSubmitted.countDown();
              });
        }
        assertTrue(allSubmitted.await(5, TimeUnit.SECONDS));
      } finally {
        askers.shutdown();
      }

      assertTimeoutPreemptively(
          Duration.ofSeconds(5),
          () -> CompletableFuture.allOf(verifications.toArray(new CompletableFuture<?>[0])).get());
    }
  }

  @Test
  void closingTheSystemWhilePendingAsksAreOutstandingFailsThemInsteadOfHanging() throws Exception {
    ActorSystem system = ActorSystem.start("test");
    ActorRef<Echo> blackHole = system.spawn(() -> (context, message) -> {});

    CompletionStage<String> reply =
        system.ask(blackHole, replyTo -> new Echo("hello", replyTo), Duration.ofSeconds(30));

    system.close();

    Throwable error =
        assertTimeoutPreemptively(
            Duration.ofSeconds(2),
            () -> reply.handle((result, err) -> err).toCompletableFuture().get());
    assertTrue(error instanceof AskFailedException, "expected AskFailedException but got " + error);
  }

  @Test
  void askToAChildActorWorksTheSameAsAskToATopLevelActor() throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      BlockingQueue<ActorRef<Echo>> childRefs = new LinkedBlockingQueue<>();
      system.spawn(
          () ->
              new Actor<Void>() {
                @Override
                public void preStart(ActorContext<Void> context) {
                  childRefs.add(
                      context.spawnChild(
                          () -> (childContext, message) -> message.replyTo().tell(message.value()),
                          "echo-child"));
                }

                @Override
                public void onMessage(ActorContext<Void> context, Void message) {}
              });

      ActorRef<Echo> child = childRefs.poll(2, TimeUnit.SECONDS);
      CompletionStage<String> reply =
          system.ask(child, replyTo -> new Echo("hello", replyTo), Duration.ofSeconds(2));

      assertEquals("hello", reply.toCompletableFuture().get(2, TimeUnit.SECONDS));
    }
  }

  @Test
  void askAfterTheSystemIsShuttingDownThrowsSynchronouslyLikeSpawn() throws InterruptedException {
    ActorSystem system = ActorSystem.start("test");
    CountDownLatch processingStarted = new CountDownLatch(1);
    CountDownLatch blockForever = new CountDownLatch(1);
    ActorRef<Echo> target =
        system.spawn(
            () ->
                (context, message) -> {
                  processingStarted.countDown();
                  awaitUninterruptibly(blockForever);
                });
    target.tell(new Echo("prime", null));
    assertTimeoutPreemptively(Duration.ofSeconds(2), () -> processingStarted.await());

    system.shutdown();

    assertThrows(
        IllegalStateException.class,
        () ->
            system.ask(
                target,
                (ActorRef<String> replyTo) -> new Echo("hello", replyTo),
                Duration.ofSeconds(1)));

    blockForever.countDown();
    system.close();
  }

  @Test
  void theReplyActorIsDeregisteredAfterASuccessfulAsk() throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<Echo> echo =
          system.spawn(() -> (context, message) -> message.replyTo().tell(message.value()));
      int before = system.registeredActorCount();

      CompletionStage<String> reply =
          system.ask(echo, replyTo -> new Echo("hello", replyTo), Duration.ofSeconds(2));
      assertEquals("hello", reply.toCompletableFuture().get(2, TimeUnit.SECONDS));

      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (system.registeredActorCount() > before) {
              Thread.sleep(5);
            }
          });
    }
  }

  @Test
  void theReplyActorIsDeregisteredAfterATimedOutAsk() throws Exception {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<Echo> blackHole = system.spawn(() -> (context, message) -> {});
      int before = system.registeredActorCount();

      system.ask(
          blackHole,
          (ActorRef<String> replyTo) -> new Echo("hello", replyTo),
          Duration.ofMillis(100));

      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            while (system.registeredActorCount() > before) {
              Thread.sleep(5);
            }
          });
    }
  }

  @Test
  void rejectsANonPositiveTimeout() {
    try (ActorSystem system = ActorSystem.start("test")) {
      ActorRef<Echo> echo = system.spawn(() -> (context, message) -> {});

      assertThrows(
          IllegalArgumentException.class,
          () ->
              system.ask(
                  echo, (ActorRef<String> replyTo) -> new Echo("hello", replyTo), Duration.ZERO));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              system.ask(
                  echo,
                  (ActorRef<String> replyTo) -> new Echo("hello", replyTo),
                  Duration.ofSeconds(-1)));
    }
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

  private record Echo(String value, ActorRef<String> replyTo) {}
}
