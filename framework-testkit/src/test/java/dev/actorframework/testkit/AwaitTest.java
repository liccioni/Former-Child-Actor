package dev.actorframework.testkit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.actorframework.core.ActorRef;
import dev.actorframework.core.ActorSystem;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ActorSystemExtension.class)
class AwaitTest {

  @Test
  void awaitTrueReturnsAsSoonAsTheConditionHolds() {
    AtomicBoolean flag = new AtomicBoolean(false);
    new Thread(
            () -> {
              try {
                Thread.sleep(50);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              flag.set(true);
            })
        .start();

    Await.awaitTrue(Duration.ofSeconds(2), "flag should flip", flag::get);

    assertTrue(flag.get());
  }

  @Test
  void awaitTrueThrowsOnTimeout() {
    assertThrows(
        AssertionError.class,
        () -> Await.awaitTrue(Duration.ofMillis(50), "never true", () -> false));
  }

  @Test
  void awaitTerminatedObservesAnActorThatFailed(ActorSystem system) {
    ActorRef<String> flaky =
        system.spawn(
            () ->
                (context, message) -> {
                  throw new RuntimeException("boom");
                });

    flaky.tell("trigger");

    Await.awaitTerminated(flaky, Duration.ofSeconds(2));
  }
}
