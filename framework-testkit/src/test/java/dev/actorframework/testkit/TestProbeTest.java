package dev.actorframework.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.actorframework.core.ActorSystem;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ActorSystemExtension.class)
class TestProbeTest {

  @Test
  void receivesAMessageSentDirectlyToItsRef(ActorSystem system) {
    TestProbe<String> probe = TestProbe.create(system);

    probe.ref().tell("hello");

    assertEquals("hello", probe.expectMessage(Duration.ofSeconds(1)));
  }

  @Test
  void expectMessageTimesOutWhenNothingArrives(ActorSystem system) {
    TestProbe<String> probe = TestProbe.create(system);

    assertThrows(AssertionError.class, () -> probe.expectMessage(Duration.ofMillis(100)));
  }

  @Test
  void expectMessageWithExpectedValueFailsOnMismatch(ActorSystem system) {
    TestProbe<String> probe = TestProbe.create(system);
    probe.ref().tell("actual");

    assertThrows(
        AssertionError.class, () -> probe.expectMessage("expected", Duration.ofSeconds(1)));
  }
}
