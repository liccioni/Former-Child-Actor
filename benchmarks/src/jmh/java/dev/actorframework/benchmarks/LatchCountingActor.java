package dev.actorframework.benchmarks;

import dev.actorframework.core.Actor;
import dev.actorframework.core.ActorContext;
import java.util.concurrent.CountDownLatch;

/**
 * Counts down the latch carried in each message.
 *
 * <p>{@code ask()} does not exist yet (ADR-015, M5), so benchmarks that need to observe when a
 * message has actually been processed — not just enqueued — carry their own completion signal
 * instead of a request-response call.
 */
final class LatchCountingActor implements Actor<CountDownLatch> {

  @Override
  public void onMessage(ActorContext<CountDownLatch> context, CountDownLatch message) {
    message.countDown();
  }
}
