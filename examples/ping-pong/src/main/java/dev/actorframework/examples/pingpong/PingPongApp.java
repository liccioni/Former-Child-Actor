package dev.actorframework.examples.pingpong;

import dev.actorframework.core.Actor;
import dev.actorframework.core.ActorContext;
import dev.actorframework.core.ActorRef;
import dev.actorframework.core.ActorSystem;
import java.util.concurrent.CountDownLatch;

/**
 * TASK-109 — two actors exchanging messages, each owning its own mutable state, demonstrating that
 * per-actor state needs no synchronization thanks to the sequential-processing guarantee
 * (TASK-106).
 */
public final class PingPongApp {

  private static final int ROUNDS = 5;

  sealed interface Message permits Ping, Pong {}

  record Ping(int round, ActorRef<Message> replyTo) implements Message {}

  record Pong(int round, ActorRef<Message> replyTo) implements Message {}

  /** Holds its own round counter as plain (unsynchronized) actor state. */
  static final class PingActor implements Actor<Message> {
    private int roundsPlayed = 0;
    private final CountDownLatch done;

    PingActor(CountDownLatch done) {
      this.done = done;
    }

    @Override
    public void onMessage(ActorContext<Message> context, Message message) {
      if (message instanceof Pong pong) {
        roundsPlayed++;
        System.out.println("ping: received pong #" + pong.round());
        if (roundsPlayed >= ROUNDS) {
          context.system().stop(context.self());
          done.countDown();
        } else {
          pong.replyTo().tell(new Ping(roundsPlayed, context.self()));
        }
      }
    }
  }

  static final class PongActor implements Actor<Message> {
    @Override
    public void onMessage(ActorContext<Message> context, Message message) {
      if (message instanceof Ping ping) {
        System.out.println("pong: received ping #" + ping.round());
        ping.replyTo().tell(new Pong(ping.round(), context.self()));
      }
    }
  }

  public static void main(String[] args) throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("ping-pong")) {
      CountDownLatch done = new CountDownLatch(1);
      ActorRef<Message> pong = system.spawn(PongActor::new, "pong");
      ActorRef<Message> ping = system.spawn(() -> new PingActor(done), "ping");

      pong.tell(new Ping(0, ping));

      done.await();
    }
  }

  private PingPongApp() {}
}
