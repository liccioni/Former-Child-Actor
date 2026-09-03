package dev.actorframework.examples.helloworld;

import dev.actorframework.core.Actor;
import dev.actorframework.core.ActorContext;
import dev.actorframework.core.ActorRef;
import dev.actorframework.core.ActorSystem;
import java.util.concurrent.CountDownLatch;

/**
 * TASK-108 — the smallest complete application: start a system, spawn one actor, send it one
 * message, and observe the result.
 *
 * <p>The example waits on a {@link CountDownLatch} rather than sleeping arbitrarily: message
 * delivery is asynchronous, and {@link ActorSystem#close()} discards any message still queued at
 * shutdown time (TASK-107), so a real reply is the only way to know the greeting actually happened
 * before the system is closed.
 */
public final class HelloWorldApp {

  sealed interface Greeting permits Greet {}

  record Greet(String name, ActorRef<String> replyTo) implements Greeting {}

  static final class GreeterActor implements Actor<Greeting> {
    @Override
    public void onMessage(ActorContext<Greeting> context, Greeting message) {
      if (message instanceof Greet greet) {
        greet.replyTo().tell("Hello, " + greet.name() + "!");
      }
    }
  }

  static final class PrinterActor implements Actor<String> {
    private final CountDownLatch done;

    PrinterActor(CountDownLatch done) {
      this.done = done;
    }

    @Override
    public void onMessage(ActorContext<String> context, String message) {
      System.out.println(message);
      done.countDown();
    }
  }

  public static void main(String[] args) throws InterruptedException {
    try (ActorSystem system = ActorSystem.start("hello-world")) {
      CountDownLatch done = new CountDownLatch(1);
      ActorRef<String> printer = system.spawn(() -> new PrinterActor(done), "printer");
      ActorRef<Greeting> greeter = system.spawn(GreeterActor::new, "greeter");

      greeter.tell(new Greet("World", printer));

      done.await();
    }
  }

  private HelloWorldApp() {}
}
