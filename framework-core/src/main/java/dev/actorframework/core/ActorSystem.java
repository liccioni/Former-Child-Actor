package dev.actorframework.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * The runtime responsible for creating, scheduling and terminating actors (TASK-104).
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (ActorSystem system = ActorSystem.start()) {
 *     ActorRef<OrderCommand> orders = system.spawn(OrderActor::new);
 *     orders.tell(new CreateOrder(...));
 * }
 * }</pre>
 */
public final class ActorSystem implements AutoCloseable {

  private final String name;
  private final Dispatcher dispatcher;
  private final Map<String, ActorCell<?>> actors = new ConcurrentHashMap<>();
  private final AtomicLong anonymousActorCount = new AtomicLong();
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

  private ActorSystem(String name) {
    this.name = name;
    this.dispatcher = new Dispatcher();
  }

  public static ActorSystem start() {
    return start("actor-system");
  }

  public static ActorSystem start(String name) {
    return new ActorSystem(name);
  }

  public String name() {
    return name;
  }

  /** Spawns a new actor with an automatically generated name. */
  public <T> ActorRef<T> spawn(Supplier<Actor<T>> factory) {
    return spawn(factory, "actor-" + anonymousActorCount.incrementAndGet());
  }

  /**
   * Spawns a new actor with the given name.
   *
   * @throws IllegalArgumentException if an actor with this name already exists
   * @throws IllegalStateException if this system is shutting down
   */
  public <T> ActorRef<T> spawn(Supplier<Actor<T>> factory, String name) {
    return spawnInternal(null, factory, name);
  }

  /**
   * Spawns a new child of {@code parent} (TASK-402). Package-private: reached only through {@link
   * ActorContext#spawn}, never called directly by application code.
   *
   * @throws IllegalArgumentException if {@code parent} already has a child with this name
   * @throws IllegalStateException if this system is shutting down
   */
  <T> ActorRef<T> spawnChild(ActorCell<?> parent, Supplier<Actor<T>> factory, String name) {
    return spawnInternal(
        parent, factory, name != null ? name : "actor-" + anonymousActorCount.incrementAndGet());
  }

  private <T> ActorRef<T> spawnInternal(
      ActorCell<?> parent, Supplier<Actor<T>> factory, String name) {
    if (shuttingDown.get()) {
      throw new IllegalStateException(
          "Cannot spawn actor '" + name + "': ActorSystem '" + this.name + "' is shutting down");
    }
    String id = parent == null ? name : parent.id() + "/" + name;
    ActorCell<T> cell = new ActorCell<>(this, id, factory, parent);
    if (actors.putIfAbsent(id, cell) != null) {
      throw new IllegalArgumentException("An actor named '" + id + "' already exists");
    }
    if (parent != null) {
      parent.addChild(cell);
    }
    dispatcher.execute(cell::run);
    return cell.ref();
  }

  /**
   * Requests that the actor behind {@code ref} stop. See {@link ActorRef} for termination
   * semantics. A no-op if the actor does not belong to this system or has already stopped.
   */
  public void stop(ActorRef<?> ref) {
    ActorCell<?> cell = actors.get(ref.id());
    if (cell != null) {
      cell.requestStop();
    }
  }

  /**
   * Stops accepting new actors and requests that every currently running actor stop. Does not
   * block; use {@link #close()} to also wait for shutdown to complete.
   */
  public void shutdown() {
    if (!shuttingDown.compareAndSet(false, true)) {
      return;
    }
    for (ActorCell<?> cell : actors.values()) {
      cell.requestStop();
    }
  }

  public boolean isShuttingDown() {
    return shuttingDown.get();
  }

  /** Shuts down the system and blocks until every actor has finished terminating. */
  @Override
  public void close() {
    shutdown();
    dispatcher.close();
  }

  void deregister(ActorCell<?> cell) {
    actors.remove(cell.id());
  }
}
