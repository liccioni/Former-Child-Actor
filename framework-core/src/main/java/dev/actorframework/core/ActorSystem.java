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
    if (shuttingDown.get()) {
      throw new IllegalStateException(
          "Cannot spawn actor '" + name + "': ActorSystem '" + this.name + "' is shutting down");
    }
    ActorCell<T> cell = new ActorCell<>(this, name, factory, null, SupervisorStrategy.stop());
    if (actors.putIfAbsent(name, cell) != null) {
      throw new IllegalArgumentException("An actor named '" + name + "' already exists");
    }
    dispatcher.execute(cell::run);
    return cell.ref();
  }

  /**
   * Spawns a child actor supervised by {@code parentCell} (TASK-402), called from {@link
   * ActorContext#spawnChild}. The child's id is namespaced under its parent's ({@code
   * "parent-id/name"}), reusing the same flat registry as top-level actors.
   *
   * @throws IllegalArgumentException if the parent already has a child with this name
   * @throws IllegalStateException if this system, or the parent actor, is stopping
   */
  <C> ActorRef<C> spawnChild(
      ActorCell<?> parentCell,
      Supplier<Actor<C>> factory,
      String name,
      SupervisorStrategy strategy) {
    if (shuttingDown.get()) {
      throw new IllegalStateException(
          "Cannot spawn child actor '"
              + name
              + "': ActorSystem '"
              + this.name
              + "' is shutting down");
    }
    if (parentCell.isStopRequested()) {
      throw new IllegalStateException(
          "Cannot spawn child actor '"
              + name
              + "': parent actor '"
              + parentCell.id()
              + "' is stopping");
    }
    String id = parentCell.id() + "/" + name;
    ActorCell<C> cell = new ActorCell<>(this, id, factory, parentCell, strategy);
    if (actors.putIfAbsent(id, cell) != null) {
      throw new IllegalArgumentException("An actor named '" + id + "' already exists");
    }
    parentCell.children().add(cell);
    // Closes the race between this spawn and a concurrent parent-stop whose cascade already
    // snapshotted `children` before this cell was added to it (TASK-402).
    if (parentCell.isStopRequested()) {
      cell.requestStop();
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
    ActorCell<?> parentCell = cell.parent();
    if (parentCell != null) {
      parentCell.children().remove(cell);
    }
  }
}
