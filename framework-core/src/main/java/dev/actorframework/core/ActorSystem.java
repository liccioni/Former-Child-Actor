package dev.actorframework.core;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
  private final JournalStore store;

  private ActorSystem(String name, JournalStore store) {
    this.name = name;
    this.store = store;
    this.dispatcher = new Dispatcher();
  }

  public static ActorSystem start() {
    return start("actor-system");
  }

  public static ActorSystem start(String name) {
    return new ActorSystem(name, null);
  }

  /**
   * Starts a system configured with {@code store} for persistent actors (TASK-601): an actor opts
   * into journaling by being spawned via {@link #spawn(Supplier, String, MessageCodec)}. {@link
   * #spawn(Supplier)}/{@link #spawn(Supplier, String)} are unaffected — non-persistent actors never
   * touch {@code store}. See {@code docs/decisions/ADR-016-durable-actor-journal-and-recovery.md}.
   *
   * @throws IllegalArgumentException if {@code store} is {@code null}
   */
  public static ActorSystem start(String name, JournalStore store) {
    if (store == null) {
      throw new IllegalArgumentException("store must not be null");
    }
    return new ActorSystem(name, store);
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
    ActorCell<T> cell =
        new ActorCell<>(this, name, factory, null, SupervisorStrategy.stop(), null, null);
    if (actors.putIfAbsent(name, cell) != null) {
      throw new IllegalArgumentException("An actor named '" + name + "' already exists");
    }
    dispatcher.execute(cell::run);
    return cell.ref();
  }

  /**
   * Spawns a new persistent top-level actor with the given name (TASK-601): passing {@code codec}
   * opts it into journaling via this system's configured {@link JournalStore}. On spawn, the
   * actor's journal (if any records exist) is replayed via {@code onMessage} before any live
   * message is processed, reconstructing its history from previous runs. Every live message is
   * durably appended to the journal <em>before</em> {@code onMessage} runs (write-ahead), so it
   * survives a process restart even if processing was interrupted mid-message. Existing {@link
   * #spawn(Supplier)}/{@link #spawn(Supplier, String)} overloads are unaffected — an actor spawned
   * through them never touches a journal. See {@code
   * docs/decisions/ADR-016-durable-actor-journal-and-recovery.md}.
   *
   * @throws IllegalStateException if this system has no configured {@link JournalStore}, or is
   *     shutting down
   * @throws IllegalArgumentException if an actor with this name already exists
   */
  public <T> ActorRef<T> spawn(Supplier<Actor<T>> factory, String name, MessageCodec<T> codec) {
    if (store == null) {
      throw new IllegalStateException(
          "Cannot spawn persistent actor '"
              + name
              + "': ActorSystem '"
              + this.name
              + "' has no configured JournalStore");
    }
    if (shuttingDown.get()) {
      throw new IllegalStateException(
          "Cannot spawn actor '" + name + "': ActorSystem '" + this.name + "' is shutting down");
    }
    Journal journal = store.open(name);
    ActorCell<T> cell =
        new ActorCell<>(this, name, factory, null, SupervisorStrategy.stop(), journal, codec);
    if (actors.putIfAbsent(name, cell) != null) {
      journal.close();
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
    ActorCell<C> cell = new ActorCell<>(this, id, factory, parentCell, strategy, null, null);
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
   * Sends {@code target} a message built by {@code messageFactory} and returns a {@link
   * CompletionStage} of its reply (M5, {@code docs/decisions/ADR-015-ask-pattern.md}). {@code
   * messageFactory} is given a reply-to {@link ActorRef} to embed in the outgoing message; whatever
   * the first message sent to that reply-to ref is becomes the stage's result.
   *
   * <p>Fails fast with {@link AskFailedException}, without sending anything, if {@code target} has
   * already terminated — but a target that terminates in the window between that check and actual
   * delivery is indistinguishable from a slow-but-alive one: this only ever completes exceptionally
   * with {@link AskFailedException} for a case knowable up front, never for "no reply yet". Every
   * other way a reply might never come (a slow target, a request dropped by a supervised restart, a
   * target stopped by a supervision cascade, ...) surfaces uniformly as a {@link
   * java.util.concurrent.TimeoutException} once {@code timeout} elapses — see ADR-015 for why this
   * is deliberately not a richer failure taxonomy.
   *
   * @throws IllegalArgumentException if {@code timeout} is not positive
   * @throws IllegalStateException if this system is shutting down (same as {@link #spawn})
   */
  public <REQ, RES> CompletionStage<RES> ask(
      ActorRef<REQ> target, Function<ActorRef<RES>, REQ> messageFactory, Duration timeout) {
    if (timeout.isNegative() || timeout.isZero()) {
      throw new IllegalArgumentException("timeout must be positive: " + timeout);
    }
    if (target.isTerminated()) {
      return CompletableFuture.failedFuture(
          new AskFailedException("Cannot ask '" + target.id() + "': it has already terminated"));
    }
    CompletableFuture<RES> future = new CompletableFuture<>();
    ActorRef<RES> replyTo = spawn(() -> new AskReplyActor<>(future));
    // Fires exactly once, however `future` ends up completed (a real reply, postStop's catch-all
    // below, or orTimeout() itself) — complete()/completeExceptionally() are no-ops once a future
    // is already completed, so there is no double-completion hazard here.
    future.whenComplete((result, error) -> stop(replyTo));
    target.tell(messageFactory.apply(replyTo));
    return future.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * The one-shot reply channel behind {@link #ask}: completes {@code future} with the first (and
   * only) message it ever receives, then stops itself.
   */
  private static final class AskReplyActor<RES> implements Actor<RES> {
    private final CompletableFuture<RES> future;

    AskReplyActor(CompletableFuture<RES> future) {
      this.future = future;
    }

    @Override
    public void onMessage(ActorContext<RES> context, RES message) {
      future.complete(message);
      // Races harmlessly with the whenComplete-triggered stop() above: ActorCell.requestStop() is
      // a no-op past its first call.
      context.system().stop(context.self());
    }

    /**
     * Runs whenever this actor stops, for any reason. On the happy path {@code future} is already
     * complete by the time this runs, making this a documented no-op; the one path that actually
     * needs it is {@link ActorSystem#close()}/{@link ActorSystem#shutdown()} running while this ask
     * is still pending, which would otherwise leave the returned stage incomplete forever.
     */
    @Override
    public void postStop(ActorContext<RES> context) {
      future.completeExceptionally(
          new AskFailedException(
              "The reply channel for this ask() stopped before a reply arrived"));
    }
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
    if (store != null) {
      store.close();
    }
  }

  void deregister(ActorCell<?> cell) {
    actors.remove(cell.id());
    ActorCell<?> parentCell = cell.parent();
    if (parentCell != null) {
      parentCell.children().remove(cell);
    }
  }

  /** Test support only: the number of currently-registered actors, top-level and children alike. */
  int registeredActorCount() {
    return actors.size();
  }
}
