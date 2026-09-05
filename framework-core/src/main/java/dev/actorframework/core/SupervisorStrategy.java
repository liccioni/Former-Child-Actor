package dev.actorframework.core;

import java.util.function.Function;

/**
 * Decides what happens to an actor after it throws (TASK-402), replacing the fixed M1 default
 * (TASK-107a: log and stop). Provided when an actor is spawned via {@link ActorContext#spawnChild};
 * a top-level actor spawned via {@link ActorSystem#spawn} always uses {@link #stop()}, matching the
 * M1 default exactly and not configurable at that level.
 *
 * <p>A strategy is consulted only by the actor it was given to, on that actor's own dispatcher
 * thread, about that actor's own failures — never across actors or threads. It must be a pure
 * function of the failure: it is not given, and must not need, access to any actor's mutable state.
 *
 * <p>See {@code docs/decisions/ADR-008-supervision-strategies-and-hierarchies.md} for full
 * semantics, including exactly what each {@link Directive} does.
 */
@FunctionalInterface
public interface SupervisorStrategy {

  /** Chooses a directive for the given failure. */
  Directive decide(Throwable failure);

  /** A strategy that always resumes, regardless of the failure. */
  static SupervisorStrategy resume() {
    return decide(failure -> Directive.RESUME);
  }

  /** A strategy that always restarts, regardless of the failure. */
  static SupervisorStrategy restart() {
    return decide(failure -> Directive.RESTART);
  }

  /** A strategy that always stops, regardless of the failure — the M1 (TASK-107a) default. */
  static SupervisorStrategy stop() {
    return decide(failure -> Directive.STOP);
  }

  /** A strategy that always escalates, regardless of the failure. */
  static SupervisorStrategy escalate() {
    return decide(failure -> Directive.ESCALATE);
  }

  /** A strategy that varies its directive by the failure, e.g. by exception type. */
  static SupervisorStrategy decide(Function<Throwable, Directive> decider) {
    return decider::apply;
  }
}
