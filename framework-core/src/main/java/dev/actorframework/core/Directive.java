package dev.actorframework.core;

/**
 * The four outcomes a {@link SupervisorStrategy} can choose for a failed actor (TASK-402).
 *
 * <p>See {@code docs/decisions/ADR-008-supervision-strategies-and-hierarchies.md} for the full
 * semantics of each directive.
 */
public enum Directive {

  /**
   * The actor keeps running with its existing state, as if the failure never happened. The message
   * that caused the failure is dropped, never redelivered.
   */
  RESUME,

  /**
   * The actor is replaced with a fresh instance (state is reset) and resumes processing. The
   * message that caused the failure is dropped, never redelivered — {@link Actor#preRestart} and
   * {@link Actor#postRestart} are given a chance to react around the swap.
   */
  RESTART,

  /**
   * The actor stops, following the normal lifecycle (TASK-107). Every actor this one supervises
   * (its children, if any) is stopped too.
   */
  STOP,

  /**
   * The failure is handed to this actor's own supervisor: this actor stops, and so does every
   * ancestor up to the root (and, at each ancestor level, every actor that ancestor supervises). An
   * actor with no parent (a top-level actor) has nowhere to escalate to and stops on its own, same
   * as {@link #STOP}.
   */
  ESCALATE
}
