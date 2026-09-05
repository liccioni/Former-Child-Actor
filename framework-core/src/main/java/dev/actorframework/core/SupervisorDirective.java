package dev.actorframework.core;

/**
 * The outcome a supervisor chooses when one of its children fails (TASK-402).
 *
 * <p>Returned from {@link Actor#supervisorStrategy}, which a parent uses to decide what happens to
 * a child that threw from {@link Actor#preStart} or {@link Actor#onMessage}. See {@code
 * docs/decisions/ADR-008-supervision-strategies-and-actor-hierarchies.md}.
 */
public enum SupervisorDirective {

  /**
   * Discard the message that caused the failure (the poison message) and let the child keep running
   * with its existing actor instance and state.
   */
  RESUME,

  /**
   * Replace the child's actor instance with a fresh one from its original factory and run {@link
   * Actor#preStart} again; the child's mailbox is untouched, so messages queued behind the poison
   * message are still delivered afterward. Any children the child itself had are stopped, not
   * restarted.
   */
  RESTART,

  /** Stop the child, following the normal lifecycle. */
  STOP,

  /**
   * Stop the child regardless of what happens next, and additionally treat the failure as this
   * supervisor's own: ask its own parent to decide, recursively, using this same process.
   */
  ESCALATE
}
