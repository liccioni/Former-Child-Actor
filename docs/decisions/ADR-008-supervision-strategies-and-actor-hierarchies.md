# ADR-008: Configurable supervision strategies and actor hierarchies

* Status: Accepted
* Written during: M4 (TASK-402)
* Supersedes: `docs/architecture.md` §6's non-configurable default (TASK-107a) and the poison-message
  guarantee in `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md` §2, as that
  ADR itself anticipated and required

## Context

TASK-107a (M1) gave every actor exactly one, non-configurable failure response: log the failure
and stop that actor alone. `docs/architecture.md` §6 and `AGENTS.md` both flagged this as
provisional, explicitly deferring configurable strategies (Resume, Restart, Stop, Escalate) and
actor hierarchies to M4 (TASK-402). ADR-004 went further and made re-examining the poison-message
guarantee mandatory once Restart existed: "M4 (TASK-402) introduces configurable supervision,
including Restart, and must explicitly re-examine whether a restarted actor can receive the same
poison message again."

No hierarchy concept exists yet: `ActorSystem.actors` is a flat registry, `ActorContext` exposes
only `self()` and `system()`, and every actor is a top-level sibling. Supervision without
hierarchy is meaningless — Resume/Restart/Stop/Escalate are all things a *supervisor* decides
about a *child*, so this ADR treats "actor hierarchies" and "configurable supervision" as one
decision, not two, matching how the GitHub issue for this task frames it.

## Decision

### 1. Hierarchy

* `ActorContext<T>` gains `<C> ActorRef<C> spawn(Supplier<Actor<C>> factory, String name)` and an
  anonymous-name overload, letting a running actor spawn children of its own. The spawning actor
  becomes the child's supervisor.
* `ActorContext<T>` gains `Optional<ActorRef<?>> parent()` — empty for an actor spawned directly
  via `ActorSystem.spawn`, present for a child.
* A child's id is its parent's id, `/`, then the given (or generated) name — e.g. `"a/child-1"`.
  This is still a single, stable, system-unique string (`ActorRef.id()`'s existing contract is
  unchanged), it just now reveals the hierarchy for free — cheap inspectability, no new
  `ActorPath` type needed.
* **Stopping a parent stops its children.** `ActorCell.requestStop()` cascades to every currently
  registered child immediately (a child added afterward, racing the cascade, is caught by the same
  check at registration time). A parent is not considered terminated — `postStop` has not run, and
  `isTerminated()` is not yet `true` — until every child it still has has itself finished
  terminating. This is the one hierarchy invariant worth blocking on: without it, a "terminated"
  parent could still have live descendants running underneath it, which would make termination an
  unreliable signal for cleanup.

### 2. Supervision

* `Actor<T>` gains `default SupervisorDirective supervisorStrategy(Throwable failure)`, defaulting
  to `STOP`. This is asked of a **parent** about a **child's** failure — never about an actor's own
  failure. An actor with no parent (spawned via `ActorSystem.spawn`) always resolves its own
  failure to `STOP`, exactly like TASK-107a, because there is no supervisor to ask; a default
  override changes nothing for anyone who doesn't opt in, so this is behavior-preserving for every
  M1–M3 actor and every root actor going forward.
* `SupervisorDirective` is `RESUME`, `RESTART`, `STOP`, or `ESCALATE`:
  * **RESUME** — the poison message is discarded; the child keeps its existing actor instance and
    state and continues taking messages.
  * **RESTART** — the child's actor instance is discarded and replaced with a fresh one from its
    original factory, then `preStart` runs again; the mailbox itself is untouched (messages queued
    behind the poison one are still delivered, once, in order). The child's own children (if any)
    are stopped, not restarted — restarting resets this actor's own state, and nothing currently
    needs a restarted actor's subtree to survive that reset (YAGNI; revisit with a real use case,
    per the same reasoning ADR-004 used for deferring rejection-signal detail).
  * **STOP** — the child stops, following the normal lifecycle (§5 of `docs/architecture.md`).
  * **ESCALATE** — the child is stopped regardless of what happens next; additionally, the
    *parent* now treats the same `Throwable` as if it were its own failure, asking its own
    supervisor (the grandparent) to resolve it via this exact same decision process, recursively.
    Escalating never resurrects the originally-failing child — only an ancestor further up the
    chain is Resumed/Restarted/Stopped by the eventual resolution. An actor with no parent that
    escalates (there is nothing further up) simply stops, same as any unhandled failure.
* **The poison message is never redelivered, under any directive** (this is the re-examination
  ADR-004 required). Resume discards it and moves on; Restart discards it and starts the fresh
  instance on whatever comes next; Stop and Escalate terminate the actor without it. `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md`'s original guarantee ("processed at
  most once, never redelivered") holds exactly as before — restart does not change what "poison
  message" means, only what happens to the actor after it.
* No restart limit or backoff exists in M4. A `supervisorStrategy` that always returns `RESTART`
  for a persistently-failing `preStart` can restart-loop indefinitely — that risk is confined to
  the failing actor's own dedicated thread and its subtree, the same accepted risk class as an
  actor whose `onMessage` never returns (`docs/architecture.md` §3). Adding a restart-limit policy
  ahead of a concrete need for one would be exactly the kind of premature API surface `AGENTS.md`
  asks this project not to add.

### 3. Concurrency: why a child's failure is resolved on the *parent's* own thread

`docs/decisions/ADR-005-jmm-review-sequential-processing.md` established that an actor's own
fields need no synchronization only because exactly one thread — that actor's own dispatcher
thread — ever touches them. A realistic `supervisorStrategy` override needs to read and write the
parent's own state (a common case: "restart up to N times, then stop"), so the decision must run
on the parent's own dispatch thread, in strict turn with its own messages, or ADR-005's guarantee
breaks the moment someone writes exactly that override.

To get this without inventing a second blocking channel that would need its own wake-up mechanism,
a child's failure report is delivered through the **same mailbox** the parent's own messages use,
wrapped in an internal, package-private `Envelope<T>` (`Message<T>` for a normal `tell()`,
`ChildFailure<T>` for a report). The parent's dispatch loop already blocks on that mailbox; a
`ChildFailure` envelope wakes it exactly the way a real message would, is dequeued in the same FIFO
order, and is resolved by calling `actor.supervisorStrategy(failure)` — safely, because that call
happens on the one thread already licensed to touch this actor's fields. The reporting child blocks
(via a `CompletableFuture<SupervisorDirective>` carried in the envelope) until its parent gets
around to resolving it; if the parent's mailbox is already closed by then (rejected, per
ADR-004's rejected-message vocabulary) or closes with the envelope still queued and undelivered,
the child is unblocked with `STOP` rather than left waiting forever for a supervisor that is gone.

**This is also why Restart does not wait for its children to finish stopping**, unlike true
termination. If it did, the following is a real deadlock: actor P is restarting (its own failure
was resolved as Restart) and, as part of that, synchronously waits for child C to finish stopping;
meanwhile C has *already* enqueued its own unrelated `ChildFailure` envelope into P's mailbox and is
blocked waiting for P to resolve it — but P is blocked waiting for C, and P's dispatch thread is
the only thing that will ever resolve that envelope. Restart therefore only requests that children
stop (fire-and-forget) and moves on; it does not block on them. True termination is different and
provably safe to wait on: by the time a cell reaches "wait for my children," its own mailbox is
already closed (this always precedes it — see `docs/architecture.md` §5), so no *new*
`ChildFailure` from any of its children can arrive un-resolved — every one either was already
processed in an earlier turn or was resolved to `STOP` at close time.

## Alternatives considered

* **A stateless decider (`Throwable -> SupervisorDirective` with no thread-affinity guarantee)**,
  calling `supervisorStrategy` directly from the child's own thread. Rejected: this is exactly the
  kind of cross-thread access to actor-owned state ADR-005 documents as unsafe. It would work for
  the trivial always-return-the-same-value case and silently break the moment anyone wrote a
  stateful decider — an invitation to exactly the class of bug this project's ADRs exist to rule
  out, not a milestone-appropriate simplification.
* **A separate control queue for `ChildFailure` reports**, independent of the user-facing mailbox.
  Rejected: nothing wakes a dispatch thread parked in `Mailbox.take()` waiting for the next user
  message, so a report could sit unresolved indefinitely if the parent is otherwise quiet. Folding
  it into the existing mailbox reuses the one wake-up mechanism that already exists instead of
  building a second one.
* **Restarting a child also restarts (rather than stops) its own children**, preserving a deeper
  subtree across a restart. Rejected for M4: no current use case needs it, and it would require the
  same subtree to be recreated coherently (new instances, replayed or dropped mailboxes, and so
  on) with no concrete requirement driving the design — YAGNI, revisit if a real need appears.
* **Restart waits for its children to finish stopping, matching true termination's behavior**,
  for consistency. Rejected: this is the deadlock described above. Not waiting is not a
  simplification of convenience — waiting is unsound.
* **Exposing a new `ActorPath` type instead of a plain hierarchical id string.** Rejected: nothing
  in this milestone needs to do more with a path than read it (e.g. in logs); a plain `String`
  already satisfies `ActorRef.id()`'s existing contract, and a dedicated type ahead of a concrete
  need repeats the mistake ADR-004 flagged when it declined to split "rejected" into a richer
  taxonomy no caller could yet observe.

## Consequences

* `docs/architecture.md` §6 is rewritten to describe configurable supervision and hierarchies
  instead of "one, non-configurable default"; §5 (Lifecycle) gains the parent-waits-for-children
  rule.
* `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md` §2's poison-message
  guarantee is reaffirmed, not broken, by this ADR — restart never redelivers the poison message.
* `AGENTS.md`'s "minimal failure-handling default... intentionally not configurable in M1" line is
  updated to point here now that M4 has landed.
* Any future milestone that wants a restarted actor's children to survive the restart, a restart
  limit/backoff policy, or a richer rejection/decision signal on the child's side must engage with
  this ADR's reasoning explicitly (per the same rule this ADR is itself honoring for ADR-004),
  not silently extend past it.
