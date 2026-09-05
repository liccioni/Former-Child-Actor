# Architecture

Status: living document, updated as milestones land. Latest landed: **M4 (TASK-402) — configurable
supervision strategies and actor hierarchies**.

## 1. Actor model

An **actor** is a stateful unit of behavior (`Actor<T>`) that processes messages of a single
type `T` sequentially. Actor state is held in plain, non-synchronized fields: the runtime
guarantees at most one message is ever being processed by a given actor at a time (see
"Concurrency guarantees" below), so no additional locking is needed inside `onMessage`.

An actor is addressed only through an `ActorRef<T>` — a stable, thread-safe handle exposing
`tell(T message)`. There is no way to reach into an actor's state from outside it.

**Hierarchies (TASK-402):** an actor spawned via `ActorContext.spawn` (from within another
actor's `preStart` or `onMessage`) becomes a *child* of the actor that spawned it, which becomes
its *supervisor* — see "Failure handling," below. A child's id is its parent's id, `/`, then its
own name (e.g. `"a/child-1"`), still a single, system-unique string. An actor spawned directly via
`ActorSystem.spawn` has no parent. See
`docs/decisions/ADR-008-supervision-strategies-and-actor-hierarchies.md`.

## 2. Message semantics

* **Delivery is asynchronous and fire-and-forget.** `tell()` returns before the message is
  processed (possibly before it's even enqueued, if the mailbox is full and blocking — see
  below).
* **Per-actor ordering:** messages sent by a single thread to a single actor are delivered in
  the order they were sent (the mailbox is FIFO).
* **Cross-sender causal ordering (TASK-201):** if `tell(A)` returns before `tell(B)` is called on
  the same actor, and that ordering is established by real synchronization between the calling
  threads, `A` is delivered before `B`. Two `tell()` calls with no happens-before relationship
  between them have unspecified relative order — this is inherent to concurrency, not a gap. See
  `docs/decisions/ADR-003-cross-sender-mailbox-ordering.md`.
* **Backpressure admission order (TASK-201):** when the mailbox is full, senders blocked in
  `tell()` are admitted in the order they began waiting (FIFO) once space frees up; a sender that
  arrives later cannot jump ahead of one that is already blocked.

## 3. Concurrency guarantees

At most one invocation of `Actor.onMessage` (and `preStart`/`postStop`) runs at any instant for
a given actor instance. This is achieved with the simplest possible mechanism: **each actor owns
a single dedicated virtual thread for its entire lifetime**, and that thread is the only thread
that ever calls into the actor's code. See `Dispatcher` and ADR-002.

**Dispatch strategy confirmed with data (TASK-303):** the shared-executor alternative ADR-002
deferred to measurement was built and benchmarked against the same workload as TASK-301/302's
data. It loses decisively at every actor count tested — a fixed thread pool plateaus at the core
count, while one-thread-per-actor keeps scaling well past it. See ADR-006 for the full comparison.

**Java Memory Model review (TASK-207):** because all of an actor's own state is touched only by
that one thread, visibility across messages is plain program order — no synchronization is
needed for actor-owned fields. The one real cross-thread boundary is the mailbox: its lock gives
the happens-before edge that makes state a sender established before calling `tell()` visible
inside `onMessage`, and this same edge underlies the ordering guarantees in ADR-003 and the
lifecycle semantics in ADR-004. Lifecycle flags (`terminated`, `stopRequested`,
`shuttingDown`) are `AtomicBoolean`, and the actor registry is a `ConcurrentHashMap`; both give
their own documented cross-thread guarantees. Safe publication of a newly spawned actor to its
dispatcher thread relies on `final` fields plus the JMM's documented happens-before edge for
`Executor.execute()`. The one real pitfall is caller-side: mutating a message object *after*
calling `tell()` is unguarded shared mutable state, same as anywhere else in Java — treat a
message as handed off, not shared, once sent. Full derivation in
`docs/decisions/ADR-005-jmm-review-sequential-processing.md`.

## 4. Mailbox

Each actor has its own `Mailbox<T>`, a FIFO queue.

**Bounds confirmed with benchmark data (TASK-306):** the mailbox is bounded with a default capacity
of 1024 and blocks the sending thread when full ("block-on-full"). TASK-103a picked this only so
early benchmarks would be comparable; TASK-306 built a dedicated benchmark to drive the mailbox to
real backpressure (something TASK-301/302's realistic workloads never did) and confirmed both the
policy and the specific capacity: throughput and blocking latency are invariant to capacity once
saturated, and the current default costs only ~5.4 KB per actor even fully saturated. See
`docs/decisions/ADR-007-mailbox-bounds-confirmed.md` (supersedes
`docs/decisions/mailbox-bounds-provisional.md`).

**Rejected message (TASK-203):** a rejected message is one offered to a mailbox that will never
process it. Exactly three mechanisms produce a rejected message: `offer()` called after the
mailbox is already closed; a sender blocked in `offer()` on a full mailbox, unblocked when
`close()` runs before space frees; and a message already enqueued when `close()` runs, discarded
before delivery (see "Lifecycle," below). Because `ActorRef.tell()` is `void`, a caller cannot
distinguish any of these three from each other, or from successful delivery — "rejected" is a
single, unobservable outcome from the caller's side. Overflow (blocking on a full mailbox) is not
itself rejection: a blocked sender's message is still delivered once space frees, unless the actor
stops while it is waiting. See
`docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md`.

## 5. Lifecycle

1. `ActorSystem.spawn(...)` constructs the actor, registers it, and starts its dispatcher
   thread, which calls `preStart` and then enters the message loop.
2. An actor stops when:
   * `ActorSystem.stop(ref)` is called explicitly, or
   * an uncaught exception escapes `preStart` or `onMessage` (see "Failure handling" below), or
   * the owning `ActorSystem` shuts down.
3. On stop, the mailbox is closed immediately: any messages still queued at that moment are
   **rejected, not delivered** (see "Mailbox," above), and any sender currently blocked in
   `tell()` on a full mailbox is unblocked (its message is also rejected — see "Mailbox,"
   above). The actor finishes the message it is currently processing, if any, then `postStop`
   runs (best-effort — an exception there is logged and ignored, since the actor is already
   terminating).
4. Once `postStop` completes, the actor is terminated: `ActorRef.isTerminated()` becomes `true`,
   and every subsequent `tell()` is silently rejected (see "Mailbox," above). The `ActorRef`
   itself remains a valid, inert object — it is never invalidated or reused for a different
   actor.
5. **A parent is not terminated until its children are (TASK-402).** Stopping a parent
   immediately requests that every child of it stop too (recursively, through the whole subtree);
   but the parent itself does not finish terminating — `postStop` does not run, and
   `isTerminated()` does not become `true` — until every child it still has has itself finished
   terminating. This does not apply to a mid-flight restart (see "Failure handling," below): a
   restarting actor asks its children to stop but does not wait for them, since waiting there can
   deadlock (`docs/decisions/ADR-008-supervision-strategies-and-actor-hierarchies.md` §3).

This is a deliberately simple stop model for M1: no draining, no "let the mailbox empty out
first" mode. `ActorSystem.close()` (or the try-with-resources form) stops every actor and blocks
until all of them have finished terminating.

## 6. Failure handling (TASK-402)

A failure — an uncaught exception from `onMessage` or `preStart` — is resolved by asking the
failing actor's parent, if it has one, for a `SupervisorDirective`: `RESUME` (discard the poison
message, keep running), `RESTART` (fresh actor instance from the same factory, `preStart` runs
again, mailbox otherwise untouched), `STOP` (normal lifecycle, as above), or `ESCALATE` (stop this
actor regardless, and ask the parent's own parent to resolve the same failure, recursively). The
parent decides via `Actor.supervisorStrategy(Throwable)`, called on the parent's own dispatch
thread — in strict turn with its own messages — so it is safe for an override to read and write
the parent's own state. **An actor with no parent always resolves its own failure to `STOP`** —
this is the fixed M1 default (TASK-107a) — because there is no supervisor to ask; a
`supervisorStrategy` override changes nothing for an actor that never opts into a parent
relationship, or for any actor that doesn't override it (the default is `STOP`). Restarting or
stopping an actor stops its own children too (cascading, as in "Lifecycle" above); restarting does
not restart them. See `docs/decisions/ADR-008-supervision-strategies-and-actor-hierarchies.md` for
the full decision, including why this is resolved on the parent's own thread rather than the
failing child's.

**Poison message (TASK-203, re-examined per ADR-004 for M4):** the message whose processing
triggers a failure — the one `onMessage` or `preStart` was handling when it threw — is termed a
poison message. It is never redelivered, under any directive: `RESUME` and `RESTART` both discard
it and move on to whatever is queued next; `STOP` and `ESCALATE` terminate the actor without it.
This holds regardless of how many times an actor is restarted. See
`docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md` and
`docs/decisions/ADR-008-supervision-strategies-and-actor-hierarchies.md`.

## 7. API philosophy

* Small core, own the stack, local-first.
* Complexity is opt-in: nothing beyond `ActorSystem`, `ActorRef`, `Actor`, `ActorContext`,
  `Mailbox`, `Dispatcher`, and `SupervisorDirective` (M4, TASK-402) exists yet, on purpose.
* No API is finalized until proven through implementation and tests — in particular, `ask()`
  does not exist yet; it is designed in ADR-015 (M5) before it is added.

## Roadmap

See the milestone table and dependency graph in the original design document
(`docs/decisions/` for individual decisions, and the project tracker for the full task list).
The short version: correct local semantics → efficient local runtime → reliable lifecycle →
persistence → transport → remote actors → cluster → production hardening → ecosystem.
