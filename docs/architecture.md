# Architecture

Status: living document, updated as milestones land. Current milestone: **M4 — Supervision**.

## 1. Actor model

An **actor** is a stateful unit of behavior (`Actor<T>`) that processes messages of a single
type `T` sequentially. Actor state is held in plain, non-synchronized fields: the runtime
guarantees at most one message is ever being processed by a given actor at a time (see
"Concurrency guarantees" below), so no additional locking is needed inside `onMessage`.

An actor is addressed only through an `ActorRef<T>` — a stable, thread-safe handle exposing
`tell(T message)`. There is no way to reach into an actor's state from outside it.

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
   * an uncaught exception escapes `preStart` or `onMessage` and its `SupervisorStrategy` decides
     `Stop` or `Escalate` (see "Failure handling" below), or
   * its supervisor (parent) stops, or an `Escalate` decision elsewhere in its ancestor chain
     cascades a stop down to it (TASK-402), or
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

This is a deliberately simple stop model for M1: no draining, no "let the mailbox empty out
first" mode. `ActorSystem.close()` (or the try-with-resources form) stops every actor and blocks
until all of them have finished terminating.

## 6. Actor hierarchies (TASK-402)

An actor may spawn children of its own from within `preStart`/`onMessage`, via
`ActorContext.spawnChild(factory, name[, strategy])`, becoming their supervisor. A child's id is
namespaced under its parent's (`parent.id() + "/" + name`). A **top-level actor** (spawned via
`ActorSystem.spawn`) has no parent and always behaves exactly as TASK-107a originally specified
— see "Failure handling," below; only a spawned child gets a configurable `SupervisorStrategy`.

Stopping an actor stops every actor it supervises, transitively — a supervisor's children (and
their own children, and so on) always go with it. This applies uniformly whether the stop was
requested explicitly (`ActorSystem.stop`), came from a `Stop` directive (below), or cascaded down
from a stop somewhere above it in the hierarchy.

## 7. Failure handling (TASK-107a, TASK-402)

**Top-level actors are unchanged from M1:** if `preStart`/`onMessage` throws, the failure is
logged and *that actor alone* is stopped, following the normal lifecycle above. This remains
non-configurable for a top-level actor — becoming a supervisor (spawning children) is how an
actor gets access to the configurable behavior below.

**A child actor's failure is handled by the `SupervisorStrategy` it was given at spawn time** (a
plain, stateless function of the failure — consulted only by the failing actor itself, on its own
dispatcher thread), choosing one of four directives:

* **Resume** — the actor keeps its existing state and keeps running, as if nothing happened.
* **Restart** — the actor is replaced with a fresh instance (state reset); `preRestart`/
  `postRestart` hooks run around the swap, best-effort.
* **Stop** — the actor stops, following the normal lifecycle (and cascades to its own children,
  per "Actor hierarchies," above) — TASK-107a's old default, now one of four choices rather than
  the only one.
* **Escalate** — the actor stops, and so does its entire ancestor chain up to the root (each
  ancestor's own stop cascading back down to *its* children too). An actor with no parent stops on
  its own, same as `Stop`.

**Poison message (TASK-203):** the message whose processing triggers a failure — the one
`onMessage` or `preStart` was handling when it threw — is termed a poison message. It is always
processed at most once and never redelivered, **including under `Restart`**: `Mailbox.take()`
already removes a message before `onMessage` (or the failure) happens, so there is nothing left in
the mailbox to redeliver by the time any `SupervisorStrategy` is even consulted. See
`docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md` and
`docs/decisions/ADR-008-supervision-strategies-and-hierarchies.md` for the full derivation and
semantics (including what this deliberately does not cover: restart rate-limiting, and dynamic
re-supervision on `Escalate`).

## 8. API philosophy

* Small core, own the stack, local-first.
* Complexity is opt-in: an actor only gets configurable supervision by choosing to spawn children
  (`ActorContext.spawnChild`); a plain top-level actor still behaves exactly like M1's fixed
  default, with nothing extra to learn or configure.
* No API is finalized until proven through implementation and tests — in particular, `ask()`
  does not exist yet; it is designed in ADR-015 (M5) before it is added.

## Roadmap

See the milestone table and dependency graph in the original design document
(`docs/decisions/` for individual decisions, and the project tracker for the full task list).
The short version: correct local semantics → efficient local runtime → reliable lifecycle →
persistence → transport → remote actors → cluster → production hardening → ecosystem.
