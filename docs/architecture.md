# Architecture

Status: living document, updated as milestones land. Current milestone: **M1 — Minimal Actor
Runtime**.

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

**Provisional bounds decision (TASK-103a):** the mailbox is bounded with a generous default
capacity (1024) and blocks the sending thread when full ("block-on-full"). This is explicitly
provisional — chosen only so early benchmarks are comparable — and is revisited with real
measurement data at TASK-306 (M3). See `docs/decisions/mailbox-bounds-provisional.md`.

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

This is a deliberately simple stop model for M1: no draining, no "let the mailbox empty out
first" mode. `ActorSystem.close()` (or the try-with-resources form) stops every actor and blocks
until all of them have finished terminating.

## 6. Failure handling (TASK-107a)

There is one, non-configurable default for M1: if `onMessage` (or `preStart`) throws, the
failure is logged with the actor's identity and the message being processed, and *that actor
alone* is stopped, following the normal lifecycle above. No other actor, and not the
`ActorSystem` itself, is affected — this holds structurally, because each actor runs on its own
thread and failures never cross that boundary.

**Poison message (TASK-203):** the message whose processing triggers this — the one `onMessage`
or `preStart` was handling when it threw — is termed a poison message. Because M1–M3 have no
redelivery or retry mechanism, a poison message structurally cannot cause a retry loop: it is
processed at most once, the actor stops, and the message is never redelivered to this or any
other actor. This guarantee holds only through M1–M3; M4 (TASK-402) must explicitly re-examine
whether a restarted actor can receive the same poison message again. See
`docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md`.

This is a safety net, not the supervision model. Configurable strategies (Resume, Restart, Stop,
Escalate) and actor hierarchies arrive in M4 (TASK-402).

## 7. API philosophy

* Small core, own the stack, local-first.
* Complexity is opt-in: nothing beyond `ActorSystem`, `ActorRef`, `Actor`, `Mailbox`, and
  `Dispatcher` exists yet, on purpose.
* No API is finalized until proven through implementation and tests — in particular, `ask()`
  does not exist yet; it is designed in ADR-015 (M5) before it is added.

## Roadmap

See the milestone table and dependency graph in the original design document
(`docs/decisions/` for individual decisions, and the project tracker for the full task list).
The short version: correct local semantics → efficient local runtime → reliable lifecycle →
persistence → transport → remote actors → cluster → production hardening → ecosystem.
