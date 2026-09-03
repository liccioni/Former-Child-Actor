# ADR-005: Java Memory Model review of the sequential-processing guarantee

* Status: Accepted
* Written during: M2 (TASK-207)

## Context

TASK-106 (M1) guarantees at most one `Actor.onMessage` invocation in flight per actor at any
instant, and `docs/architecture.md` §3 has, until now, stated only informally that this holds
"because each actor owns a single dedicated virtual thread for its entire lifetime." That is true,
but "informally" is not good enough for a guarantee the rest of the framework — and every
application built on it — leans on to justify holding actor state in plain, unsynchronized fields.
This ADR does the rigorous version: for every piece of shared mutable state the runtime touches,
it identifies the exact Java Memory Model (JMM) rule that makes cross-thread visibility safe, or
says plainly where none exists.

Scope, per TASK-207 and the design document's Section 12 note ("Actor state, Mailboxes,
Lifecycle, Scheduling"):

1. Actor state itself.
2. The mailbox as the boundary between sender threads and the actor's own thread.
3. Lifecycle flags (`stopRequested`, `terminated`, `ActorSystem.shuttingDown`).
4. The actor registry (`ActorSystem.actors`).
5. Safe publication of an `ActorCell`'s own fields to the dispatcher thread that runs it.
6. What the framework does *not* protect against.

## Review

### 1. Actor state — program order on a single thread, not cross-thread visibility

`ActorSystem.spawn()` submits exactly one task, `cell::run`, to
`Executors.newVirtualThreadPerTaskExecutor()`. That executor starts exactly one new virtual thread
per submitted task and runs it to completion. `ActorCell.run()` calls `actor.preStart(context)`
and then, in the same call stack on that same thread, loops calling `actor.onMessage(context,
message)` until the actor stops, finishing with `actor.postStop(context)`.

This means every read and write an `Actor` implementation makes to its own fields, across every
message it ever processes, happens on one `java.lang.Thread` object, in program order (JLS
17.4.5). No `volatile`, no lock, no atomic is needed for actor-owned state, because there is no
second thread ever reading or writing it — this is not "very likely safe," it is categorically
outside the set of situations the JMM's cross-thread visibility rules even apply to.

**Virtual threads do not change this.** A virtual thread can be unmounted from one carrier
(platform) thread and remounted onto a different one across a blocking operation (such as
`Mailbox.take()`'s `Condition.await()`). This is invisible to the JMM: the memory model reasons
about `java.lang.Thread` identity and program order on that logical thread, not about which OS
thread happens to carry it at a given moment. The JDK's virtual-thread implementation is
responsible for whatever low-level barriers are needed to make mounting/unmounting transparent;
application and framework code never needs to reason about carrier threads at all. Worth stating
explicitly here since it is a natural (and wrong) intuition to worry about.

### 2. The mailbox — the one real cross-thread boundary

Every other shared-state question in the runtime reduces to: is `Mailbox` correctly synchronized?
It is the only structure regularly written by arbitrary sender threads and read by the actor's own
dispatcher thread.

`Mailbox.queue` (a plain, non-thread-safe `ArrayDeque`) and `Mailbox.closed` are **only ever
touched while holding `Mailbox.lock`** — every access in `offer()`, `take()`, and `close()` is
inside a `lock()/finally { unlock() }` block; there is no unguarded read or write of either field
anywhere in the class. `java.util.concurrent.locks.Lock`'s documented memory consistency effects
give the same guarantee as `synchronized`: actions in a thread prior to calling `unlock()`
happen-before actions following a successful `lock()` by another thread. Concretely:

* A sender's `offer()` call happens-before the dispatcher thread's `take()` call that dequeues that
  same message — so any state the sender established before calling `tell()` (including
  constructing the message object itself) is visible inside `onMessage` when that message is
  processed. This is the mechanism ADR-003 already relies on for cross-sender causal ordering; this
  review confirms *why* it is sound, not just that it is.
* Symmetrically, `close()` happens-before any `offer()`/`take()` call that observes `closed ==
  true`, which is what makes the discard/reject semantics in ADR-004 and §5/§6 of
  `docs/architecture.md` reliably observable rather than racy.

No gap was found here.

### 3. Lifecycle flags

`ActorCell.stopRequested`, `ActorCell.terminated`, and `ActorSystem.shuttingDown` are all
`AtomicBoolean`, always accessed through its atomic methods (`compareAndSet`, `get`, `set`) —
never as a plain field. `AtomicBoolean` (like all `java.util.concurrent.atomic` types) has
`volatile`-equivalent read/write semantics: a `set(true)` happens-before any subsequent `get()`
that observes `true`. Two consequences worth being explicit about, since applications and the
testkit both depend on them:

* `ActorCell.terminated.set(true)` runs at the very end of `finishTermination()` — **after**
  `actor.postStop(context)` has returned. So once `ActorRef.isTerminated()` (and therefore
  `Await.awaitTerminated` in `framework-testkit`) observes `true` from any thread, every side
  effect the actor ever performed — every `onMessage`, and `postStop` itself — is guaranteed
  visible to that observing thread. This is the property the testkit's failure assertions (TASK-110)
  actually rely on, and it holds.
* `ActorRefImpl.tell()`'s `terminated.get()` check and `ActorSystem.spawn()`'s
  `shuttingDown.get()` check are correctness *optimizations* (fail fast without touching the
  mailbox lock), not the sole enforcement mechanism — the mailbox's own `closed` flag, under its
  own lock, is what actually guarantees a post-close `offer()` is rejected even in the race window
  right around a stop request. No gap.

### 4. The actor registry

`ActorSystem.actors` is a `ConcurrentHashMap`. Its own documented guarantees cover every operation
used here (`putIfAbsent`, `get`, `remove`, iteration in `shutdown()`) without any additional
synchronization needed. No gap.

### 5. Safe publication of `ActorCell` to its own dispatcher thread

`ActorCell`'s `system`, `id`, `actor`, `mailbox`, `ref`, and `context` fields are all `final`,
assigned in the constructor, which runs to completion (`new ActorCell<>(...)`) before
`dispatcher.execute(cell::run)` is called — on the same thread, in program order. Two independent
guarantees make the fully-constructed `ActorCell` (and the `Actor` instance inside it) safely
visible to the new virtual thread that runs `cell::run`:

* JLS 17.5's final-field safe-publication guarantee, and
* `java.util.concurrent.Executor`'s documented memory consistency effects: "actions in a thread
  prior to submitting a `Runnable` object to an `Executor` happen-before its execution begins,"
  which applies to `Dispatcher.execute()` (a thin wrapper over
  `ExecutorService.execute()`) exactly as it would to a raw call.

No gap.

### 6. What is explicitly *not* guaranteed

* **Mutating a message after sending it.** The mailbox's happens-before edge covers state
  established *before* `tell()` is called. If a sender keeps a reference to a mutable message
  object and mutates it *after* calling `tell()`, there is no guarantee about which version (or
  what partial state) the receiving actor observes — this is an ordinary shared-mutable-state bug,
  not a framework gap, but it is exactly the kind of pitfall a JMM review exists to name rather
  than leave implicit. Guidance: treat a message as handed off, not shared, the moment `tell()` is
  called. Prefer immutable messages (records are a natural fit).
* **`ActorSystem.close()` / `Dispatcher.close()` as a source of visibility.** `close()` blocks
  until every actor's dispatcher thread has finished, but that "wait for completion" behavior is
  not the same documented, citable happens-before edge as `AtomicBoolean` or a `Future.get()`
  provides. Code that needs to observe a specific actor's final side effects should use
  `ActorRef.isTerminated()` (§3, above), not "I called `close()` so everything must be visible."
  In practice `close()` already goes through `ExecutorService.close()`'s own termination-tracking,
  which is safe, but this ADR deliberately does not lean on that path as the *documented* public
  guarantee.

## Decision

Adopt this review as the record of why TASK-106's guarantee holds. No code changes: the review
found no visibility gap in the current implementation. `docs/architecture.md` §3 is updated to
state the guarantee's basis directly (single dedicated thread + safe publication via the
executor) instead of deferring to "informally, for M1." A new regression test,
`SafePublicationTest` in `framework-core`, exercises the mailbox happens-before edge directly:
a sender mutates a plain (non-volatile, non-atomic) field immediately before calling `tell()`,
and the receiving actor's `onMessage` observes the update — repeated across many iterations and
sender threads to keep the test meaningful as a guard against a future regression (e.g. someone
swapping the mailbox's lock for something that doesn't provide the same happens-before edge).

## Consequences

* `docs/architecture.md` §3 now states the JMM basis for TASK-106 explicitly and links here.
* The "don't mutate a message after sending it" guidance is added to `Actor`'s Javadoc, since it's
  the one real caller-facing pitfall this review surfaced.
* Any future change to `Mailbox`'s synchronization strategy, or to the one-thread-per-actor
  dispatch strategy (TASK-303, M3), must re-derive these happens-before edges explicitly rather
  than assume they still hold — this ADR is the checklist for that re-derivation.
