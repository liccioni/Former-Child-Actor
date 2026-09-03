# ADR-003: Cross-sender mailbox ordering guarantees

* Status: Accepted
* Written during: M2 (TASK-201)

## Context

M1 guarantees per-sender ordering: messages sent by a single thread to a single actor are
delivered in the order they were sent, because the mailbox is FIFO and a single sender thread's
calls are naturally sequential. It leaves cross-sender ordering — two different threads calling
`tell()` on the same actor — unspecified. `docs/architecture.md` §2 flagged this gap and deferred
resolving it to TASK-201.

`Mailbox.offer()` already serializes all enqueue operations behind a single lock, which gives a
real ordering property "for free." The open questions are (1) precisely what that property is
safe to promise, and (2) whether backpressure (the mailbox being full) can silently break it.

## Decision

Two guarantees are formalized:

1. **Causal (happens-before) ordering.** If `tell(A)` returns before `tell(B)` is called on the
   same actor, and that ordering is established by real synchronization between the two calling
   threads (not just coincidental timing), then `A` is delivered before `B`. This falls directly
   out of `offer()`'s single lock: enqueueing is a synchronization action, so if a happens-before
   relationship exists between two `tell()` calls, the Java Memory Model guarantees the first
   call's enqueue completes (and is visible) before the second call's enqueue can begin.
   Two `tell()` calls with **no** happens-before relationship between them — genuinely racing
   threads — have unspecified relative order. This is not a limitation to work around; "first" is
   not a meaningful concept for two calls with no synchronization tying them together, in this or
   any framework.

2. **Backpressure admission order.** When the mailbox is full, senders blocked in `offer()` are
   admitted in the order they began waiting (FIFO) once space frees up. A sender that arrives
   after another is already blocked cannot jump ahead of it.

Guarantee (1) already holds today and requires no implementation change. Guarantee (2) does not:
`Mailbox` currently uses a non-fair `ReentrantLock`. `Condition.signal()` already wakes blocked
waiters in FIFO order relative to each other, but a non-fair lock lets a *new* caller barge in
and acquire the lock — and enqueue — ahead of an already-signaled waiter that is still in the
process of reacquiring it. The fix is to construct `Mailbox`'s lock in fair mode
(`new ReentrantLock(true)`), which removes barging: the JDK's fair-lock contract admits threads
in strict arrival order.

## Alternatives considered

* **No guarantee at all for cross-sender ordering.** Simpler to state, but throws away a property
  the current lock-based implementation already provides, and later milestones (M4 supervision,
  M5 ask-pattern) would have no ordering to reason about at all when composing calls across
  actors. Rejected as strictly weaker than what's achievable at no extra cost.
* **Strict real-time submission order** (deliver in the literal wall-clock order `tell()` was
  invoked, even without happens-before). Not a coherent guarantee: for two threads racing with no
  synchronization between them, "which one happened first" is not well-defined by the Java Memory
  Model or by physics at this granularity. Achieving it would require imposing synchronization
  the caller never asked for, for a guarantee that can't actually be honestly kept. Rejected.
* **Leave backpressure admission order unspecified**, deferring to TASK-306's mailbox-bounds
  revisit (M3). Rejected because it would leave a real, currently-observable gap in an otherwise
  clean guarantee, and the fix (a fair lock) is small and self-contained; TASK-306 revisits the
  bounds/backpressure *policy* (capacity, block-vs-reject), not this ordering property, and can
  build on top of a fair lock either way.

## Consequences

* `Mailbox`'s internal lock is now fair, which trades some throughput under contention for the
  ordering guarantee. This is acceptable for M2; TASK-306 (M3) revisits mailbox behavior with real
  benchmark data and can reassess if fairness turns out to be a measured bottleneck.
* `docs/architecture.md` §2 states both guarantees precisely instead of deferring to this ADR by
  reference alone.
* Any future change to `Mailbox`'s locking strategy must preserve both guarantees, or supersede
  this ADR explicitly.
