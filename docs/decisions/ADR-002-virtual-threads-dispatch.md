# ADR-002: Virtual threads as the initial execution model

* Status: Accepted — confirmed with benchmark data at TASK-303 (see ADR-006)
* Written during: M0/M1

## Context

TASK-105 calls for an initial `Dispatcher` built on Java virtual threads, explicitly without
premature optimization. TASK-106 requires that at most one message is processed for a given
actor at any instant. We need a strategy that satisfies both without overengineering M1.

## Decision

**One dedicated virtual thread per actor, for the actor's entire lifetime.** When an actor is
spawned, `ActorSystem` starts a virtual thread (via `Executors.newVirtualThreadPerTaskExecutor()`)
running that actor's dispatch loop: `preStart`, then repeatedly `mailbox.take()` followed by
`onMessage`, until the actor stops.

This is chosen because:

* It gives the sequential-processing guarantee (TASK-106) **structurally**, for free — there is
  only ever one thread that can call into a given actor's code, so no additional synchronization,
  locking, or scheduling logic is needed to enforce it.
* Virtual threads make a blocking `mailbox.take()` per actor cheap: unlike platform threads, tens
  or hundreds of thousands of parked virtual threads are not expected to be a scalability
  problem, which is exactly what makes "one thread per actor" viable in a way it would not be
  with platform threads.
* It is the simplest strategy to reason about, which matters for M1: understandability over
  throughput until there is benchmark data to optimize against (Section 12 of the design
  document: "no performance optimization is accepted without a benchmark").

## Alternatives considered (deferred to TASK-303, M3)

* **Shared executor** (a fixed pool of virtual or platform threads processing a work queue of
  "actor has a message ready" events) — better resource control, more complex to implement
  correctly (needs an explicit per-actor "is this actor currently being processed" flag/lock to
  preserve sequential processing, since the thread that picks up an actor's next message is no
  longer fixed).
* **Work-stealing** and **hybrid schedulers** — deferred entirely; not evaluated until there is
  benchmark data motivating them.

TASK-303 (M3) evaluates all of these against real measurements and may replace this strategy.
Until then, this ADR records *why* the current strategy was chosen, not that it is final.

**Resolved at TASK-303 (ADR-006):** the shared-executor alternative was built and benchmarked
against the exact same workload as TASK-301's data. It loses decisively and consistently across
every actor count tested — a fixed thread pool plateaus at the core count, while one-thread-per-
actor keeps scaling well past it. The strategy below is confirmed, not just provisional; see
ADR-006 for the full comparison and its caveats.

## Consequences

* `Dispatcher` is intentionally a thin wrapper around `Executors.newVirtualThreadPerTaskExecutor()`
  — no scheduling logic of its own yet.
* Actor count is currently bounded only by how many virtual threads (and their associated
  mailboxes) the JVM can hold — this is exactly the kind of question the Day-1 experiment
  (Section 18 of the design document) and TASK-301 (M3) are meant to measure.
* Changing the dispatch strategy later must preserve the sequential-processing guarantee by some
  other explicit mechanism — it does not come for free the way it does here.
