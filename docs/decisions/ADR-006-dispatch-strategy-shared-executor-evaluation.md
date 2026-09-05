# ADR-006: Dispatch strategy — evaluating the shared-executor alternative (TASK-303)

* Status: Accepted
* Written during: M3 (TASK-303)
* Supersedes: nothing — confirms ADR-002's provisional strategy with data, as ADR-002 itself
  called for

## Context

ADR-002 chose one dedicated virtual thread per actor for M1, explicitly deferring two
alternatives to real measurement rather than guessing: a **shared executor** (a fixed pool of
threads processing a queue of "actor has work" events) and **work-stealing/hybrid schedulers**
(deferred entirely, not evaluated until there is data motivating them). Per `AGENTS.md`, the
dispatch strategy is an ADR-gated decision — it does not change without one.

TASK-301/302 already produced throughput and latency data for the current strategy. TASK-303's
job is to build the same measurement for the shared-executor alternative and compare.

## What was built and measured

`SharedPoolActorCell` (in the `benchmarks` module, not wired into `framework-core`): a minimal
but correct prototype of the shared-executor pattern ADR-002 described — a fixed pool of platform
threads (sized to `Runtime.availableProcessors()`, 4 on the machine these numbers were measured
on), with each actor scheduled onto the pool only when it has work and isn't already scheduled
(a compare-and-set gate), draining up to 16 messages per turn before yielding the thread back to
the pool. This is the same pattern real event-driven dispatchers use (e.g. Akka's default
dispatcher) and is deliberately *not* a hypothetical strawman: `SharedPoolActorCellTest` proves it
gives the same two guarantees `ActorCell` does — every message delivered exactly once, and never
two messages processed concurrently for the same actor — under real concurrent load, before any
benchmark number from it is trusted.

`SharedPoolDispatchBenchmark` runs the *exact* workload shape as TASK-301's
`ActorCountScalabilityBenchmark` — same actor counts (1/10/100/1,000/10,000), same 16 concurrent
sender threads, same round-robin send pattern — against `SharedPoolActorCell` instead of the real
`ActorSystem`. Matching the workload exactly is what makes the two numbers comparable rather than
measuring two different things.

## Results

Both suites measured back-to-back on the same 4-core machine:

| Actors | One-thread-per-actor (ADR-002, TASK-301) | Shared 4-thread pool (this ADR) | Per-actor-thread advantage |
|---|---|---|---|
| 1 | ≈104,057 ops/s | ≈66,131 ops/s | ~1.6× |
| 10 | ≈196,306–207,049 ops/s | ≈168,292 ops/s | ~1.2× |
| 100 | ≈314,304–347,691 ops/s | ≈171,889 ops/s | ~1.9× |
| 1,000 | ≈404,853–416,656 ops/s | ≈176,299 ops/s | ~2.3× |
| 10,000 | ≈319,390–360,420 ops/s | ≈172,070 ops/s | ~2× |

The confidence intervals on both suites are wide (this is a shared, virtualized sandbox, not a
dedicated benchmark machine — see the same caveat in TASK-301/302's own data), but the shape is
consistent and repeats across the full actor-count sweep, which is what a single noisy number
would not give confidence in:

* The **shared pool's throughput plateaus almost immediately**, around ~170K ops/s from 10 actors
  onward, and never grows further as actor count increases to 1,000 or 10,000. This is exactly
  what the design predicts: the pool itself — 4 platform threads — is the hard ceiling on
  concurrent work, regardless of how many actors exist above that.
* The **one-thread-per-actor strategy keeps scaling** well past the 4-core ceiling (up to
  ~400K ops/s around 1,000 actors) because many parked virtual threads let the JVM interleave
  work across blocking points far more cheaply than a fixed pool of platform threads can, and
  there is no shared bottleneck resource analogous to the pool.
* The one-thread-per-actor strategy wins at **every** actor count tested, including the
  single-actor case, where there is no pool-contention story at all — the shared-pool's
  scheduling overhead (the compare-and-set gate, the executor hand-off) is itself more expensive
  than a virtual thread already parked in `mailbox.take()` waking up.

## Decision

**Keep ADR-002's one-dedicated-virtual-thread-per-actor strategy.** The shared-executor
alternative it deferred to real measurement is now measured, and loses decisively and
consistently, not marginally or only in an edge case. Adopting it would trade away real,
measured throughput for "better resource control" that this runtime does not currently need —
exactly the kind of premature optimization Section 12 of the design document warns against
("no performance optimization is accepted without a benchmark," which cuts both ways: a
*regression* also needs a benchmark before it's accepted).

Work-stealing and hybrid schedulers remain out of scope, unchanged from ADR-002: they were
deferred pending data motivating them, and a shared *fixed* pool — the simpler alternative in the
same family — just lost decisively. There is no motivating data to build the more complex
variants right now.

## What this does not cover

* Only a fixed **platform-thread** pool was measured, per ADR-002's literal description of the
  alternative. A shared pool of *virtual* threads used the same way (scheduled per-turn rather
  than parked for an actor's whole lifetime) was not benchmarked and might behave differently —
  if a future concrete need motivates revisiting dispatch strategy, that variant is the first
  place to look, not a reason to reopen this decision speculatively now.
* `SharedPoolActorCell`'s batch size (16 messages per turn) was not tuned or swept. A different
  batch size could shift the shared pool's numbers somewhat, but would not remove its structural
  ceiling (a fixed number of threads bounds concurrent work no matter how large the batch is) —
  the qualitative conclusion (it plateaus; per-actor threads don't) would not change.
* This is one hardware profile (4 cores, shared/virtualized). The *shape* of the result — a fixed
  pool plateaus at the core count, per-actor virtual threads keep scaling — is expected to
  generalize, but the exact crossover numbers would differ on different hardware.

## Consequences

* No change to `framework-core`: `Dispatcher` and `ActorCell` are unchanged.
* `SharedPoolActorCell` and `SharedPoolDispatchBenchmark` remain in the `benchmarks` module as the
  recorded comparison point — if dispatch strategy is ever revisited (e.g. at much higher actor
  counts than tested here, or on hardware with a very different core count), this is the harness
  to extend rather than rebuild.
* ADR-002's "Alternatives considered" section is now resolved with data for the shared-executor
  option; TASK-303 is closed.
