# ADR-007: Mailbox bounds and backpressure — confirmed with benchmark data (TASK-306)

* Status: Accepted
* Written during: M3 (TASK-306)
* Finalizes: TASK-103a's provisional default (`docs/decisions/mailbox-bounds-provisional.md`)
* Builds on: ADR-004 (overflow/poison/rejection semantics — the qualitative policy, already
  formalized in M2 and unchanged here)

## Context

TASK-103a picked bounded, block-on-full, capacity 1,024 as a provisional default purely so early
benchmarks would be comparable, explicitly deferring the real decision to TASK-306 once M3's
benchmark data existed. `AGENTS.md` gates any change to this default on that data. ADR-004 already
settled the *qualitative* policy question (overflow blocks; it does not reject, drop, or fail) —
what TASK-306 has left to confirm or replace is the *specific* capacity value, using TASK-301,
TASK-302, and TASK-307's data as `mailbox-bounds-provisional.md` said it would.

There was a gap in that data going in: TASK-301/302's busiest workload (16 concurrent senders
against one actor) never drives a mailbox anywhere near capacity 1,024 — those benchmarks show how
the runtime behaves well below the limit, not what the limit itself costs or buys. TASK-306 closes
that gap directly rather than deciding from data that never actually exercised backpressure.

## What was built and measured

`BoundedBlockingMailbox` (in the `benchmarks` module, not wired into `framework-core`) is a
faithful copy of `framework-core`'s package-private `Mailbox` — same fair-lock, same
`ArrayDeque`-backed design — parameterized by capacity, since the real `Mailbox`'s capacity isn't
part of `framework-core`'s public API. `BoundedBlockingMailboxTest` proves it behaves identically
(FIFO order, blocks while full, capacity enforced) before trusting any number from it — the same
discipline TASK-303's `SharedPoolActorCell` established.

Two new measurements, at capacities 16 / 128 / 1,024 / 8,192:

1. **`MailboxBackpressureBenchmark`** — a consumer deliberately throttled to ~1 message/ms (far
   slower than 8 concurrent senders can produce), so the mailbox is genuinely saturated throughout
   the measurement, not just occasionally full. Measures accepted messages/sec and per-call
   `offer()` blocking latency distribution — the actual backpressure a sender feels.
2. **A fourth `MemoryFootprintHarness` scenario** — mailboxes filled to exactly their capacity with
   a single shared sentinel message (isolating the backing array's cost from per-message payload
   size, which TASK-307's other scenarios already cover and which the framework doesn't control
   anyway), sampled at rest.

## Results

**Throughput and blocking latency are invariant to capacity once the mailbox is actually
saturated:**

| Capacity | Accepted throughput | Mean `offer()` latency |
|---|---|---|
| 16 | 980.65 ops/s | 8,155.15 µs |
| 128 | 974.91 ops/s | 8,139.51 µs |
| 1,024 | 977.95 ops/s | 8,146.41 µs |
| 8,192 | 978.11 ops/s | 8,144.25 µs |

This is exactly what queueing theory predicts and what this benchmark was built to confirm rather
than assume: once arrivals exceed a fixed service rate, steady-state acceptance throughput is
capped by the consumer's rate, and average blocking latency is governed by the fair lock's arrival
order divided by that same rate — neither depends on how big the buffer is. Capacity's only real
effect is how much bursty demand can be absorbed before backpressure kicks in at all, and how much
memory is retained while saturated.

**Memory scales with capacity, but only when a mailbox is actually filled that far** — extending
TASK-307's finding that an idle mailbox's backing array only pre-sizes to `min(capacity, 256)` and
grows lazily:

| Capacity | Retained bytes per mailbox, fully saturated |
|---|---|
| 16 | 250 |
| 128 | 700 |
| 1,024 | 5,398 |
| 8,192 | 40,184 |

Growth is roughly linear in capacity once saturated (as expected — the backing array's size is the
dominant cost), at roughly 5 bytes per queued-slot. At the current default, a *fully saturated*
actor costs about 5.4 KB — barely more than TASK-307's idle-actor baseline (~3.2 KB) — so even
10,000 actors all saturated simultaneously (a pathological worst case; TASK-301/302 never came
close to producing it) would cost roughly 54 MB. At 8,192 the same worst case would cost roughly
400 MB — a real, measured cost for headroom nothing tested here needed.

## Decision

**Confirm the current default: bounded mailbox, block-on-full, capacity 1,024.** No change to
`framework-core`'s behavior.

* **Block-on-full is unchanged and unchallenged** — nothing in this data argues for reject, drop,
  or fail; blocking remains the only policy that never silently loses a message, and switching
  would mean re-litigating ADR-004's already-formalized semantics for no measured benefit.
* **1,024 is confirmed, not just left alone by default** — the data rules out the two directions a
  change could go:
  * *Raising it* (e.g. to 8,192) buys burst-absorption headroom nothing tested here has needed —
    TASK-301/302's realistic workloads never approach even the current limit — while measurably
    increasing worst-case per-actor memory under sustained overload (~7.4× at 8,192 vs. 1,024).
  * *Lowering it* (e.g. to 128 or 16) saves memory that is already negligible next to an actor's
    other fixed costs (TASK-307: mailbox capacity contributes only a few hundred bytes at most
    realistic depths, dwarfed by the ~3 KB baseline from `ActorCell`, its thread, and registry
    entry) while reducing burst-absorption headroom for no measured gain.
* 1,024 is therefore a reasonable, now-confirmed middle ground: cheap even in the pathological
  worst case, with meaningful slack for realistic bursts.

## What this does not cover

* **No realistic workload benchmarked here has ever actually approached capacity 1,024.** This ADR
  confirms the default is *safe and inexpensive* under genuine saturation, not that 1,024
  specifically is *optimal* for some higher-scale production pattern this project hasn't measured
  yet. If a real workload is ever found that habitually saturates the mailbox, that is new data
  this ADR does not have, and would warrant revisiting — with that data, not speculatively.
* **Per-actor configurable capacity was deliberately not added.** TASK-306 is about confirming or
  replacing the one hardcoded default, not about exposing capacity as a new tunable — that is a
  separate, undecided API-surface question (raised, not settled, here) that would need its own
  ADR if a concrete need for it ever arises.
* Measured on the same 4-core, shared/virtualized sandbox as every other benchmark in this suite —
  the *shape* of the results (capacity-invariant throughput/latency; linear memory scaling only
  under saturation) is expected to generalize; the exact numbers would differ on other hardware.

## Consequences

* No change to `framework-core`'s behavior. `Mailbox.DEFAULT_CAPACITY` remains 1,024; the
  "provisional" language in `Mailbox`'s Javadoc and `docs/architecture.md` §4 is updated to record
  that this is now a confirmed decision, not a placeholder.
* `docs/decisions/mailbox-bounds-provisional.md` is retained as the historical record of the
  provisional decision, with a note pointing here.
* `BoundedBlockingMailbox`, `MailboxBackpressureBenchmark`, and the fourth `MemoryFootprintHarness`
  scenario remain in the `benchmarks` module as the recorded comparison point, the same way
  TASK-303's harness remains available if dispatch strategy is ever revisited.
* Any future change to the mailbox's bounds or backpressure policy must engage with this ADR's
  data explicitly, or supersede it with new data of its own — it does not get changed casually.
