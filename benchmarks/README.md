# Benchmarks

JMH benchmark suite for the actor runtime (M3): TASK-301, TASK-302, TASK-303, TASK-306, TASK-307,
TASK-308.

Benchmark sources live in `src/jmh/java`, using the
[`me.champeau.jmh`](https://github.com/melix/jmh-gradle-plugin) Gradle plugin. They are not part
of `./gradlew build` or `check` — JMH runs are too slow and too noise-sensitive to belong in
every CI build — so they only run when asked for explicitly:

```
./gradlew :benchmarks:jmh
```

Standard JMH command-line options can be passed through, e.g. to run one benchmark or override a
`@Param`:

```
./gradlew :benchmarks:jmh -Pjmh.includes=DispatchRoundTripBenchmark
./gradlew :benchmarks:jmh -Pjmh.includes=ActorCountScalabilityBenchmark -Pjmh.benchmarkParameters=actorCount=100000
./gradlew :benchmarks:jmh -Pjmh.includes=MessagingLatencyDistributionBenchmark
./gradlew :benchmarks:jmh -Pjmh.includes=SharedPoolDispatchBenchmark
./gradlew :benchmarks:jmh -Pjmh.includes=MailboxBackpressureBenchmark
```

TASK-307's memory-footprint measurement is not a JMH benchmark (see below for why) and runs via its
own task instead:

```
./gradlew :benchmarks:memoryFootprint
```

## TASK-301: dispatch strategy measurement

ADR-002 (one dedicated virtual thread per actor) named two open questions it deferred to
benchmark data rather than guessing:

* `DispatchRoundTripBenchmark` — the baseline cost of a single round trip (`tell()` through the
  mailbox, onto the actor's dispatcher thread, through `onMessage`, and back) for one already-warm
  actor. This is the floor every alternative dispatch strategy evaluated at TASK-303, and any
  mailbox-bounds change at TASK-306, is measured against.
* `ActorCountScalabilityBenchmark` — how that round-trip throughput holds up as the number of
  live actors grows (1 through 10,000 by default), under a fixed number of concurrent sending
  threads. Directly answers ADR-002's "actor count is currently bounded only by how many virtual
  threads the JVM can hold."

Both benchmarks carry their own `@Warmup`/`@Measurement`/`@Fork` settings rather than relying on
JMH's (much slower) defaults, since spawning up to 10,000 actors per trial is itself non-trivial
work.

`ask()` does not exist yet (ADR-015, M5), so "was this message actually processed" is observed by
carrying a `CountDownLatch` in the message rather than a request-response call — see
`LatchCountingActor`.

## TASK-302: messaging throughput and latency distribution

The original design document's Section 12 asks specifically for Messages/sec and a p50/p95/p99/p999
latency breakdown — a percentile distribution, not just an average. TASK-301's benchmarks give solid
throughput numbers (`ActorCountScalabilityBenchmark`'s `actorCount=1` row is itself a "messages/sec
against one actor under 16 senders" data point) but nothing yet reports the tail, which is exactly
where mailbox contention (lock acquisition, block-on-full backpressure — TASK-103a/ADR-003) actually
shows up and an average or throughput number alone hides.

`MessagingLatencyDistributionBenchmark` closes that gap, against a single already-warm actor:

* `throughputUnderLoad` — Messages/sec under 16 concurrent senders (`Mode.Throughput`).
* `latencyUncontended` — full latency distribution with a single sender (`Mode.SampleTime`), as a
  clean baseline.
* `latencyUnderLoad` — full latency distribution under the same 16-sender load as the throughput
  benchmark — the number that actually answers TASK-302's question.

JMH's `SampleTime` mode buckets individual invocation timings into percentiles automatically
(`p50.0`, `p90.0`, `p95.0`, `p99.0`, `p99.9`, `p99.99`, `p99.999`, `p100`) — "p999" in the original
document is the industry-standard shorthand for the 99.9th percentile, i.e. JMH's `p99.9` row.

## TASK-303: dispatch strategy evaluation

ADR-002 deferred one specific alternative to real measurement: a shared executor (a fixed pool of
threads processing a queue of "actor has work" events) instead of one dedicated virtual thread per
actor. `SharedPoolActorCell` (in `src/main/java`, so both `src/test/java` and `src/jmh/java` can
see it) is a minimal but correct prototype of that pattern — `SharedPoolActorCellTest` proves it
gives the same delivery and sequential-processing guarantees `ActorCell` does before any benchmark
number from it is trusted. `SharedPoolDispatchBenchmark` runs the exact same workload shape as
TASK-301's `ActorCountScalabilityBenchmark` against it, so the two are directly comparable.

Result: the shared executor loses decisively and consistently at every actor count tested — see
ADR-006 for the full comparison and data. ADR-002's strategy is confirmed, not replaced; no changes
to `framework-core`.

## TASK-307: memory footprint per actor

The design document's Section 12 also asks for memory per actor: an empty actor, an actor with a
mailbox, and an actor under load. This is deliberately **not** a JMH benchmark: JMH's timing
harness (and its bundled GCProfiler) measures allocation *rate* — bytes allocated per operation —
which fits throughput/latency questions (TASK-301/302) but not "how much heap does N *live, idle*
actors cost, standing around?" That is a retained-footprint snapshot, not a per-operation rate.

`MemoryFootprintHarness` (a plain `main()`, not `@Benchmark`-annotated) measures it directly: force
GC, sample `Runtime` heap usage, spawn or exercise N actors, sample again, divide the delta by N.
Three scenarios:

* **Empty actor** — freshly spawned, no message ever sent: the baseline cost of `ActorCell` plus a
  never-touched `Mailbox` (whose backing `ArrayDeque` pre-sizes to 256 slots at construction,
  regardless of whether it's ever used).
* **Actor with mailbox after a burst, now idle** — every actor already has a mailbox from birth, so
  the meaningful comparison isn't "with vs. without" but whether a mailbox that briefly held a
  backlog leaves a permanently larger footprint once idle again. `ArrayDeque` never shrinks its
  backing array back down after growing, so a burst of 300 messages (forced past the initial
  256-slot array by briefly blocking the actor's own dispatcher thread) is expected to leave, and
  did leave in a local run, a measurably larger idle footprint than an actor that was never
  touched.
* **Actor under sustained load** — memory sampled *while* many actors are being actively bombarded
  with messages, not after things settle, and deliberately without a forced GC (which would itself
  distort an active workload) — expect more sample-to-sample noise here than the other two
  scenarios, including the occasional low or even near-zero delta if an unforced GC happens to run
  during the sampling window; that's the tradeoff for not perturbing the live workload, not a bug.

This is inherently approximate — `Runtime.gc()` is a request, not a guarantee, and heap usage
includes JIT-compiled code and class-metadata growth unrelated to actor count — so all trials are
printed, not just an average, to keep that noise visible rather than hidden behind one falsely
precise number.

## TASK-306: mailbox bounds/backpressure decision

TASK-103a's bounded, block-on-full default (`Mailbox.DEFAULT_CAPACITY = 1024`) was explicitly
provisional, deferred to real benchmark data at TASK-306. TASK-301/302's benchmarks don't supply
that data by themselves: their busiest case (16 senders against one actor) never drives a mailbox
anywhere near capacity 1024, so they show how the runtime behaves well below the limit, not what
the limit itself costs or buys. TASK-306 closes that gap with two purpose-built measurements.

`BoundedBlockingMailbox` (in `src/main/java`, so both `src/test/java` and `src/jmh/java` can see
it) is a faithful copy of `framework-core`'s package-private `Mailbox`, parameterized by capacity —
the real `Mailbox`'s capacity isn't public API, so this is the only way to compare capacities
directly. `BoundedBlockingMailboxTest` proves it behaves identically (FIFO order, blocks while
full, capacity enforced, unblocks on `close()`) before any benchmark number from it is trusted, the
same discipline TASK-303's `SharedPoolActorCell` established.

* `MailboxBackpressureBenchmark` — a consumer deliberately throttled to ~1 message/ms against 8
  concurrent senders, so the mailbox is genuinely saturated throughout the run rather than only
  occasionally full. Measures accepted throughput and `offer()` blocking latency at capacities 16,
  128, 1024, and 8192. Result: both are invariant to capacity once saturated (~975-981 ops/s,
  ~8140-8155 µs mean latency across all four) — exactly what queueing theory predicts once arrivals
  exceed a bottlenecked consumer's fixed service rate.
* A fourth `MemoryFootprintHarness` scenario (see below) fills `BoundedBlockingMailbox` instances to
  exactly their capacity with a single shared sentinel message, isolating the backing array's cost
  from per-message payload size. Result: retained memory scales roughly linearly with capacity once
  actually saturated — 250 / 700 / 5,398 / 40,184 bytes per mailbox at capacities 16 / 128 / 1024 /
  8192 — while the current default (1024) costs only ~5.4 KB even fully saturated, barely more than
  TASK-307's idle-actor baseline.

Decision: **confirm** the current default — bounded, block-on-full, capacity 1024. No change to
`framework-core`. See `docs/decisions/ADR-007-mailbox-bounds-confirmed.md` for the full reasoning,
including what this data does and doesn't cover.

TASK-308 is not yet implemented; see the open issues for what remains.
