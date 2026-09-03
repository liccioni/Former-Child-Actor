# Benchmarks

JMH benchmark suite for the actor runtime (M3): TASK-301, TASK-302, TASK-307, TASK-308.

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

TASK-307 and TASK-308 are not yet implemented; see the open issues for what remains.
