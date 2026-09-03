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

TASK-302, TASK-307, and TASK-308 are not yet implemented; see the open issues for what remains.
