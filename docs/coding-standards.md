# Coding Standards

## Java version

Target JDK 25 (LTS). Use the language and virtual-thread features it provides freely — records,
sealed interfaces, pattern matching, `ExecutorService.close()`.

## Build

Gradle (Kotlin DSL). Every subproject applies the root convention block in the top-level
`build.gradle.kts`: `java-library`, JDK 25 toolchain, `-Xlint:all -Werror`, JUnit 5 via the
platform launcher.

## Naming

* Packages: `dev.actorframework.<module>[.<subpackage>]`.
* Public API types live directly under the module's base package (e.g.
  `dev.actorframework.core.ActorSystem`); internal implementation types are package-private, not
  pushed into an `.internal` subpackage, so the module stays flat and easy to read end to end.
* Test classes: `<TypeUnderTest>Test`, methods named as full sentences describing the behavior
  under test (`stoppingAnActorDiscardsItsQueuedMessages`), not `testX`.

## Nullability

No nullability annotations yet (no framework dependency has been chosen). Until then: public API
methods do not accept or return `null` unless explicitly documented (e.g. `Mailbox.take()`
returning `null` to signal "closed and drained" is an internal, package-private contract, not a
public one).

## Module boundaries

* `framework-core` has no dependency on any other module in this repository. It must stay that
  way — it is the one piece every other module (and every application) depends on.
* `framework-testkit` depends on `framework-core` only.
* Examples depend on `framework-core` (and, if a test needs it, `framework-testkit`) — never on
  each other.

## API documentation

Every public type and public method in `framework-core` and `framework-testkit` carries a
Javadoc comment. Where a method's behavior encodes a specific design decision (e.g. what happens
to queued messages when an actor stops), the Javadoc says so explicitly and references the
relevant TASK/ADR — the code and the decision record should never drift apart silently.

## Testing expectations

* Every public behavioral guarantee (ordering, termination semantics, failure isolation) needs a
  test that would fail if the guarantee were silently broken — not just a happy-path test.
* Concurrency guarantees are tested by driving real concurrent load (multiple threads, thousands
  of messages), not asserted by inspection alone.
* `framework-core` tests may reach into package-private internals (e.g. `Mailbox`) directly,
  since they live in the same package. Tests for anything outside `framework-core` use the
  public API only.

## Benchmarking expectations

No JMH benchmarks exist yet (scaffolded in `benchmarks/`, implemented starting M3). Once they
exist: no performance optimization is accepted into `framework-core` without a benchmark
demonstrating the improvement, per the project's performance strategy.

## Comments

Default to no comments. Add one only when it captures something the code cannot: a non-obvious
invariant, a deliberate deviation from what a reader would otherwise expect, or a reference to
the design decision (TASK/ADR) that explains *why* the code does something surprising.
