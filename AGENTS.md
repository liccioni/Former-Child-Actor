# AGENTS.md

Guidance for anyone (human or agent) making changes to this repository.

## What this project is

A small, understandable Java actor runtime, growing milestone by milestone from a local library
(M1) toward an optional distributed cluster (M9+). See `docs/architecture.md` for the current
design and `docs/decisions/` for individual decisions (ADRs).

The guiding rule: **complexity is added because the application needs it, not because the
framework requires it.** Do not add features, abstractions, or configuration knobs ahead of the
milestone that actually needs them.

## Module boundaries

* `framework-core` — the actor runtime. No dependency on any other module in this repo. Keep it
  that way.
* `framework-testkit` — depends on `framework-core` only.
* `examples/*` — depend on `framework-core` (and `framework-testkit` for tests only); never on
  each other.
* `benchmarks` — depends on `framework-core`; JMH benchmarks land here starting M3.

## Testing requirements

* Every behavioral guarantee documented in `docs/architecture.md` (ordering, termination,
  failure isolation, sequential processing) must have a test that would fail if the guarantee
  were broken.
* Concurrency guarantees are tested with real concurrent load, not just single-threaded
  happy-path tests.
* Run `./gradlew build` before considering any change to `framework-core` or `framework-testkit`
  done.

## Things that require a new ADR before changing, not a silent code change

Write an ADR in `docs/decisions/` (see existing ADRs for the format) before changing any of the
following — these are decisions later milestones are built on top of, and changing them silently
risks invalidating work upstream or downstream of the change:

* **Mailbox bounds / backpressure policy.** The current bounded, block-on-full default
  (`Mailbox.DEFAULT_CAPACITY`) is explicitly provisional (TASK-103a,
  `docs/decisions/mailbox-bounds-provisional.md`) and is meant to be revisited *once*, with
  benchmark data, at TASK-306. Don't change it casually before then; don't leave it unexamined
  after M3 benchmarks land either.
* **What happens to queued messages when an actor stops** (currently: discarded, not delivered).
* **The dispatch strategy** (currently: one dedicated virtual thread per actor for its whole
  lifetime). Alternatives are evaluated with benchmarks at TASK-303 (M3), not swapped in ad hoc.
* **The minimal failure-handling default** (TASK-107a: log and stop that actor alone). This is
  intentionally not configurable in M1 — configurable supervision arrives in M4 (TASK-402) and
  supersedes this, but it doesn't get quietly extended before then.
* **Anything touching the pre-M10 trust boundary** — see `docs/decisions/ADR-000-*.md`. Before
  M10's real security work (TASK-1007), no transport or cluster component may bind to anything
  other than localhost by default, and none may be documented or marketed as safe for an
  untrusted network.
* Any decision listed in the ADR table in the original design document that hasn't been written
  yet for the milestone currently in progress.

## Design principles (see `docs/architecture.md` for detail)

1. Small core
2. Own the entire stack
3. Local-first
4. Explicit behavior
5. Strong, documented semantics
6. Excellent developer experience
7. Inspectability
8. Pluggable infrastructure
9. Java-first
10. Complexity only when the application needs it
