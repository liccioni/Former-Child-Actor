# Provisional decision: mailbox bounds (TASK-103a)

**This is not an ADR. It is now superseded by
[ADR-007](ADR-007-mailbox-bounds-confirmed.md) (TASK-306),** which confirms the decision recorded
here with real benchmark data. This note is kept as the historical record of the placeholder
decision; it was recorded so that the M3 benchmarks and the Day-1 experiment (Section 18 of the
design document) would measure a consistent, known policy instead of whatever was fastest to
implement.

## The decision

`Mailbox<T>` is:

* **Bounded**, with a default capacity of 1024 messages (`Mailbox.DEFAULT_CAPACITY`).
* **Block-on-full**: a sender calling `ActorRef.tell()` when the mailbox is at capacity blocks
  until space is available or the actor stops (see `docs/architecture.md`, "Lifecycle").

## Why this and not something else, for now

Bounded-blocking was picked over unbounded or drop/reject policies purely because it is a
reasonable, uncontroversial default that will not itself distort early measurements — an
unbounded mailbox can hide backpressure problems that block-on-full surfaces immediately, and a
drop/reject policy changes the shape of throughput/latency curves in ways that are hard to
compare against later once TASK-306 changes the policy again.

## What this is not

* Not a claim that block-on-full is the right policy long-term.
* Not a claim that 1024 is a well-chosen capacity — no benchmark backs that number yet.
* Not exhaustive: this note only covers what TASK-103a needs to unblock M1/M2/M3 work. Overflow,
  poison-message, and rejected-message semantics are formally specified in TASK-203's
  `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md`.

## What happened next

TASK-306 (M3) revisited this in light of the M3 benchmarks (TASK-301, TASK-302, TASK-307) plus a
new benchmark built specifically to drive the mailbox to real backpressure, since none of the
existing ones did. **The decision recorded here is confirmed, not replaced:** bounded, block-on-
full, capacity 1024. See [ADR-007](ADR-007-mailbox-bounds-confirmed.md) for the data and reasoning.
Any future change to this default now needs its own ADR that engages with ADR-007's data.
