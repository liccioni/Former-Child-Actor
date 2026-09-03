# Provisional decision: mailbox bounds (TASK-103a)

**This is not an ADR.** It is a placeholder decision, recorded so it is visible and so the M3
benchmarks and the Day-1 experiment (Section 18 of the design document) measure a consistent,
known policy instead of whatever was fastest to implement. It is superseded by TASK-306's own ADR
(M3), once real benchmark data exists.

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

## What happens next

TASK-306 (M3) revisits this in light of the M3 benchmarks (TASK-301, TASK-302, TASK-307) and
either confirms or replaces it, written up as its own ADR. Until then, do not change this
default without updating this note and flagging that the M3 benchmarks it fed may need
re-running.
