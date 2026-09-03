# TASK-203: Mailbox overflow, poison-message, and rejection semantics — design

* Status: Approved
* Tracks: GitHub issue #13 (TASK-203), milestone M2 — Ordering & Memory Model

## Problem

TASK-103a (`docs/decisions/mailbox-bounds-provisional.md`) picked a provisional mailbox policy
(bounded, block-on-full, capacity 1024) but explicitly left overflow, poison-message, and
rejected-message semantics unspecified — it only covers what was needed to unblock M1–M3 work.
`AGENTS.md` requires any change touching mailbox backpressure policy, queued-message-on-stop
behavior, or the M1 failure-handling default to go through an ADR, not a silent code change.
TASK-203 is that ADR.

## Scope

This is formalization only, not a policy change:

* **In scope**: precisely naming and documenting behavior the codebase already has, so later
  milestones (M3 TASK-306, M4 TASK-402, M5 `ask()`) have settled terminology and guarantees to
  build on.
* **Out of scope**: changing mailbox capacity, the block-on-full policy itself (TASK-306, M3),
  or adding new observable behavior (e.g. a signaled rejection return from `tell()`).

## Decision

### 1. Overflow

Reaffirms TASK-103a's block-on-full as-is: a full mailbox delays a sender in `offer()`, it does
not by itself reject a message. Rejection (see §3) only follows if `close()` happens while the
sender is still waiting.

### 2. Poison message

Defined as the message whose processing (`onMessage` or `preStart`) throws, per
`docs/architecture.md` §6 / TASK-107a — the actor that received it stops, log-and-stop, alone.

The ADR states explicitly, as a guarantee and not merely an observation: because M1/M2 have no
redelivery or retry mechanism, a poison message structurally cannot cause a retry loop. M4
(TASK-402, configurable supervision including Restart) is flagged as the milestone that must
re-examine this guarantee once redelivery becomes possible.

### 3. Rejected message

Unifies "rejected" and "dropped"/"discarded" (currently scattered across `docs/architecture.md`
§5 and `Mailbox`'s javadoc) as one term — "rejected" — covering exactly three mechanisms:

1. `offer()` called after the mailbox is already closed (`offerAfterCloseIsDroppedNotEnqueued`).
2. A sender blocked in `offer()` on a full mailbox, unblocked when `close()` runs
   (`closeUnblocksASenderWaitingOnAFullMailbox`).
3. A message already enqueued, discarded at `close()` before delivery
   (`closeDiscardsQueuedMessagesAndUnblocksTake`).

This unification is safe because `ActorRef.tell()` is `void`: the caller receives no signal in
any of the three cases today, so there is no observable difference between them. The ADR flags
that a future signaled send path (e.g. around `ask()`, M5) may need to re-split this taxonomy,
and treats that as explicitly out of scope now.

## Alternatives considered

* **Expose `offer()`'s boolean return through `tell()` now.** Rejected — API change with no
  consumer until `ask()` (M5); premature.
* **Keep "rejected" and "discarded-after-enqueue" as separate concepts.** Rejected — no
  observable difference to callers in M1/M2; unnecessary taxonomy that only adds surface area to
  keep consistent.

## Deliverables

1. `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md` — new ADR, following
   the ADR-003 format (Context / Decision / Alternatives considered / Consequences), recording
   the three guarantees above.
2. `docs/architecture.md` — §4 (Mailbox) gains the unified "rejected" terminology; §5 (Lifecycle)
   and §6 (Failure handling) get cross-references to ADR-004 instead of their current scattered
   "discarded"/"dropped" wording.
3. `docs/decisions/mailbox-bounds-provisional.md` — "What this is not" section updated: the
   "TASK-203 still needs to specify..." line is replaced with a pointer to ADR-004. Also fixes a
   stale self-reference: the note currently says the bounds/backpressure policy will be
   "superseded by the full ADR-003" at TASK-306, but ADR-003 is already taken (cross-sender
   ordering) and ADR-004 is this ADR, not the M3 bounds revisit. Reworded to point forward to
   "the ADR TASK-306 produces" without presupposing its number.

## Testing

None needed. `framework-core/src/test/java/dev/actorframework/core/MailboxTest.java` already
exercises all three rejection mechanisms listed in §3. This task only names and cross-references
behavior already proven by those tests — no new test coverage is required.
