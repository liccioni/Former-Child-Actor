# ADR-004: Mailbox overflow, poison-message, and rejection semantics

* Status: Accepted
* Written during: M2 (TASK-203)

## Context

TASK-103a (`docs/decisions/mailbox-bounds-provisional.md`) picked a provisional mailbox policy —
bounded, block-on-full, default capacity 1024 — but explicitly left overflow, poison-message, and
rejected-message semantics unspecified, covering only what M1–M3 needed to proceed.
`docs/architecture.md` and `Mailbox`'s own javadoc describe the underlying behavior (block-on-full,
log-and-stop failure handling, discard-on-close) but use inconsistent terms for it —
"discarded," "dropped," "not delivered" — without a single formal vocabulary. Later milestones (M3
TASK-306's bounds revisit, M4 TASK-402's supervision, M5's `ask()`) need settled terminology and
explicit guarantees to build on, not just observed behavior.

This ADR formalizes three things already true of the codebase. It changes no behavior.

## Decision

### 1. Overflow

The mailbox's overflow policy remains block-on-full (TASK-103a): a sender calling `offer()` (via
`ActorRef.tell()`) against a full mailbox blocks until space frees up or the actor stops. Blocking
is not itself rejection — a blocked sender's message is still delivered once space frees, unless
the actor stops while it is waiting (see "Rejected message," below). This ADR does not change the
policy or its capacity; TASK-306 (M3) owns revisiting both with benchmark data.

### 2. Poison message

A **poison message** is the message whose processing (`onMessage` or `preStart`) throws, causing
the actor that received it to stop — the log-and-stop default (`docs/architecture.md` §6,
TASK-107a).

Guarantee: because M1/M2 have no redelivery or retry mechanism, a poison message structurally
cannot cause a retry loop — it is processed at most once, the actor stops, and the message is
never redelivered to this or any other actor. This guarantee holds only through M1–M3; M4
(TASK-402) introduces configurable supervision, including Restart, and must explicitly re-examine
whether a restarted actor can receive the same poison message again.

### 3. Rejected message

A **rejected message** is one offered to a mailbox that will never process it. This term unifies
what `docs/architecture.md` and `Mailbox`'s javadoc currently call "discarded," "dropped," and "not
delivered" — they are the same outcome from the caller's perspective. Exactly three mechanisms
produce a rejected message:

1. `offer()` is called after the mailbox is already closed — the message is never enqueued.
2. A sender is blocked in `offer()` on a full mailbox, and `close()` runs before space frees — the
   sender is unblocked and its message is dropped, never enqueued.
3. A message is already enqueued when `close()` runs — it is discarded before delivery, even
   though it was successfully enqueued.

All three are grouped under one term because `ActorRef.tell()` is `void`: the caller receives no
signal distinguishing them, or distinguishing any of them from a successfully delivered message.
From the caller's perspective today, "rejected" is a single, unobservable outcome, not three.

A future signaled send path (e.g. `ask()`, designed in ADR-015 at M5) may need to expose which of
the three mechanisms rejected a message, or expose rejection at all. That is explicitly out of
scope for this ADR — YAGNI until a caller exists that can observe the difference.

## Alternatives considered

* **Expose `offer()`'s boolean return through `tell()` now**, giving callers a rejection signal
  today. Rejected: no caller consumes it yet (`tell()` is fire-and-forget by design,
  `docs/architecture.md` §2), and adding it ahead of `ask()` (M5) would be an API change with no
  use, contradicting the "complexity only when the application needs it" principle (`AGENTS.md`).
* **Keep "rejected" and "discarded-after-enqueue" as separate concepts**, since mechanism 3 above
  did successfully enqueue while mechanisms 1–2 did not. Rejected: no observable difference to any
  caller in M1–M3 (`tell()` is void); a taxonomy split with no consumer is unnecessary surface area
  to keep consistent across future milestones.
* **Leave terminology unspecified until TASK-306** (M3), on the theory that overflow/rejection
  semantics are tied to the bounds policy TASK-306 revisits anyway. Rejected: TASK-306 changes the
  *policy* (capacity, block-vs-reject-on-full), not these definitions — poison-message and
  rejected-message semantics hold regardless of what TASK-306 decides, and M4/M5 need this
  vocabulary sooner than M3 necessarily lands.

## Consequences

* `docs/architecture.md` §4–§6 now use "rejected message" and "poison message" as defined terms,
  cross-referencing this ADR instead of describing the same behavior with inconsistent wording in
  each section.
* `docs/decisions/mailbox-bounds-provisional.md`'s "What this is not" section no longer needs to
  flag overflow/poison/rejection semantics as unspecified; it points here instead.
* No code or test changes:
  `framework-core/src/test/java/dev/actorframework/core/MailboxTest.java` already exercises all
  three rejection mechanisms (`offerAfterCloseIsDroppedNotEnqueued`,
  `closeUnblocksASenderWaitingOnAFullMailbox`, `closeDiscardsQueuedMessagesAndUnblocksTake`); this
  ADR only names what they already prove.
* Any future change to overflow policy (TASK-306), failure handling (TASK-402), or send semantics
  (`ask()`, M5) that alters what "poison message" or "rejected message" means must supersede this
  ADR explicitly, per the ADR-required list in `AGENTS.md`.
