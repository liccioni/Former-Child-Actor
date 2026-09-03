# TASK-203: Mailbox Overflow, Poison-Message, and Rejection Semantics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Formally document overflow, poison-message, and rejected-message semantics that the
mailbox already implements, so later milestones (TASK-306, TASK-402, `ask()`/M5) have settled
terminology and guarantees instead of scattered wording.

**Architecture:** Pure documentation change across three files: a new ADR recording the three
guarantees, `docs/architecture.md` updated to use and cross-reference the new terminology, and
`docs/decisions/mailbox-bounds-provisional.md` updated to point at the new ADR instead of flagging
the gap as open. No code or test changes — `MailboxTest.java` already proves every behavior being
named.

**Tech Stack:** Markdown only.

**Spec:** `docs/superpowers/specs/2026-09-03-mailbox-overflow-poison-rejection-semantics-design.md`

## Global Constraints

- Formalization only — do not change mailbox capacity, block-on-full policy, or add any new
  observable behavior (no code or test files are touched by this plan).
- New ADR file path: `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md`,
  following the ADR-003 format (Status/Written-during header, Context, Decision, Alternatives
  considered, Consequences).
- Defined terms and their exact meaning (must be used consistently across all three files):
  - **Poison message** — the message whose processing (`onMessage` or `preStart`) throws,
    causing the actor to stop. Guarantee: cannot cause a retry loop in M1–M3 (no redelivery).
  - **Rejected message** — a message offered to a mailbox that will never process it. Unifies
    three mechanisms: (1) `offer()` after close, (2) a blocked sender unblocked by `close()`,
    (3) an already-enqueued message discarded at `close()`. Unified because `tell()` is `void` —
    callers cannot distinguish these from each other or from success.
  - Overflow (blocking on a full mailbox) is explicitly **not** itself rejection.
- Every cross-reference must use the exact file path
  `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md` (not "ADR-004" alone,
  except in prose that already names the file elsewhere in the same section).

---

### Task 1: Write ADR-004

**Files:**
- Create: `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md`

**Interfaces:**
- Consumes: terminology and mechanism list from the Global Constraints section above.
- Produces: the canonical definitions of "poison message" and "rejected message" that Task 2 and
  Task 3 cross-reference by file path.

- [ ] **Step 1: Write the ADR file**

Create `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md` with exactly this
content:

```markdown
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
```

- [ ] **Step 2: Verify the file was created correctly**

Run: `test -f docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md && wc -l docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md`
Expected: file exists, non-zero line count.

- [ ] **Step 3: Commit**

```bash
git add docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md
git commit -m "Add ADR-004: mailbox overflow, poison-message, and rejection semantics (TASK-203)"
```

---

### Task 2: Update docs/architecture.md

**Files:**
- Modify: `docs/architecture.md:42-49` (§4 Mailbox)
- Modify: `docs/architecture.md:59-63` (§5 Lifecycle, point 3)
- Modify: `docs/architecture.md:72-78` (§6 Failure handling)

**Interfaces:**
- Consumes: ADR-004's definitions from Task 1 (file must exist at
  `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md` before this task's
  cross-references are added).
- Produces: `docs/architecture.md` as the place other docs/code comments point to for the
  "rejected message" (§4) and "poison message" (§6) terms.

- [ ] **Step 1: Replace §4 Mailbox**

In `docs/architecture.md`, replace this block (current lines 42-49):

```markdown
## 4. Mailbox

Each actor has its own `Mailbox<T>`, a FIFO queue.

**Provisional bounds decision (TASK-103a):** the mailbox is bounded with a generous default
capacity (1024) and blocks the sending thread when full ("block-on-full"). This is explicitly
provisional — chosen only so early benchmarks are comparable — and is revisited with real
measurement data at TASK-306 (M3). See `docs/decisions/mailbox-bounds-provisional.md`.
```

with:

```markdown
## 4. Mailbox

Each actor has its own `Mailbox<T>`, a FIFO queue.

**Provisional bounds decision (TASK-103a):** the mailbox is bounded with a generous default
capacity (1024) and blocks the sending thread when full ("block-on-full"). This is explicitly
provisional — chosen only so early benchmarks are comparable — and is revisited with real
measurement data at TASK-306 (M3). See `docs/decisions/mailbox-bounds-provisional.md`.

**Rejected message (TASK-203):** a rejected message is one offered to a mailbox that will never
process it. Exactly three mechanisms produce a rejected message: `offer()` called after the
mailbox is already closed; a sender blocked in `offer()` on a full mailbox, unblocked when
`close()` runs before space frees; and a message already enqueued when `close()` runs, discarded
before delivery (see "Lifecycle," below). Because `ActorRef.tell()` is `void`, a caller cannot
distinguish any of these three from each other, or from successful delivery — "rejected" is a
single, unobservable outcome from the caller's side. Overflow (blocking on a full mailbox) is not
itself rejection: a blocked sender's message is still delivered once space frees, unless the actor
stops while it is waiting. See
`docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md`.
```

- [ ] **Step 2: Replace §5 Lifecycle, point 3**

In `docs/architecture.md`, replace this list item (current lines 59-63):

```markdown
3. On stop, the mailbox is closed immediately: any messages still queued at that moment are
   **discarded, not delivered**, and any sender currently blocked in `tell()` on a full mailbox
   is unblocked (its message is also dropped). The actor finishes the message it is currently
   processing, if any, then `postStop` runs (best-effort — an exception there is logged and
   ignored, since the actor is already terminating).
```

with:

```markdown
3. On stop, the mailbox is closed immediately: any messages still queued at that moment are
   **rejected, not delivered** (§4), and any sender currently blocked in `tell()` on a full
   mailbox is unblocked (its message is also rejected, §4). The actor finishes the message it is
   currently processing, if any, then `postStop` runs (best-effort — an exception there is logged
   and ignored, since the actor is already terminating).
```

- [ ] **Step 3: Update §6 Failure handling**

In `docs/architecture.md`, replace this block (current lines 72-78):

```markdown
## 6. Failure handling (TASK-107a)

There is one, non-configurable default for M1: if `onMessage` (or `preStart`) throws, the
failure is logged with the actor's identity and the message being processed, and *that actor
alone* is stopped, following the normal lifecycle above. No other actor, and not the
`ActorSystem` itself, is affected — this holds structurally, because each actor runs on its own
thread and failures never cross that boundary.
```

with:

```markdown
## 6. Failure handling (TASK-107a)

There is one, non-configurable default for M1: if `onMessage` (or `preStart`) throws, the
failure is logged with the actor's identity and the message being processed, and *that actor
alone* is stopped, following the normal lifecycle above. No other actor, and not the
`ActorSystem` itself, is affected — this holds structurally, because each actor runs on its own
thread and failures never cross that boundary.

**Poison message (TASK-203):** the message whose processing triggers this — the one `onMessage`
or `preStart` was handling when it threw — is termed a poison message. Because M1–M3 have no
redelivery or retry mechanism, a poison message structurally cannot cause a retry loop: it is
processed at most once, the actor stops, and the message is never redelivered to this or any
other actor. This guarantee holds only through M1–M3; M4 (TASK-402) must explicitly re-examine
whether a restarted actor can receive the same poison message again. See
`docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md`.
```

- [ ] **Step 4: Verify the three edits landed and nothing else changed**

Run: `git diff docs/architecture.md`
Expected: exactly three additions (the new "Rejected message" paragraph in §4, the reworded point
3 in §5, and the new "Poison message" paragraph in §6), no other lines touched.

- [ ] **Step 5: Commit**

```bash
git add docs/architecture.md
git commit -m "Formalize rejected-message and poison-message terminology in architecture.md (TASK-203)"
```

---

### Task 3: Update docs/decisions/mailbox-bounds-provisional.md

**Files:**
- Modify: `docs/decisions/mailbox-bounds-provisional.md:24-29` ("What this is not")
- Modify: `docs/decisions/mailbox-bounds-provisional.md:31-36` ("What happens next")

**Interfaces:**
- Consumes: ADR-004's file path from Task 1.
- Produces: nothing consumed by later tasks — this closes out the plan.

- [ ] **Step 1: Replace "What this is not"**

In `docs/decisions/mailbox-bounds-provisional.md`, replace this block (current lines 24-29):

```markdown
## What this is not

* Not a claim that block-on-full is the right policy long-term.
* Not a claim that 1024 is a well-chosen capacity — no benchmark backs that number yet.
* Not exhaustive: TASK-203 (M2) still needs to specify overflow, poison messages, and rejected
  messages formally; this note only covers what TASK-103a needs to unblock M1/M2/M3 work.
```

with:

```markdown
## What this is not

* Not a claim that block-on-full is the right policy long-term.
* Not a claim that 1024 is a well-chosen capacity — no benchmark backs that number yet.
* Not exhaustive: this note only covers what TASK-103a needs to unblock M1/M2/M3 work. Overflow,
  poison-message, and rejected-message semantics are formally specified in TASK-203's
  `docs/decisions/ADR-004-mailbox-overflow-poison-rejection-semantics.md`.
```

- [ ] **Step 2: Fix the stale ADR-003 self-reference in "What happens next"**

In `docs/decisions/mailbox-bounds-provisional.md`, replace this block (current lines 31-36):

```markdown
## What happens next

TASK-306 (M3) revisits this in light of the M3 benchmarks (TASK-301, TASK-302, TASK-307) and
either confirms or replaces it, written up as the full ADR-003. Until then, do not change this
default without updating this note and flagging that the M3 benchmarks it fed may need
re-running.
```

with:

```markdown
## What happens next

TASK-306 (M3) revisits this in light of the M3 benchmarks (TASK-301, TASK-302, TASK-307) and
either confirms or replaces it, written up as its own ADR. Until then, do not change this
default without updating this note and flagging that the M3 benchmarks it fed may need
re-running.
```

(This also fixes a stale reference: the note previously said TASK-306 would be "written up as the
full ADR-003," but ADR-003 is already taken by cross-sender ordering, and ADR-004 is TASK-203's
ADR, not TASK-306's — so the fix points forward without presupposing TASK-306's ADR number.)

- [ ] **Step 3: Verify the two edits landed and nothing else changed**

Run: `git diff docs/decisions/mailbox-bounds-provisional.md`
Expected: exactly two edits (the "What this is not" bullet reworded, the "What happens next"
paragraph reworded), no other lines touched.

- [ ] **Step 4: Commit**

```bash
git add docs/decisions/mailbox-bounds-provisional.md
git commit -m "Point TASK-103a's provisional note at ADR-004; fix stale ADR-003 self-reference (TASK-203)"
```

---

## Final check

- [ ] Run `grep -rn "ADR-004-mailbox-overflow-poison-rejection-semantics" docs/` and confirm it
  resolves in all three files that reference it (`docs/architecture.md` x2,
  `docs/decisions/mailbox-bounds-provisional.md` x1, plus the ADR's own filename).
- [ ] Run `git log --oneline -4` and confirm three new commits exist on top of the spec commit,
  one per task above.
- [ ] Confirm no files outside `docs/` were touched: `git diff --stat main...HEAD` should show
  only the four markdown files (spec + ADR-004 + architecture.md + mailbox-bounds-provisional.md).
