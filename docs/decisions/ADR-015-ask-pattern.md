# ADR-015: `ask()` request-response messaging (M5)

* Status: Accepted
* Written during: M5
* Builds on: ADR-004 (mailbox rejection semantics — resolves its flagged open question), ADR-008
  (supervision strategies and hierarchies — reasons about, but does not change, how `ask()`
  interacts with a `Restart`'s message-drop and a supervision cascade's stop)

## Context

M1 deliberately left request-response messaging out. `docs/architecture.md` §8 committed to
designing it here, in M5, before adding it. Everywhere the repo has needed "wait for a reply" in
the meantime, it hand-rolled a completion signal instead of a real request-response call:
`benchmarks`' `LatchCountingActor` carries a `CountDownLatch` in the message payload;
`framework-testkit`'s `TestProbe` is a plain recorder with no request/reply correlation — any
message from any sender lands in the same queue, in arrival order.

ADR-004 explicitly left a question open for whenever this day came: "a future signaled send path
(`ask()`, designed in ADR-015 at M5) may need to expose which of the three [rejection] mechanisms
rejected a message, or expose rejection at all." Having now designed `ask()`, the answer is: no —
see "Decision," below.

## Decision

`ActorSystem` gains one new method:

```java
public <REQ, RES> CompletionStage<RES> ask(
    ActorRef<REQ> target, Function<ActorRef<RES>, REQ> messageFactory, Duration timeout)
```

`messageFactory` is handed a reply-to `ActorRef<RES>` to embed in the outgoing request; whatever
the first message sent to that reply-to ref is becomes the returned stage's result. Internally,
`ask()` spawns a small one-shot actor (`AskReplyActor`, a private nested class of `ActorSystem`) as
the reply-to target: its `onMessage` completes a `CompletableFuture<RES>` with whatever it receives
and immediately stops itself; its `postStop` unconditionally tries to fail that same future on the
way out (a documented no-op if a reply already arrived, since `CompletableFuture.complete`/
`completeExceptionally` are no-ops once a future is already completed — the one path that
*actually* needs this is `ActorSystem.close()`/`shutdown()` running while the ask is still
pending, which would otherwise leave the stage incomplete forever). This is the first use of
`CompletableFuture`/`CompletionStage` anywhere in the repo; nothing existing to stay consistent
with, and ADR-001's own "Alternatives considered" already treats `CompletableFuture` as fine for an
individual, narrow, complementary call — just not as the framework's core programming model.

### Only two failure shapes, not a rejection taxonomy

This resolves ADR-004's open question: `ask()` does **not** need the three-mechanism rejection
taxonomy after all. It exposes exactly two outcomes beyond a normal reply:

1. **`AskFailedException`** (new, `framework-core`'s first custom exception type) — for a failure
   that is *synchronously or structurally knowable*, never for "no reply yet":
   - `target.isTerminated()` is already `true` the moment `ask()` is called. Checked first, before
     spawning anything or sending anything — fails immediately, not after waiting out `timeout`.
   - The internal `AskReplyActor` itself stops for any reason before ever receiving a reply. Today
     the only way that happens is `ActorSystem.close()`/`shutdown()` running while the ask is still
     pending — described generically in the exception's message since nothing else can stop it.
2. **`java.util.concurrent.TimeoutException`** (JDK's own type, via `CompletableFuture.orTimeout`)
   — everything else: a slow-but-alive target, a request a `Restart` directive silently dropped
   (ADR-008: a restarted actor never redelivers the message that failed), a target stopped by a
   supervision cascade, or the accepted race below. `ask()` never distinguishes *why* no reply
   came — matching ADR-004's own "rejected is a single, unobservable outcome" philosophy, now
   extended to the send-with-a-future-reply case ADR-004 deferred.

### `orTimeout`/cleanup mechanics

`future.whenComplete((result, error) -> stop(replyTo))` is attached to the original
`CompletableFuture`; `orTimeout` completes and returns that same instance (per its own javadoc), so
this cleanup fires exactly once no matter which of the three paths — a real reply, `postStop`'s
catch-all, or `orTimeout`'s own timeout — completes it first. No new executor or scheduler: JDK 9+'s
`CompletableFuture.orTimeout` already provides one internally.

## What this deliberately does not cover

* **The `target.isTerminated()`-then-`tell()` race.** If `target` terminates in the window between
  that check and actual delivery, `tell()` silently drops the message (its own long-documented
  contract) and the ask times out rather than fast-failing with `AskFailedException` — structurally
  the same kind of accepted, unobservable gap ADR-004 already documents for `tell()` itself. Closing
  it would need cross-thread signaling into another actor's cell — exactly the kind of machinery
  ADR-008 already chose not to build for `Escalate`. Not fixed here; the `isTerminated()` check is a
  best-effort fast path, not a guarantee.
* **No distinction between why a timeout happened.** A slow target, a dropped restart, and a
  supervision-cascade stop are all just "no reply within `timeout`" from `ask()`'s perspective —
  deliberately, not an oversight (see "Only two failure shapes," above).
* **Actor-to-actor request-response ergonomics.** `ask()` lives on `ActorSystem`, reachable from
  inside an actor via `context.system().ask(...)`, but nothing here adds a safer wrapper for it.
  Attaching a callback (`.thenApply`/`.thenAccept`) to the returned stage runs on an arbitrary
  thread — whichever thread completes the future — never guaranteed to be the calling actor's own
  dispatcher thread; touching that actor's own state from such a callback would violate the
  single-thread-per-actor guarantee ADR-005 relies on. The safe pattern (pipe the result back as an
  ordinary message via `self.tell(...)` from the callback, and only act on it from `onMessage`) is
  documented in `ask()`'s javadoc but not otherwise enforced or wrapped — no concrete caller need
  for a "pipe to self" convenience exists yet.
* **A non-positive `timeout` is rejected loudly** (`IllegalArgumentException`), matching
  `Mailbox(int capacity)`'s existing precedent for validated constructor/method inputs — a small
  addition, not a deferred question.

## Consequences

* `framework-core`: new `AskFailedException` (public, unchecked); `ActorSystem` gains `ask(...)`,
  the private `AskReplyActor` nested class, and a package-private `registeredActorCount()` test
  accessor — the first thing in this codebase that is package-private *solely* for a test's
  benefit, not for a production reason (`ActorSystem.actors` itself stays `private`), justified
  because no other observable signal exists for "was the reply actor actually cleaned up" without
  reflection, which this codebase never uses.
* No change to `Mailbox`, `ActorCell`, `Dispatcher`, `SupervisorStrategy`, or any existing public
  signature — `ask()` is purely additive. `ActorFailureTest`, `ActorSystemTest`, `SupervisionTest`
  needed no edits, the regression signal that nothing existing changed behavior.
* `docs/decisions/ADR-004-...md` carries a short "Resolved at M5" addendum rather than being
  rewritten. `docs/architecture.md` bumped to M5 and gains an `ask()` section; its §8 line claiming
  `ask()` doesn't exist yet is updated.
* New tests: `framework-core/src/test/java/dev/actorframework/core/AskTest.java` — a successful
  reply; the postStop-after-success no-op interaction specifically; a genuine timeout; the
  already-terminated fast-fail path (bounded well under the ask's own timeout, proving it doesn't
  just wait it out); concurrent asks to the same actor each resolving to their own correct reply
  (`AGENTS.md`'s real-concurrency testing requirement); `ActorSystem.close()` failing a pending ask
  instead of hanging; an ask to a `spawnChild`-created child behaving the same as to a top-level
  actor; `ask()` throwing synchronously (not via the stage) once the system is shutting down, same
  as `spawn()`; the reply actor's deregistration after both a successful and a timed-out ask; and
  the non-positive-timeout rejection.
* Any future change to `ask()`'s failure taxonomy, or to what `AskFailedException` covers, must
  engage with this ADR explicitly, per `AGENTS.md`'s ADR-gate list.
