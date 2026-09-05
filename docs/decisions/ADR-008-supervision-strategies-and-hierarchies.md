# ADR-008: Configurable supervision strategies and actor hierarchies (TASK-402)

* Status: Accepted
* Written during: M4 (TASK-402)
* Supersedes: TASK-107a's fixed failure-handling default, per the ADR-gate `AGENTS.md` places on
  it — see below for exactly what changes and what doesn't.
* Builds on: ADR-004 (poison-message semantics — resolves its flagged open question), ADR-005
  (JMM review — see its TASK-402 addendum for the one field this touches)

## Context

M1 shipped one fixed default (TASK-107a): if `preStart`/`onMessage` throws, the failure is logged
and *that actor alone* stops. `docs/architecture.md` §6 named this "a safety net, not the
supervision model" from the start, and committed M4 to adding the four directives real actor
frameworks use — **Resume, Restart, Stop, Escalate** — plus actor hierarchies, since
`ActorContext` had no parent/child concept at all until now.

This also resolves a question ADR-004 explicitly deferred here: whether a restarted actor can
receive the same poison message again. See ADR-004's own "Resolved at TASK-402" addendum for the
answer (no, mechanically guaranteed by `Mailbox.take()`'s existing dequeue-before-processing
order) — not repeated in full here.

**Escalate's scope was decided deliberately narrow, up front**, to avoid inventing concurrency
machinery this project doesn't have a proven need for yet (see "What this deliberately does not
cover," below).

## Decision

### Top-level actors are unchanged

`ActorSystem.spawn(...)` keeps its exact existing signature. Internally, a top-level actor's
`ActorCell` is always constructed with `parent = null` and `strategy = SupervisorStrategy.stop()`
— byte-for-byte TASK-107a's old default: log and stop alone, nothing configurable at this level.
Every existing test and example (`ActorFailureTest`, `ActorSystemTest`, `hello-world`,
`ping-pong`) needed zero changes, which is itself the regression signal that this holds.

Real supervision only exists for actors spawned via the new `ActorContext.spawnChild(...)` — an
actor becomes a supervisor only by choosing to spawn children.

### Hierarchies

`ActorContext<T>` gains:

```java
<C> ActorRef<C> spawnChild(Supplier<Actor<C>> factory, String name); // defaults to stop()
<C> ActorRef<C> spawnChild(Supplier<Actor<C>> factory, String name, SupervisorStrategy strategy);
```

A child's id is namespaced under its parent's: `parent.id() + "/" + name"`, reusing the same flat
`ActorSystem.actors` registry top-level actors already use — no new lookup structure. **Known,
accepted limitation:** a top-level actor literally named e.g. `"parent/child"` could collide with
a real child `"child"` spawned under a real top-level actor `"parent"`. This fails exactly like any
other duplicate-name spawn today (`IllegalArgumentException`, loud, not silent or corrupting) and
is not solved here — consistent with ADR-007 leaving per-actor mailbox capacity unsettled until a
concrete need shows up, this is flagged rather than speculatively engineered around.

Spawning a child while its parent (or the system) is stopping is rejected
(`IllegalStateException`, mirroring `spawn()`'s existing shutdown guard) — checked both before
registering (fail fast) and immediately after adding the new cell to its parent's children (to
close the window where a concurrent parent-stop's cascade already ran past that point without
seeing the not-yet-added child; see "Cascading a stop," below).

### The four directives

A `SupervisorStrategy` (`Directive decide(Throwable failure)`, plus static factories `resume()`,
`restart()`, `stop()`, `escalate()`, and a general `decide(Function<Throwable, Directive>)` for
varying the directive by exception type) is given to a child at spawn time and consulted only by
that child, about its own failures, on its own dispatcher thread — never across actors or threads.
This is what keeps the whole design free of new cross-thread concurrency machinery: a strategy is
a plain, immutable, stateless function of the failure, fixed at spawn time, not fetched dynamically
from a live (and possibly differently-threaded) `Actor` instance.

* **Resume** — the actor keeps running with its existing state, as if nothing happened. The
  message that caused the failure is dropped, never redelivered. A `preStart` failure resumed this
  way proceeds straight into the message loop with the existing instance (there is no "before the
  exception" state to preserve for `preStart` specifically, unlike `onMessage`).
* **Restart** — the actor is replaced with a fresh instance (`factory.get()` called again) and
  resumes processing. `preRestart(context, failure, message)` is called on the *old* instance
  first, best-effort (caught and log-ignored, exactly like `postStop` always has been); the fresh
  instance's `preStart` is then called, and `postRestart(context, failure)` runs on it,
  best-effort, once that succeeds. The message that caused the failure is dropped, never
  redelivered (ADR-004's addendum). If the fresh instance's own `preStart` itself throws, the
  strategy is re-consulted for *that* new failure and retried — implemented as a loop inside
  `ActorCell`, not recursive method calls, so a persistently-broken `preStart` under a `Restart`
  strategy cannot grow the call stack; it is *not* rate-limited or backed off (see "What this
  deliberately does not cover").
* **Stop** — unchanged from TASK-107a for the failing actor itself: log, stop, normal lifecycle.
  New: stopping now cascades to every actor this one supervises, transitively (see "Cascading a
  stop," below).
* **Escalate** — the failing actor stops, and so does its entire ancestor chain up to the root;
  at *every* ancestor level, that ancestor's own stop cascades back down to its own children too
  (i.e. every sibling subtree at every level along the way also stops). An actor with no parent has
  nowhere to escalate to and stops on its own, same as `Stop` (unreachable today, since a top-level
  actor's strategy is always `stop()` — kept for if a future strategy ever makes a top-level
  `Escalate` possible).

### Cascading a stop

`ActorCell.requestStop()` — already the single mechanism behind explicit `ActorSystem.stop()`,
`ActorSystem.shutdown()`, and the `Stop` directive — now also walks this cell's `children` and
requests each of *them* stop too, transitively. This is one iterative traversal (an explicit
work-queue, not recursive calls), so an arbitrarily deep hierarchy does not grow the call stack
one frame per level. `Escalate` reuses the exact same, already-thread-safe `requestStop()` — it was
already safe to call from any thread (`ActorSystem.stop()` already does, from whatever thread the
caller is on) before this task, so escalating up an ancestor chain from a *child's* dispatcher
thread needs no new synchronization.

## What this deliberately does not cover

* **No restart rate-limiting or backoff.** A `Restart` strategy paired with an actor whose
  `preStart` always throws will retry forever (bounded by CPU, not by any limit this task adds).
  Real actor frameworks add a max-restarts-within-a-time-window policy for exactly this; this task
  does not, matching the project's established pattern of shipping the minimal thing and revisiting
  with real data if a concrete need shows up (e.g. ADR-007 leaving mailbox capacity
  non-configurable-per-actor until motivated).
* **No dynamic re-supervision on Escalate.** A fully general design would let an ancestor's *own*
  supervisor decide to restart or resume it in response to a descendant's escalated failure
  (classic actor-framework semantics). That was considered and deliberately rejected for this
  iteration: it requires a new mechanism letting a *different* thread trigger a restart that must
  still execute on the ancestor's own dispatcher thread (its `actor` instance/state must only ever
  be touched by its own thread, per ADR-005) — real, untested concurrency machinery this project
  does not have a proven need for yet. `Escalate` here always terminates the ancestor chain; if a
  concrete need for dynamic re-supervision emerges, it is new design work, not a silent extension
  of this ADR.
* **The id-namespacing collision** described above is accepted, not solved.

## Consequences

* `framework-core`: new `Directive` (enum) and `SupervisorStrategy` (interface) types; `Actor` gains
  default no-op `preRestart`/`postRestart` hooks; `ActorContext` gains `spawnChild` (two overloads);
  `ActorCell` and `ActorSystem` implement hierarchy tracking and the four directives as described
  above. No change to `Mailbox` or `Dispatcher`.
* `docs/architecture.md` §6 rewritten for the new model; current milestone bumped to M4.
* `AGENTS.md`'s "minimal failure-handling default" ADR-gate bullet now points here.
* `docs/decisions/ADR-004-...md` and `docs/decisions/ADR-005-...md` each carry a short addendum
  (poison-message-on-restart; `actor` no longer `final`, respectively) rather than being rewritten.
* New tests: `framework-core/src/test/java/dev/actorframework/core/SupervisionTest.java`, covering
  all four directives, both cascade directions, the spawn-during-stop race, the shutdown guard, id
  namespacing, and a real-concurrent-load scenario, per `AGENTS.md`'s testing requirements.
  `ActorFailureTest`/`ActorSystemTest` needed no changes — the regression signal that top-level
  behavior is unchanged.
* Any future change to the failure-handling default, hierarchy semantics, or either deliberately
  deferred item above must engage with this ADR explicitly, per `AGENTS.md`'s ADR-gate list.
