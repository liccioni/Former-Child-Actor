# ADR-016: Death watch (M6)

* Status: Accepted
* Written during: M6
* Builds on: ADR-004 (mailbox rejection semantics — delivery here reuses `tell()`'s existing
  silent-drop-if-terminated contract, not a new one), ADR-008 (supervision strategies and
  hierarchies — a `Restart` never fires a watch; a cascaded/escalated stop does), ADR-015 (`ask()`
  — the closest existing precedent for "let a caller supply a message shape", and a useful contrast
  for what death watch does *not* get for free)

## Context

M4 (supervision) gave an actor's own parent a way to react to its failure. Nothing before M6 let
an *unrelated* actor — not a parent, not a child — find out when some other actor it depends on
terminates, short of polling `ActorRef.isTerminated()`. This is the standard "death watch"
primitive every actor framework ships (Akka's `watch`/`Terminated`, Erlang's `monitor`), and the
next item under `docs/architecture.md`'s "reliable lifecycle" roadmap phase, following M4 and M5.

## Decision

`ActorContext<T>` gains two new methods:

```java
void watch(ActorRef<?> target, T onTerminated);
void unwatch(ActorRef<?> target);
```

`watch` registers `onTerminated` — an ordinary, caller-supplied `T`-typed message — to be enqueued
into this actor's own mailbox exactly once, the moment `target` terminates for any reason. There is
no separate "signal" channel: `Actor<T>` processes exactly one message type via `onMessage`, so
adding a distinct `onSignal`-style hook (as Akka Typed does) would be a much larger, more invasive
change for one feature. Instead this mirrors `ask()`'s existing idea of a caller-supplied message
shape (`ask`'s `messageFactory`), but simpler: the caller passes the exact message value up front,
since (unlike a reply) there is only ever one thing to say — "it died." A watch delivers like any
other message: processed by a later `onMessage` call, on this actor's own dispatcher thread, never
re-entrantly.

### Implementation

`ActorCell<T>` gains a `ConcurrentHashMap<ActorRef<?>, Object> watchedBy` (message type erased,
since watchers have unrelated `T`s — the same kind of erasure `Mailbox` already does within one
actor, just across actors here). `finishTermination()` — already the one place `postStop` runs and
`terminated` flips to `true` — now also drains `watchedBy`, delivering each entry.

`ActorSystem` gains two package-private methods, `watch`/`unwatch`, in the same "look the cell up
by id in `actors`" style `stop()` already uses:

```java
void watch(ActorRef<?> target, ActorRef<?> watcher, Object message) {
  ActorCell<?> targetCell = actors.get(target.id());
  if (targetCell == null || targetCell.ref() != target) {
    deliver(watcher, message);              // unknown, or a different actor now holds this name
    return;
  }
  targetCell.addWatcher(watcher, message);
  if (target.isTerminated()) {
    targetCell.notifyWatcherIfPresent(watcher);   // closes the race below
  }
}
```

Two things worth calling out:

* **The `targetCell.ref() != target` identity check** goes further than `stop()`'s own id-string
  lookup. An actor's name can be reused once the actor that held it has fully terminated and
  deregistered (`spawn`'s `putIfAbsent` allows a fresh actor under the same name once the old
  entry is gone). Without this check, `watch(oldRef)` called after a new, unrelated actor was
  spawned under `oldRef`'s old name would silently attach to the *new* actor instead of correctly
  treating the original as already gone. `stop()` has this same id-reuse gap today; it is not
  fixed here (out of scope — a different method, not part of what M6 asked for), but the new
  `watch`/`unwatch` are written correctly from the start since there is no compatibility reason to
  copy a gap into new code.
* **The race between a late `watch` and the target's own concurrent termination.** `watchedBy` is
  drained two ways — the bulk loop in `finishTermination()`, and this per-watcher fallback in
  `watch()` itself (needed because a watcher might register in the narrow window after `terminated`
  flips `true` but the bulk loop already passed it by, or is running concurrently). Both go through
  the same `notifyWatcherIfPresent`, whose exactly-once mechanism is simply `ConcurrentHashMap`'s
  atomic `remove`: whichever caller's `remove` actually returns the message is the one that
  delivers it; the other sees `null` and does nothing. No lock, no extra flag — the map itself is
  the single source of truth for "has this already fired."

`unwatch` mirrors `watch`'s lookup (including the identity check) and just calls
`targetCell.removeWatcher(watcher)`.

Neither `watch` nor `unwatch` gets a shutting-down guard (unlike `spawn`/`ask`): they don't create
a new registered resource, they only write into an existing cell's map, so a call during shutdown
just races normally with that cell's own termination via the same closing logic described above.

### Semantics

* **A `Restart` never fires a watch.** The actor's identity (its `ActorRef`, its id) survives a
  restart untouched; `terminated` is never set for a restart. Only an actual stop — explicit,
  failed-and-`Stop`/`Escalate`, or cascaded from a supervisor or `ActorSystem.shutdown()` — fires
  it. (`WatchTest.restartingAWatchedActorDoesNotFireTheWatch`.)
* **A cascaded or escalated stop still fires it**, with no special-casing needed: cascading a stop
  (ADR-008) just calls `requestStop()` on every descendant, and each one's own, ordinary
  `finishTermination()` eventually runs and notifies its own watchers exactly as it would for a
  direct stop. (`WatchTest.aChildStoppedByASupervisionCascadeStillFiresPendingWatches`.)
* **Last-watch-wins.** Watching the same target twice from the same watcher before the first watch
  fires replaces the pending message (`ConcurrentHashMap.put` semantics) rather than queuing two
  deliveries. (`WatchTest.reWatchingTheSameTargetReplacesThePendingMessage`.)
* **Watching yourself never delivers.** By the time `finishTermination` runs, `requestStop()` has
  already closed this actor's own mailbox — much earlier in the lifecycle — so a self-notification
  would hit an already-closed mailbox and be silently dropped, the same as any other post-close
  `tell()`. Not special-cased in the implementation; just a consequence of the existing lifecycle
  order, documented and covered by a test proving the actor still terminates cleanly rather than
  hanging. (`WatchTest.watchingYourselfNeverDeliversButTheActorStillTerminatesCleanly`.)
* **`unwatch` is best-effort against an in-flight notification** — if termination's notify loop
  already removed the entry before `unwatch` runs, the message may already be in flight. This
  mirrors "you can't unsend a `tell()` already accepted," not a new kind of gap.

## What this deliberately does not cover

* **No delivery guarantee across a whole-system shutdown.** `ask()` (ADR-015) can unconditionally
  fail a pending ask on `ActorSystem.close()`/`shutdown()` because its result is an independent
  `CompletableFuture`, not gated by any actor's mailbox. A watch notification has no such side
  channel — it is delivered via an ordinary `tell()` into the watcher's own mailbox, and
  `shutdown()` requests every actor's stop (closing every mailbox) essentially at once. If the
  watcher's own mailbox happens to close before the target's `finishTermination` gets around to
  notifying it, the notification is silently dropped — consistent with `tell()`'s long-standing
  contract, not a new gap, but explicitly not the stronger guarantee `ask()` has. Not fixed here;
  would need an independent completion channel per watch, which is far more machinery than this
  feature's actual use case (an actor reacting to another *actor's* death while both are still
  part of a running system) has ever needed.
* **No `Terminated` signal type, no `onSignal` hook.** Deliberately: see "Decision," above. A
  caller who wants to know *which* target died when watching several at once encodes that in the
  message itself (e.g. `watch(child, new ChildDied(child))`), exactly as `ask()`'s `messageFactory`
  already lets a caller embed identifying information in a reply.
* **No "watch everyone who watches me" reflection API** (Akka's `getWatchedActors`-style
  introspection). Nothing today needs it; add it later with real motivation if that changes,
  matching this project's established pattern (e.g. ADR-007 leaving mailbox capacity
  non-configurable-per-actor until a concrete need showed up).
* **`stop()`'s own id-reuse gap is not fixed here** — see "Implementation," above. Worth its own
  follow-up if it ever bites in practice; out of scope for this task.

## Consequences

* `framework-core`: `ActorContext` gains `watch`/`unwatch`; `ActorCell` gains a `watchedBy` map and
  three small package-private methods (`addWatcher`, `removeWatcher`, `notifyWatcherIfPresent`) plus
  one new line in `finishTermination`; `ActorSystem` gains package-private `watch`/`unwatch` and a
  small `deliver` helper. No change to `Mailbox`, `Dispatcher`, `SupervisorStrategy`, or `Actor` —
  purely additive, like `ask()` was. `ActorFailureTest`, `ActorSystemTest`, `SupervisionTest`,
  `AskTest` needed zero edits — the same "nothing existing changed behavior" signal `ask()`'s PR
  used.
* New tests: `framework-core/src/test/java/dev/actorframework/core/WatchTest.java` — a live target
  that later stops; an already-terminated target (bounded well under a larger timeout, proving
  immediate delivery); `unwatch` before termination; `Restart` never firing a watch; a cascaded/
  escalated stop still firing one; multiple watchers of the same target; last-watch-wins; self-watch
  never delivering but the actor still terminating cleanly; the actor-id-reuse hazard (watching an
  old, fully-terminated ref after a new actor was spawned under the same name); and a concurrent-load
  test racing many watch registrations against the target's own termination (`AGENTS.md`'s
  real-concurrency-testing requirement).
* Any future change to what fires a watch, to the exactly-once delivery guarantee, or to the
  identity-check lookup, must engage with this ADR explicitly, per `AGENTS.md`'s ADR-gate list.
