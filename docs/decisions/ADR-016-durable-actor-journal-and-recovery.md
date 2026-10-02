# ADR-016: Durable per-actor message journal and recovery on restart (TASK-601, M6)

* Status: Accepted
* Written during: M6 (TASK-601)
* Builds on: ADR-004 (poison-message semantics — reconciles journal replay with it), ADR-008
  (supervision strategies — reconciles `Restart` with replay)

## Context

M1–M5 hold all actor state in plain in-memory fields (ADR-005 §1); nothing survives a process
restart. This task designs and implements a write-ahead journal per actor — persisted messages
replayed on `ActorSystem.spawn(...)` to reconstruct an actor's history — with a pluggable storage
abstraction and a local file-backed default (local-first, `AGENTS.md` design principle #3). It
must explicitly reconcile with ADR-004's "poison message... never redelivered" guarantee and
ADR-008's `Restart` directive (a restarted actor must not replay the message that caused the
failure it's recovering from).

## Decision

### New interfaces (`framework-core`)

```java
public interface MessageCodec<T> {
  byte[] encode(T message);
  T decode(byte[] bytes);
}

public interface JournalStore extends AutoCloseable {
  Journal open(String actorId);
  default void close() {}
}

public interface Journal extends AutoCloseable {
  void append(byte[] record);
  List<byte[]> readAll();
  default void close() {}
}
```

`JournalStore`/`Journal` operate on raw bytes only — they know nothing about `T`. `MessageCodec<T>`
is the only place serialization is decided, supplied by whoever opts an actor into persistence.
This keeps the storage abstraction pluggable (file-backed today, nothing stops a future DB-backed
store) without coupling it to a serialization library, matching "small core" and "pluggable
infrastructure" (`docs/architecture.md` §7/§8, e.g. `ask()` reusing `tell()`/`Mailbox` unchanged
rather than inventing new machinery).

### API surface (opt-in, non-persistent actors unchanged)

* `ActorSystem.start(String name, JournalStore store)` configures one store for the whole system.
  Existing `start()`/`start(name)` configure none.
* `ActorSystem.spawn(Supplier<Actor<T>> factory, String name, MessageCodec<T> codec)` is a new
  overload; passing a codec is what opts an actor into journaling. Throws `IllegalStateException`
  if the system has no configured `JournalStore`. Existing `spawn(factory)`/`spawn(factory, name)`
  are byte-for-byte unchanged — a non-persistent actor never touches the journal, mirroring how a
  top-level actor never touches `SupervisorStrategy` beyond the fixed `stop()` default (ADR-008
  precedent: complexity only when explicitly asked for).
* `JournalStore.fileBacked(Path root)` is the concrete local file-backed default. `root` is always
  explicit; no implicit default directory (`docs/architecture.md` §8 "explicit behavior").
* Child actors (`ActorContext.spawnChild`) are not extended with a persistent overload in this
  task — no concrete need yet; adding one later is a small, additive change to `ActorContext` and
  `ActorSystem.spawnChild` following the same shape as top-level `spawn`.

### `ActorCell` integration: one loop, not two

Replay and live processing are a single loop: `dispatchLoop()` drains a journal-replay iterator
first, then falls through to the mailbox, journaling each live message *before* `onMessage` runs
(write-ahead). `handleFailureAndDecideContinue` (and therefore all four supervisor directives)
needed **no changes** — this is what makes the ADR-004/ADR-008 reconciliation fall out for free
instead of needing new logic:

* **Write-ahead, not write-after**: a live message is journaled before `onMessage` runs, so a
  message that completes processing is always durably recoverable even if the process is killed
  mid-processing — the in-memory effect is lost with the process, but replay reconstructs it from
  scratch on the next spawn.
* **Poison message, live**: unchanged from today — the same catch block, the same strategy
  consultation, regardless of persistence.
* **Poison message, on replay**: if a journaled message throws again during replay (a
  deterministic actor reproducing the same failure), it is handled by the *same* strategy on the
  *same* thread via the *same* code path. Under `stop()` this halts recovery at that record.
  **Accepted limitation, stated explicitly**: a truly poison journaled message fails identically
  on every future recovery attempt — nothing removes it from the journal. TASK-602's snapshotting
  is the future lever for bounding or working around it; this task does not invent a fix.
* **`Restart` directive**: orthogonal to the journal. `Restart` replaces `actor` and re-runs
  `preStart`; the loop moves on to the next record/message regardless of how the previous
  iteration's failure was handled — the failing record is consumed exactly once from the recovery
  iterator, the same way `Mailbox.take()` already consumes a message exactly once before
  `onMessage` runs. No journal-specific `Restart` logic is needed.
* **Corrupt/undecodable record**: `codec.decode()` throwing is a fatal recovery error, not an
  actor failure — the actor never saw a message, so `SupervisorStrategy` is not consulted. It is
  logged and the actor stops immediately.

### File-backed default `JournalStore`

* One file per actor id, at `root.resolve(sanitize(actorId))`. Actor ids may contain `/` (child
  namespacing); the id is sanitized into a safe filename (`/` → `_`) rather than creating nested
  directories. Sanitization collisions (e.g. `"a/b"` and `"a_b"`) are an accepted limitation,
  documented not solved.
* Format: sequential length-prefixed records — a 4-byte big-endian length header followed by that
  many payload bytes, repeated to EOF. Minimal, streamable, no external dependency.
* `open(actorId)` creates the root directory and file if absent, and opens the file in read/write
  mode, kept open for the actor's entire lifetime — closed on actor termination, alongside
  `postStop`.
* `readAll()` reads the file from the start, once, at replay time, before any live append happens
  on that same open handle.
* **No internal synchronization.** A given `Journal` instance is only ever touched by its actor's
  own dispatcher thread — append happens inline in `dispatchLoop`, `readAll` happens once before
  the live loop starts. Same reasoning as `Mailbox`'s actor-owned state (ADR-005), stated
  explicitly so it is not accidentally reused across threads later.
* I/O failures (disk full, permission error) on `append`/`open` propagate as unchecked exceptions
  (wrapping `IOException`), treated like the corrupt-record case: fatal to that actor, not
  `SupervisorStrategy`-arbitrated, since it is an environment problem, not actor logic.
* No automatic cleanup or deletion. A persistent actor's journal outlives an explicit
  `ActorSystem.stop()`/termination — respawning a new actor under the same id later replays the
  old history. Persistence is keyed by actor id, not by process lifetime, the same way `ask()`'s
  reply channel and every other actor identity concept in this codebase is keyed by id.

## What this deliberately does not cover

* **Snapshotting / bounding replay time** (TASK-602, explicitly the next task).
* **Compaction or deletion of journaled records.**
* **Exactly-once side-effect semantics for replayed messages** — an actor's `onMessage` must
  tolerate at-least-once redelivery of its own side effects if it opts into persistence, the same
  accepted tradeoff every event-sourced actor framework makes.
* **A codec implementation beyond the interface** — callers supply their own, same as they supply
  their own `Actor<T>`.
* **Per-actor journal stores** — one store per `ActorSystem`, matching one `Dispatcher` per system
  today; no concrete need for mixed backends within one system exists yet.
* **A separate command/event split** (a new `applyEvent` method distinct from `onMessage`).
  Rejected: doubles the `Actor` API surface for a guarantee this project has no concrete need for
  yet — replay-via-`onMessage` is simpler and matches "small core."
* **A marker interface (`PersistentActor<T>`) instead of a spawn overload.** Rejected: couples
  persistence to the actor's static type rather than to how it's spawned; a spawn-time argument is
  more consistent with `spawnChild`'s existing "extra argument decides the behavior" precedent.
* **Write-after (journal only a successfully-processed message)** instead of write-ahead.
  Rejected: silently loses a message's effect forever if the process crashes between a successful
  `onMessage` and the journal write — worse than the accepted "poison record replays forever"
  tradeoff, and contradicts "write-ahead" as named in the task.
* **An implicit default journal directory** for `ActorSystem.start(name)`. Rejected: silent disk
  I/O a caller didn't ask for contradicts `docs/architecture.md` §8's "explicit behavior"
  principle.

## Consequences

* `framework-core`: new public `MessageCodec`, `JournalStore`, `Journal` interfaces; new
  package-private `FileBackedJournalStore`/`FileBackedJournal`; `ActorSystem` gains
  `start(name, store)` and `spawn(factory, name, codec)`; `ActorCell` gains `journal`/`codec`
  fields, a unified `dispatchLoop`, and journal cleanup in `finishTermination`.
* No change to `Mailbox`, `Dispatcher`, `SupervisorStrategy`, or any existing public signature —
  purely additive. `ActorFailureTest`, `ActorSystemTest`, `SupervisionTest`, `AskTest` needed no
  edits, the regression signal that nothing existing changed behavior.
* `docs/decisions/ADR-004-...md` and `docs/decisions/ADR-008-...md` each carry a short "Resolved
  at TASK-601" addendum. `docs/architecture.md` bumped to M6 and gains a §10 "Persistence"
  section.
* New tests: `FileBackedJournalStoreTest` (round trip, id sanitization, concurrent isolated files
  under real load) and `PersistentActorTest` (write-ahead ordering, poison-on-replay halting and
  repeating identically — including under concurrent load — `Restart`-during-replay resuming at
  the next record, a corrupt record and an I/O failure each stopping the actor without consulting
  `SupervisorStrategy`, a real file-backed process-restart round trip, the no-store error, and
  confirmation that non-persistent `spawn` overloads are untouched).
* Any future change to what "poison record" or "write-ahead" means for the journal, or to the
  replay-via-`onMessage` design, must engage with this ADR explicitly, per `AGENTS.md`'s ADR-gate
  list.
