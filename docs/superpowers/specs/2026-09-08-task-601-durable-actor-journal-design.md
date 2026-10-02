# TASK-601: Durable per-actor message journal with recovery on restart — design

* Status: Approved
* Tracks: GitHub issue #42 (TASK-601), milestone M6 — Persistence

## Problem

M1–M5 hold all actor state in plain in-memory fields (ADR-005 §1); nothing survives a process
restart. This task designs and implements a write-ahead journal per actor — persisted messages
replayed on `ActorSystem.spawn(...)` to reconstruct an actor's history — with a pluggable storage
abstraction and a local file-backed default (local-first, `AGENTS.md` design principle #3). It
must explicitly reconcile with ADR-004's "poison message... never redelivered" guarantee and
ADR-008's `Restart` directive (a restarted actor must not replay the message that caused the
failure it's recovering from).

## Scope

* **In scope**: opt-in per-actor persistence; a pluggable `JournalStore`/`Journal` byte-level
  storage abstraction; a `MessageCodec<T>` for message serialization; a local file-backed
  `JournalStore` implementation; replay-via-`onMessage` recovery on spawn, unified with live
  dispatch; explicit reconciliation with ADR-004 and ADR-008.
* **Out of scope**: snapshotting / bounding replay time (TASK-602, explicitly the next task);
  compaction or deletion of journaled records; exactly-once side-effect semantics for replayed
  messages (an actor's `onMessage` must tolerate at-least-once redelivery of its own side effects
  if it opts into persistence — the same accepted tradeoff every event-sourced actor framework
  makes, not solved here); a codec implementation beyond the interface (callers supply their own,
  same as they supply their own `Actor<T>`); per-actor journal stores (one store per
  `ActorSystem`, matching one `Dispatcher` per system today).

## Decision

### 1. New interfaces (`framework-core`)

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
  List<byte[]> readAll(); // records in the order they were appended
  default void close() {}
}
```

`JournalStore`/`Journal` operate on raw bytes only — they know nothing about `T`. `MessageCodec<T>`
is the only place serialization is decided, and it's supplied by whoever opts an actor into
persistence. This keeps the storage abstraction pluggable (file-backed today, nothing stops a
future DB-backed store) without coupling it to a serialization library, matching the "small core"
and "pluggable infrastructure" principles (`docs/architecture.md` §8/§7 precedent, e.g. `ask()`
reusing `tell()`/`Mailbox` unchanged rather than inventing new machinery).

### 2. API surface (opt-in, non-persistent actors unchanged)

* `ActorSystem.start(String name, JournalStore store)` — configures one store for the whole
  system. Existing `start()` / `start(name)` configure none.
* `ActorSystem.spawn(Supplier<Actor<T>> factory, String name, MessageCodec<T> codec)` — new
  overload; passing a codec is what opts an actor into journaling. Throws
  `IllegalStateException` if the system has no configured `JournalStore`. Existing `spawn(factory)`
  / `spawn(factory, name)` are byte-for-byte unchanged — a non-persistent actor never touches the
  journal, mirroring how a top-level actor never touches `SupervisorStrategy` beyond the fixed
  `stop()` default (ADR-008 precedent: complexity only when explicitly asked for).
* `JournalStore.fileBacked(Path root)` — the concrete local file-backed default. `root` is always
  explicit; no implicit default directory (`docs/architecture.md` §8 "explicit behavior").
* Child actors (`ActorContext.spawnChild`) are not extended with a persistent overload in this
  task — no concrete need yet; adding one later is a small, additive change to `ActorContext` and
  `ActorSystem.spawnChild` following the same shape as top-level `spawn`.

### 3. `ActorCell` integration: one loop, not two

Replay and live processing become a single loop. `dispatchLoop()` drains a journal-replay iterator
first, then falls through to the mailbox:

```java
private void dispatchLoop() {
  Iterator<T> recovery = journal != null ? replayIterator() : Collections.emptyIterator();
  while (true) {
    T message;
    boolean fromJournal = recovery.hasNext();
    if (fromJournal) {
      message = recovery.next();
    } else {
      message = mailbox.take();
      if (message == null) return;
      if (journal != null) {
        journal.append(codec.encode(message)); // write-ahead: before onMessage runs
      }
    }
    try {
      actor.onMessage(context, message);
    } catch (Throwable t) {
      if (!handleFailureAndDecideContinue(t, message)) return;
    }
  }
}
```

`handleFailureAndDecideContinue` (and therefore all four supervisor directives) needs **no
changes**. This is what makes the ADR-004/ADR-008 reconciliation fall out for free instead of
needing new logic:

* **Write-ahead, not write-after**: a live message is journaled *before* `onMessage` runs, so a
  message that completes processing is always durably recoverable even if the process is killed
  mid-processing — the in-memory effect is lost with the process, but replay reconstructs it from
  scratch on the next spawn. This is the durability guarantee "write-ahead" names.
* **Poison message, live**: unchanged from today — the same catch block, the same strategy
  consultation, regardless of persistence.
* **Poison message, on replay**: if a journaled message throws again during replay (a
  deterministic actor reproducing the same failure), it is handled by the *same* strategy on the
  *same* thread via the *same* code path. Under `stop()` this halts recovery at that record.
  **Accepted limitation, stated explicitly**: a truly poison journaled message fails identically on
  every future recovery attempt — nothing removes it from the journal. This is the well-known
  event-sourcing "poison record" problem. TASK-602's snapshotting is the future lever for bounding
  or working around it; this task does not invent a fix, matching the project's established
  pattern of flagging rather than speculatively solving (e.g. ADR-008's restart-rate-limiting
  deferral).
* **`Restart` directive**: orthogonal to the journal. `Restart` replaces `actor` and re-runs
  `preStart`; the loop above moves on to the next record/message regardless of how the previous
  iteration's failure was handled — the failing record is consumed exactly once from the
  `recovery` iterator, the same way `Mailbox.take()` already consumes a message exactly once
  before `onMessage` runs. No journal-specific `Restart` logic is needed.
* **Corrupt/undecodable record**: `codec.decode()` throwing is a fatal recovery error, not an
  actor failure — the actor never saw a message, so `SupervisorStrategy` is not consulted. It is
  logged and the actor stops immediately, the same way a `preStart` failure on a `null` message
  already short-circuits `dispatchLoop` from starting.

### 4. File-backed default `JournalStore`

* One file per actor id, at `root.resolve(sanitize(actorId))`. Actor ids may contain `/` (child
  namespacing); the id is sanitized into a safe filename (`/` → `_`) rather than creating nested
  directories. Sanitization collisions (e.g. `"a/b"` and `"a_b"`) are an accepted limitation,
  documented not solved — same spirit as ADR-008's id-namespacing caveat.
* Format: sequential length-prefixed records — a 4-byte big-endian length header followed by that
  many payload bytes, repeated to EOF. Minimal, streamable, no external dependency.
* `open(actorId)` creates the root directory and file if absent, and opens the file in append
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
  old history. This is intended: persistence is keyed by actor id, not by process lifetime, the
  same way `ask()`'s reply channel and every other actor identity concept in this codebase is
  keyed by id, not by any particular process instance.

## Alternatives considered

* **Separate command/event split** (a new `applyEvent` method distinct from `onMessage`, classic
  event-sourcing). Rejected: doubles the `Actor` API surface and asks every persistent actor to
  author two methods instead of one, for a guarantee (no replayed side effects) this project has
  no concrete need for yet — replay-via-`onMessage` is simpler and matches "small core."
* **Marker interface (`PersistentActor<T>`) instead of a spawn overload.** Rejected: couples
  persistence to the actor's static type rather than to how it's spawned, and requires
  instantiating the actor once just to check its type; a spawn-time argument is more consistent
  with `spawnChild`'s existing "extra argument decides the behavior" precedent.
* **Per-actor `JournalStore` at spawn time** instead of one per `ActorSystem`. Rejected: no
  concrete need for mixed backends within one system exists yet; one store per system mirrors one
  `Dispatcher` per system today.
* **Write-after (journal only a successfully-processed message)** instead of write-ahead. Rejected:
  silently loses a message's effect forever if the process crashes between a successful
  `onMessage` and the journal write — worse than the accepted "poison record replays forever"
  tradeoff, and contradicts "write-ahead" as named in the task.
* **Implicit default journal directory** (e.g. derived from the `ActorSystem` name) for
  `ActorSystem.start(name)`. Rejected: silent disk I/O a caller didn't ask for contradicts
  `docs/architecture.md` §8's "explicit behavior" principle; `start(name, store)` is explicit and
  costs one extra argument.

## Deliverables

1. `framework-core`: `MessageCodec`, `JournalStore`, `Journal` interfaces; `JournalStore.fileBacked`
   default implementation; `ActorSystem.start(name, store)` and the persistent `spawn` overload;
   `ActorCell` changes described in §3 (unified `dispatchLoop`, journal wiring, termination
   cleanup).
2. New ADR: `docs/decisions/ADR-016-durable-actor-journal-and-recovery.md` (ADR-009–014 are
   unclaimed gaps in this repo's numbering — reserved by the original design document per
   `AGENTS.md`'s ADR table reference — so this task takes the next number after the highest one
   actually in use, ADR-015, rather than risk colliding with a reservation this task can't see),
   covering the write-ahead-via-`onMessage` design, the poison-on-replay accepted limitation, and
   explicit reconciliation with ADR-004 and ADR-008 (each of which gets a short
   "Resolved/addressed at TASK-601" addendum, matching how ADR-004 already carries addenda from
   TASK-402 and M5).
3. `docs/architecture.md`: new §10 "Persistence (M6)"; current-milestone line bumped to M6.

## Testing

Real files via a JUnit `@TempDir`-backed `JournalStore.fileBacked`, plus a fake in-memory
`JournalStore`/`Journal` for fast `ActorCell`-level tests:

1. A persistent actor's state survives a simulated process restart: spawn on system A with a
   store rooted at path P, send messages, close A; spawn a fresh `ActorSystem` on the same path P
   with the same actor id/codec — replayed state matches.
2. Write-ahead durability: a message journaled but whose processing never durably completed in the
   "old" run still replays and is reprocessed on the new run.
3. Poison-on-replay: a journaled message that deterministically throws halts recovery under
   `stop()`, and repeated recovery attempts against the same journal fail identically each time —
   exercised under real concurrent actor load per `AGENTS.md`'s testing requirements, not just a
   single-threaded happy path.
4. `Restart` during replay: a journaled message causes a failure, strategy is `restart()`, the
   fresh instance takes over, replay continues from the *next* record — the failing record is not
   replayed twice.
5. Non-persistent actors (existing `spawn` overloads) are provably untouched: existing
   `ActorSystemTest`/`SupervisionTest`/`AskTest` need zero changes — the same regression signal
   ADR-008 used for top-level actors.
6. A corrupt record and a simulated I/O failure each stop the actor without consulting
   `SupervisorStrategy`.
7. File-backed store: id sanitization for namespaced child ids, append-then-reopen-and-read round
   trip, concurrent actors in one system get isolated files.
