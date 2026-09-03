# ADR-001: Why actors?

* Status: Accepted
* Written during: M0

## Context

The framework needs a programming model for systems naturally composed of independent, stateful
components that communicate asynchronously — the kind of system that otherwise tends to get
built out of ad hoc thread pools, shared mutable state guarded by locks, and hand-rolled queues,
with concurrency bugs discovered in production.

## Decision

Adopt the actor model as the framework's core abstraction: a stateful unit of behavior
(`Actor<T>`) that processes messages sequentially, addressed only through a stable reference
(`ActorRef<T>`) that exposes nothing but `tell()`.

This is chosen specifically because it gives, structurally rather than by convention:

* **No shared mutable state.** Actor state lives in plain fields; nothing outside the actor can
  read or write it directly.
* **No locking in application code.** Sequential per-actor message processing (TASK-106) means
  an actor's own state never needs synchronization.
* **A natural unit of failure isolation.** A failure inside one actor is structurally contained
  (see ADR-002 and TASK-107a) rather than something the application has to defend against by
  convention.
* **A model that scales down and up.** The same `tell()`-based programming model works whether
  the actor is in the same JVM or, later, on another machine (M8) — the application does not
  need to change when the system grows from one process to a cluster.

## Alternatives considered

* **Bare threads + locks / concurrent collections.** Rejected as the primary model: correct but
  does not scale to "a small team can reason about this," and does not naturally extend to
  distribution.
* **CompletableFuture / reactive streams as the primary abstraction.** Good for pipelines of
  transformations, not for long-lived stateful components that need to hold state across many
  independent inputs over time. Complementary, not a replacement.
* **Full compatibility with an existing actor framework (e.g. Akka).** Explicitly rejected — see
  Section 2 ("Product Philosophy") of the design document. The goal is to take the useful ideas
  behind actor systems and rethink them around modern Java (virtual threads, records, sealed
  interfaces), not to reproduce an existing API surface.

## Consequences

* The entire M1 API surface (`Actor`, `ActorRef`, `ActorContext`, `ActorSystem`) follows directly
  from this choice.
* Anything that would let application code reach into another actor's state without going
  through `tell()` is a violation of this decision and needs its own ADR to introduce.
