# actor-framework

A small, understandable Java runtime for systems built from independent, stateful components
that communicate through messages — starting as a single-process library, with room to grow
into a distributed runtime later without changing the application's programming model.

This repository is in **M1 (Minimal Actor Runtime)**. See
[`docs/architecture.md`](docs/architecture.md) for the full roadmap and design philosophy.

## Requirements

* JDK 25
* Gradle (via the included wrapper — no local Gradle install needed)

## Building

```
./gradlew build
```

This compiles every module, runs all unit tests, and builds the example applications.

## Modules

```
framework-core       The actor runtime: Actor, ActorRef, ActorSystem, Mailbox, Dispatcher.
framework-testkit     Minimal test support for asserting on actor behavior.
examples/hello-world  The smallest complete application.
examples/ping-pong    Two actors exchanging messages, each holding its own state.
benchmarks            JMH benchmark suite (scaffolded; implemented starting M3).
docs/decisions        Architecture Decision Records (ADRs).
```

## Try it

```
./gradlew :examples:hello-world:run
./gradlew :examples:ping-pong:run
```

## Quick taste of the API

```java
try (ActorSystem system = ActorSystem.start()) {
    ActorRef<OrderCommand> orders = system.spawn(OrderActor::new);
    orders.tell(new CreateOrder(...));
    orders.tell(new CancelOrder(...));
}
```

## Contributing

Read [`AGENTS.md`](AGENTS.md) before making changes — it documents module boundaries, testing
requirements, and which decisions require a new ADR rather than a silent code change.
