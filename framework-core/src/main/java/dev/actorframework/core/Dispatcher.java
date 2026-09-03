package dev.actorframework.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Executes actor dispatch loops (TASK-105).
 *
 * <p>M1 strategy: one virtual thread per actor, for the lifetime of the actor. This is the simplest
 * strategy that trivially gives sequential per-actor message processing (TASK-106) without
 * additional synchronization, and virtual threads make the per-actor thread cheap. Alternative
 * dispatch strategies (shared executor, work-stealing, hybrid) are evaluated with benchmarks at
 * TASK-303 in M3 — this is deliberately not optimized prematurely.
 */
final class Dispatcher implements AutoCloseable {

  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

  void execute(Runnable actorDispatchLoop) {
    executor.execute(actorDispatchLoop);
  }

  @Override
  public void close() {
    executor.close();
  }
}
