package dev.actorframework.core;

/**
 * Thrown by {@link ActorSystem#ask} when a reply is known, structurally or synchronously, never to
 * arrive — as opposed to simply not having arrived yet, which is reported as a plain {@link
 * java.util.concurrent.TimeoutException} instead. See {@code docs/decisions/ADR-015-ask-pattern.md}
 * for the two cases this covers and why a slow-but-alive target, a dropped request, or a supervised
 * restart all fall under the timeout instead.
 */
public final class AskFailedException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  AskFailedException(String message) {
    super(message);
  }
}
