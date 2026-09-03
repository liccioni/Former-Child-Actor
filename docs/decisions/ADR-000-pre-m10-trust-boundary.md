# ADR-000: Pre-M10 trust boundary for transport & cluster

* Status: Accepted
* Written during: M0 (TASK-006)
* Superseded by: TASK-1007 (Security, M10)

## Context

Transport (M7) and Cluster (M9) start accepting network input long before real security
(authentication, encryption, authorization — TASK-1007, M10) is implemented. Without an explicit
statement, it is easy to accidentally expose an unauthenticated listener during M7/M8
development, or in an early adopter's environment who reasonably assumes a framework component
that accepts network connections has been hardened.

## Decision

Until TASK-1007 (M10) lands:

1. **All transport and cluster components assume a trusted network.** No authentication,
   encryption, or authorization is implemented before M10, and none should be assumed present.
2. **Default bindings are localhost-only** unless a component is explicitly configured
   otherwise. A developer must take a deliberate action to expose any framework listener beyond
   the local machine.
3. **No component before M10 may be described, in documentation or examples, as suitable for
   exposure to an untrusted network.** Examples that bind to a non-loopback address must say so
   explicitly and note that this is unsafe outside a trusted network.

This ADR is retired once TASK-1007 (M10) implements real node authentication, encryption, and
authorization. Until then, this is the framework's only stated security posture, and it is
deliberately restrictive.

## Consequences

* TASK-704 (TCP transport, M7) and TASK-902 (cluster membership, M9) must reference this ADR
  explicitly in their own documentation.
* Anyone adding a new network-facing component before M10 inherits this same constraint by
  default — it does not need to be re-derived per component.
* This is a documentation and default-configuration constraint, not (yet) a technical one; M10 is
  where the framework actually becomes safe to expose, not just says it isn't yet.
