# ADR 0009: Semantic predecessor guards

- Status: Accepted
- Date: 2026-08-06
- Issue: [#25](https://github.com/JacekKardys/system-proof/issues/25)
- Prerequisite: [#24](https://github.com/JacekKardys/system-proof/issues/24)

## Context

Proof-subject attribution identifies the exact PostgreSQL transaction, HTTP exchange, and SMPP
exchange that belong to one AML message. Attribution alone does not establish that one semantic
boundary occurred before another interaction was allowed to leave its gateway.

A semantic hold deliberately pauses one selected interaction until test code releases it. A
predecessor guard instead enforces a declared invariant: the matching predecessor boundary must
already exist when the matching successor reaches the coordinator. A later proof-outcome layer
will interpret collected relations and violations across a complete scenario; a guard neither
computes that outcome nor repairs an invalid SUT order.

## Decision

`SemanticControls.guard(SemanticPredecessorGuardSpec)` arms one immutable, environment-owned,
protocol-neutral guard before stimulus. The specification contains:

- one exact `ProofSubjectRef`;
- a typed `SemanticInteractionSelector<T>` for the predecessor;
- a required `SemanticPredecessorBoundary`;
- a typed `SemanticInteractionSelector<T>` for the successor;
- one positive maximum duration starting at arm time.

Holds and guards share `SemanticInteractionSelector<T>`. It selects an exact logical connection,
flow direction, evidence schema, typed evidence value, optional exact subject, and optional native
reference. A native-flow constraint resolves a unique subject-owned contribution for the declared
correlation key and requires the originating contribution and candidate to use the same logical
connection and exact physical gateway session. No former hold-only selector remains as an alias.

The public control API exposes no correlation registry implementation, evidence snapshot,
coordinator monitor, socket, buffer, executor, journal mutation, or gateway permit.

## Semantic boundaries

`OBSERVED` means that a complete typed interaction was recorded. It is useful evidence but is not a
guard boundary by itself.

`CONFIRMED` means that a protocol adapter completely observed and recorded evidence whose typed
meaning confirms the effect. In the AML relation, a matching PostgreSQL `CommitSucceeded` is
confirmed. `CommitAttempt`, a `FORWARD` decision, write start, a timestamp, or journal insertion
order is insufficient.

`FORWARDED` means that the gateway successfully wrote and flushed the exact interaction's original
bytes and then invoked that interaction permit's `forwarded()` callback. Observation,
authorization, `RELEASING`, write start, `writeFailed()`, and `abandoned()` do not establish it.

The AML example declares exactly these relations:

1. the subject's matching PostgreSQL `CommitSucceeded` is `CONFIRMED` before its positive HTTP
   `ResponseCompleted` may be forwarded;
2. the subject's matching positive HTTP `ResponseCompleted` is `FORWARDED` before its positive
   SMPP `DeliverSmResponseCompleted` may be forwarded.

## Linearization and state machine

`SemanticControlCoordinator` is the single linearization point for predecessor observation,
successor permit decisions, forwarded/write-failed/abandoned reports, cancellation, timeout,
required route or observation failure, and environment teardown. Holds and guards use the same
coordinator monitor. Timestamps and scheduler arrival times do not resolve races.

Required-observation failure also leaves a terminal, connection-scoped marker at this point. The
coordinator records it before processing controls that already exist. `arm(...)` and `guard(...)`
check the marker under the same monitor immediately before adding a new control. If failure enters
first, a later registration is rejected even when its preceding outside-lock provider refresh had
captured stale `ACTIVE`. If registration enters first, the failure callback sees and terminates that
control according to the existing state machine. A later `ACTIVE` provider result cannot erase or
bypass the marker, and failure of one `ConnectionId` does not poison another connection.

The explicit guard states are `ARMED`, `PREDECESSOR_OBSERVED`, `PREDECESSOR_SATISFIED`,
`SUCCESSOR_AUTHORIZED`, `SATISFIED`, `VIOLATED`, `CANCELLED`, `TIMED_OUT`, and `FAILED`.

If the predecessor boundary linearizes first, the coordinator records it. A later matching
successor is authorized only after its decision fact is appended. Successful `forwarded()` then
records the exact predecessor/successor relation and completes the guard as `SATISFIED`. A write
failure or abandonment completes it as `FAILED`, never as a satisfied relation.

If the successor linearizes first, the coordinator appends an explicit
`PREDECESSOR_NOT_ESTABLISHED` violation and close decision before returning `CLOSE_SESSION`. The
gateway forwards zero successor bytes and closes the affected session. The guard remains
`VIOLATED`; a later predecessor, release, confirmation, or forwarded callback cannot repair it.
Selectors are evaluated against the guard state before the current interaction is applied. When an
`ARMED` guard's predecessor and successor selectors both match the same `InteractionRef`, that
interaction is an early successor: no predecessor fact is committed and the violation retains only
the successor. A causal relation always requires two distinct interaction identities, even when
both roles use the same connection.
Simultaneous gateway tasks therefore have a deterministic result based only on which task enters
the coordinator first.

One successor decision is aggregate across every matching guard. If any guard requires
`CLOSE_SESSION`, every guard tentatively authorized by that same interaction is completed
`FAILED` before the close decision returns. No partial `SUCCESSOR_AUTHORIZED` state, pending
completion, satisfied relation, or arm-order-dependent result survives the aggregate rejection.

Cancellation, timeout, required observation failure, route/session failure, and teardown are
serialized at the same point and never authorize pending traffic. A timed-out or failed guard is
retained as a fail-closed tombstone so a later target successor cannot pass as unrelated traffic.
The first terminal result wins. A cleanup failure after a violation is emitted only as a typed
`SUPPRESSED_FAILURE` fact with a safe classification; it cannot overwrite the violation.

`SUCCESSOR_AUTHORIZED` is no longer waiting for a timed boundary, but it remains active for
required-observation failure and teardown until its exact forwarding report wins. If failure or
teardown linearizes first, completion becomes `FAILED` or `CANCELLED` and a later `forwarded()`
report cannot append a relation. If `forwarded()` linearizes first, exactly one valid relation and
`SATISFIED` completion remain authoritative. The implementation therefore keeps distinct state
predicates for timed-boundary waiting, failure/teardown activity, and enforcement of a later target
successor; timeout capability is not a general lifecycle definition.

## Exact correlation and concurrent subjects

Every guard belongs to one exact subject, and both selectors must target that subject. Missing or
ambiguous correlation never matches either boundary, never establishes a relation, and never
chooses an arbitrary violation target. A retry or reconnect has a different interaction/session
identity and cannot reuse an old native reference. A shared key is ambiguous rather than first- or
latest-wins.

Guards for different subjects coexist independently. A predecessor for subject A does not satisfy
a guard for subject B, a violation for B does not block unrelated A traffic, and traffic without
the target subject continues through the ordinary coordinator path.

## Journal and secret safety

`SemanticPredecessorGuardEvent` is appended to the existing `ScenarioJournal`. Its typed fields
contain only guard and subject identity, state, required boundary, exact interaction references,
decision, relation or violation kind, and safe failure classification. The explicit renderer uses
only those fields.

No guard event or diagnostic contains evidence payloads, correlation-key material, SMS content,
addresses, SQL or bind values, credentials, raw protocol bytes, or exception messages from codecs
or policies. The coordinator does not create a second event history or causal graph.

Journal sequence, wall-clock time, diagnostic elapsed time, rendered order, socket order across
connections, and independent session ordinals are storage or transport observations, not causal
evidence. Only the serialized guard transition and exact typed relation establish this invariant.

## Verification fixture

The examples-owned AML integration uses REQUIRED PostgreSQL, HTTP, and SMPP routes, real protocol
adapters, a real PostgreSQL container, and deterministic controlled protocol peers. The valid mode
persists RAW and Outbox rows atomically and establishes both relations. Two deliberately invalid
modes expose explicit handshakes: one emits a positive HTTP response while commit confirmation is
blocked, and one emits a positive SMPP response after HTTP authorization but before the exact HTTP
`forwarded()` callback. They use no sleeps, log polling, or scheduling guesses and assert zero
forwarded successor bytes.

Gateway tests separately prove that forwarding a PostgreSQL commit request is not confirmation and
that original authorized successor bytes are written exactly once.

## Consequences and limits

The guard rejects an already-invalid order immediately; it does not wait for a future predecessor
and is not a generic workflow engine. Core remains protocol-neutral and contains no AML API or
protocol-native reference types.

Issue [#26](https://github.com/JacekKardys/system-proof/issues/26) still owns proof-outcome
classification. Issue [#13](https://github.com/JacekKardys/system-proof/issues/13) still owns the
final AML T1 proof API and accepted proof scenario. This decision adds neither taxonomy nor final
proof claim.
