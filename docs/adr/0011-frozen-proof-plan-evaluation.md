# ADR 0011: Frozen proof-plan evaluation

- Status: Accepted
- Date: 2026-08-08
- Issue: [#26](https://github.com/JacekKardys/system-proof/issues/26)
- Builds on: [ADR 0009](0009-semantic-predecessor-guards.md),
  [ADR 0010](0010-secret-safe-diagnostics.md)
- Enables: [#13](https://github.com/JacekKardys/system-proof/issues/13),
  [#62](https://github.com/JacekKardys/system-proof/issues/62)

## Context

Proof subjects, typed correlation, required observation, semantic holds, and predecessor guards
already provide authoritative protocol-neutral facts and enforcement. They deliberately do not
decide whether a whole controlled execution proved its claim. Scenario authors therefore need one
explicit boundary that freezes all required coverage before stimulus, interprets only typed
framework facts, and fails closed when evidence or trust is incomplete.

The final AML T1 scenario and versioned proof-artifact serialization are separate work. This
decision provides the public execution seam they can use without importing PostgreSQL, HTTP, SMPP,
JUnit, or Testcontainers into core.

## Decision

`ProofPlan` is the complete immutable declaration for one primary `ProofSubjectRef`. Its builder
retains declaration order and accepts only bounded typed metadata: prerequisite tokens,
`ConnectionId` plus exact `RequiredObservationProfile`, correlation key plus native-reference
schema, previously declared hold/guard references with their successful terminal states, typed
evidence obligations, explicit guard-owned causal relations, and a positive deadline. It accepts
at most 256 obligations. It retains no predicate, lambda, adapter, payload, throwable, decoded
native reference, or arbitrary rendering source.

One environment execution owns at most one valid proof execution. Its lifecycle is:

```text
DRAFT -> ACTIVATING -> ACTIVE -> EVALUATING -> COMPLETED
```

The execution also owns one internal stimulus lifecycle:

```text
NOT_STARTED -> RUNNING -> COMPLETED
                      \-> FAILED
```

Explicit evaluation has a separate typed lifecycle:

```text
NOT_STARTED -> RUNNING -> COMPLETED
                       \-> FAILED
```

A deadline may freeze `NOT_STARTED` or `RUNNING` evaluation as `TIMED_OUT/DEADLINE_EXPIRED` in the
result without pretending that evaluation completed.

`evaluate()` before or during the stimulus is deterministic API misuse and throws
`IllegalStateException`; it does not manufacture an outcome. `PROVED` requires exactly one
successfully completed stimulus. A thrown stimulus fixes terminal `ERROR`. Activation-time
terminal prerequisite or support outcomes never invoke the stimulus.

Malformed, contradictory, foreign, direct/bypassed, or statically incompatible declarations fail
with `ProofConfigurationException` and create no proof outcome. A structurally valid unsupported
runtime prerequisite or unavailable supported path completes `INCONCLUSIVE` before stimulus. An
internal activation failure completes `ERROR` before stimulus.

Activation validates the subject and every reference against the exact environment, validates
profiles, schemas, capabilities, and obligation coverage, samples fresh observation state, and
then arms all prepared controls as one transaction. A later arming failure cancels every control
that was already armed; cancellation never authorizes held traffic. Only after all controls remain
armed does the control coordinator invoke an internal activation boundary while retaining its
monitor. That boundary atomically enters `ACTIVE`; autonomous control terminal transitions cannot
interleave between the final armed validation and activation. The deadline is scheduled outside
framework monitors and installed only if the execution is still active.

The observation owner assigns every `InteractionRef` and records its session/direction ordinal
before journal publication and any proof or control callback. At the control-owned activation
boundary, while the control monitor is still held, the runtime captures the current ordinal
watermark for every observed stream. The observation tracker retains its recording boundary until
the proof execution has installed that immutable window and entered `ACTIVE`; recording uses the
same tracker-to-proof lock order. It then installs the resulting membership predicate into all
prepared controls before exposing the completed activation transaction. Only identities after
that watermark, or from streams created later, belong to the evidence window. A post-watermark
recording cannot publish its proof callback before `ACTIVE`, while an interaction observed before
the boundary cannot satisfy correlation, evidence, a hold, a guard, or a relation even if its
callback is delayed until after `ACTIVE`. Callback time remains irrelevant and external callbacks
remain outside framework monitors.

## Read model and evaluation

The authoritative `ScenarioJournal` remains the only history. The proof coordinator receives the
same framework-owned typed facts after journal append and maintains only a bounded current-state
index for the frozen plan. It does not expose or scan the journal, duplicate entries, interpret
payloads, or use journal sequence, timestamps, callback-await order, map iteration, or stream
ordinals as causality.

Correlation resolution comes only from the existing proof-subject registry contract. Required
correlation is satisfied only by one subject-owned candidate in the exact connection and native
schema namespace. Missing and ambiguous state remain explicit gaps. Controls resolve only from the
exact environment-owned hold or guard reference. A causal relation resolves only from the typed
predecessor-guard relation or violation fact.

A satisfied or violated predecessor guard publishes one self-contained terminal fact containing
its exact predecessor/successor provenance and, for violation, its close decision and violation
classification. Evaluation therefore cannot interleave between a terminal state and a separate
decisive fact.

Required observation covers the entire evidence window. The exact routed connection and profile
must be `ACTIVE` at activation. Every fresh observation commit is forwarded to the current-state
index. Terminal `FAILED`/`DEGRADED` cache semantics and semantic-control observation-failure
markers remain authoritative; later `ACTIVE` cannot restore lost coverage.

Proof refresh batches contain only the exact `ConnectionId`s declared by the frozen plan. A
provider for an unrelated connection is never called. Explicit evaluation starts at most one
asynchronous required-provider refresh; concurrent callers share that single flight. The proof
deadline bounds their wait. Environment teardown shuts down the daemon refresh worker without
waiting for a blocked provider, and any result arriving after terminal proof or environment close
is ignored. An actual required-provider failure before the deadline makes the proof `ERROR`.
There is no unbounded work queue. Connection-scoped materialization and cleanup failures affect a
proof only when that connection is required; environment-wide and journal-integrity failures remain
fail-closed.

Required-observation failure, explicit evaluation, correlation revalidation, and the deadline
cross the same nested semantic-control and proof-subject boundaries. Therefore their order, not
thread scheduling or callback arrival, determines the terminal result. Deadline expiry always
freezes `INCONCLUSIVE` with an evaluation `TIMED_OUT/DEADLINE_EXPIRED` gap unless `VIOLATED` or
`ERROR` was already terminal. It never invokes the outcome evaluator to manufacture `PROVED` and
never rewrites an already satisfied obligation.

At terminal evaluation, every accepted correlation is revalidated under the proof-subject
registry monitor for the exact subject, key, connection, native-reference schema, and accepted
interaction. Sharing the key with a second subject before this boundary changes the resolution to
`AMBIGUOUS`; sharing it after evaluation cannot rewrite the frozen result. Pre-activation traffic
has no accepted interaction watermark and cannot satisfy the plan.

The protocol-neutral outcome evaluator consumes one detached resolution snapshot. Every plan item
has exactly one of:

`SATISFIED`, `VIOLATED`, `MISSING`, `AMBIGUOUS`, `UNSUPPORTED`, `UNREACHED`, `TIMED_OUT`, `FAILED`,
or `NOT_EVALUATED`.

The closed outcomes are exactly:

- `PROVED`: the stimulus completed successfully and every required item is explicitly
  `SATISFIED`;
- `VIOLATED`: an authoritative explicit counterexample won the terminal transition;
- `INCONCLUSIVE`: evidence or runtime support is missing, ambiguous, unreached, or timed out;
- `ERROR`: framework, gateway, adapter, journal, control, stimulus, evaluator, or teardown trust
  failed before a violation became terminal.

Primary-outcome linearization and completion finalization are separate. Every authoritative
semantic-control operation that can terminalize multiple controls--permit authorization,
`forwarded`, `writeFailed`, `abandoned`, hold release/cancellation/timeout, guard
cancellation/timeout, or required-observation failure--applies its complete bounded fact set before
the immutable resolution/stimulus snapshot is frozen. This is current-state batching, not journal
replay. A relevant selector trust failure is emitted before ordinary matches and fixes `ERROR` at
`CONTROL`; compatible sibling facts remain primary, while a later incompatible fact is retained
only as a bounded type-only secondary diagnostic. An unrelated control that is absent from the
frozen plan cannot suppress a valid required violation. Independent events before or after the
operation remain independently ordered.

Each outer authoritative operation owns one thread-confined token. Successful journal facts,
journal-integrity failures, observation-state changes, and required-observation failure intents are
added to that token in invocation order. Nested authoritative calls join the parent token and cannot
steal or flush it. Only after the action exits does the proof monitor apply the complete token and
select one pending primary outcome. A required-observation failure therefore resolves the exact
observation as `FAILED/OBSERVATION_FAILED` and every guard transition whose journal append committed
as `FAILED/CONTROL_FAILED` in the same primary snapshot. Within an authoritative operation, a hold
or guard transition changes runtime state only after its corresponding append succeeds; an append
failure cannot manufacture a transition or replace already buffered exact facts with a generic
resolution. Exceptional exits clear the owner token and batch state before result finalization.
Unrelated threads do not inherit the token and still linearize independently before or after its
detached application.

The global nested lock order is semantic controls, the authoritative-operation boundary, proof
subjects, journal publication, proof evaluation, and then completion delivery after all framework
monitors are released. Subject creation, arming, and correlation-candidate publication use the
same authoritative boundary as controls. A subject mutation and its fact therefore linearize
entirely before or after a control operation; they cannot enter that operation's batch. The control
action, selector execution, exact current-state correlation validation, journal append, public
callback roots, and potentially blocking user code never run under the proof monitor. The proof
monitor receives only the detached complete batch after the authoritative action. This removes any
proof-to-subject acquisition while preserving control and journal order.

`permit(...)` retains the semantic-control monitor and then enters the authoritative-operation
boundary before evaluating guard or hold selectors. Subject correlation is read only at that point:
a candidate or competing arm that completed first participates in the decision, while a mutation
that lost the boundary cannot affect it. Missing-to-unique and unique-to-ambiguous changes therefore
produce exactly the same result as their mutation-first or permit-first serial order. Native-flow
resolution returns a detached immutable snapshot before invoking its codec/extractor predicate, so
no public selector code runs under the proof-subject registry monitor. The boundary remains held
through the committed hold/guard state and transport decision. A predecessor whose correlation was
invalidated before its permit commit cannot become satisfied or authorize a successor, and a
non-match that became unique before that boundary cannot authorize early-successor bytes.

Selecting the primary outcome freezes its complete primary resolutions but does not yet publish a
`ProofResult`. Relevant independent failures remain eligible for deterministic, type-only,
32-entry secondary retention until the same proof monitor atomically constructs the immutable
result and changes state to `COMPLETED`. They cannot replace the primary outcome or resolutions;
facts after `COMPLETED` are ignored. Strict result construction is the normal producer path. The
last-resort fail-closed recovery is observable in package tests, is zero for every reachable
coordinator scenario, and is exercised only by deliberately injected internal corruption. Fatal
VM, thread termination, and linkage errors are not normalized as proof results.

Every semantic-control
transition first performs mandatory internal actions, then submits each committed public completion
root independently behind a publication gate. Proof finalization cancels the deadline, performs
prepared-control transitions, catches cleanup failures, deterministically orders and caps retained
type-only secondary diagnostics at 32, freezes one result, accepts all finalization-owned completion
work, publishes that result, and only then opens the gate. Concurrent close waits for this
framework-owned handoff before invoking delivery close; it never waits for callback execution.

An environment execution accepts at most 256 controls and therefore at most 768 public roots. The
environment-owned dispatcher starts one daemon virtual-thread task per root and has no work queue
or persistent worker to shut down. Teardown still accepts bounded root outcomes from permits that
were already in flight; no new controls can be declared.
One blocked root cannot prevent later hold or guard roots from completing. Arbitrary user callback
execution order is deliberately unspecified even though framework state transitions remain ordered.
Rejection, thread-start, shutdown, and callback failures are contained and cannot replace an already
fixed result or skip subsequent cleanup. Reentrant completion callbacks may call `result()` or
`evaluate()` and receive the same frozen object.

## Result and report

`ProofResult` is detached and deeply immutable. It preserves plan identity and bounded title, an
opaque primary subject, every ordered obligation resolution, decisive evidence identities or the
decisive gap, the stimulus resolution, the explicit evaluation lifecycle and resolution,
unresolved/not-evaluated items, a type-only primary failure, and bounded secondary diagnostics.
Every obligation resolution carries a detached typed descriptor of the exact prerequisite status,
observation profile, correlation namespace, control reference and expected state, evidence kind,
or causal-relation reference evaluated. Construction validates the complete descriptor-kind-
resolution-reason-connection-provenance matrix. Provenance is role-aware and detached: correlation,
hold, predecessor, and successor roles are explicit and are never inferred from connection identity.
Only a guard control or causal relation may be `VIOLATED`, and both require an explicit successor,
optionally after the predecessor. A satisfied guard or established relation requires a distinct
predecessor followed by a distinct successor, including when both use the same connection.
Non-violated guard provenance is reason-aware: cancellation, timeout, and selector failure retain
no interaction or the predecessor only; correlation invalidation and control failure may also
retain the pair after successor authorization; session abandonment requires predecessor permit
progress and may retain the authorized pair. Successor-only non-violation, a cancellation pair,
duplicate references, and reversed roles are rejected. A
satisfied hold and every reached terminal hold retain exactly one `HOLD` reference. A pre-reach
selector failure or ambiguous match has its exact reason, no `HOLD` reference, and missing hold
evidence; an unreached cancellation may also have no interaction. `NOT_EVALUATED` has one exact terminal reason and
no provenance. Satisfied items remain satisfied after an independent
decisive item regardless of declaration order. Its
deterministic compact `ProofReport` is limited to 64 KiB characters. The outcome and decisive
reason are rendered before truncatable obligation detail, so they survive worst-case truncation.

Reports render only framework-owned identifiers, enums, connection/session/interaction identity,
stages, and normalized throwable type. They never render evidence bytes, payloads, native-reference
values, adapter/runtime objects, exception messages or graphs, predicates, or arbitrary opaque
reference `toString()` output. Full journal and troubleshooting diagnostics remain separate
artifacts governed by ADR 0010.

An activated execution left unfinished at environment teardown completes `ERROR` at `TEARDOWN`
and makes `Environment.close()` fail. It cannot disappear as an apparently successful test.

## Public sequence

The first public seam is deliberately explicit and is not a final DSL:

```java
ProofPlan plan = ProofPlan.builder("claim", "Claim", subject, Duration.ofSeconds(30))
    .prerequisite("environment-ready", environment.proofs().satisfiedPrerequisite())
    .observation("http-observation", httpConnection, httpProfile)
    .correlation("http-correlation", httpConnection, key, httpNativeSchema)
    .control("commit-before-http", commitBeforeHttp, SemanticPredecessorGuardState.SATISFIED)
    .causalRelation("commit-before-http-relation", commitBeforeHttp)
    .build();

ProofExecution execution = environment.proofs().activate(plan);
execution.runStimulus(() -> invokeSystemUnderTest());
ProofResult result = execution.evaluate().require(ProofOutcome.PROVED);
```

## Consequences and boundaries

The evaluator is not part of `SemanticControlCoordinator`, a gateway, protocol session, adapter,
correlation registry, or connection lifecycle. Those owners keep their own state and
linearization. Core imports no protocol, JUnit, or Testcontainers type.

This ADR does not claim the final AML T1 proof, define plan templates or nested/parallel proofs, or
serialize/version proof artifacts. Issue #13 may compose the public guard relations for PostgreSQL
commit before positive HTTP forwarding and positive HTTP forwarding before positive SMPP
acknowledgement. Issue #62 may serialize the already detached result without changing evaluation
semantics.
