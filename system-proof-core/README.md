# System Proof Core

This module contains the core domain contracts, extension SPI, read models, and environment
execution implementation. The canonical compatibility and dependency policy is
[Package and API architecture](../docs/architecture/package-api-architecture.md).

Public contracts:

- `environment.Environment`: lifecycle, diagnostics, and reverse-order cleanup over an
  immutable topology.
- `environment.EnvironmentBuilder` and `environment.EnvironmentCreator<E>`: the mutable component,
  connection, configuration, and logging boundary plus the typed facade creation callback.
- `environment.ComponentPortFactory`: low-level helpers for programmatic port declarations; annotated
  fields remain the normal declarative path.
- `environment.EnvironmentTopology`: one concrete immutable model of components and logical
  connections, consumed by environment facades and runtime code.
- `component.Component` and `component.AbstractComponent<C, O>`: one component identity,
  typed configuration, owned ports, driver, lifecycle state, and optional typed operations.
- `@SystemComponent` and `ComponentConfig<D>`: declarative component type, driver, flattened
  component configuration, and separate driver-only configuration.
- `RequiredPort<C>`, `ProvidedPort<C>`, `Contract<C>`, `ConnectionId`, and `Connection<C>`:
  directional typed topology without runtime addresses.
- detached `RuntimeConnectionSnapshot` values: inspection of the one authoritative internal
  runtime materialization per logical connection, without exposing endpoint values.
- `ConnectionRouting`, `ConnectionRouteProvider<C>`, `ConnectionRouteContext<C>`, and
  `ConnectionRoute<C>`: typed runtime selection, orthogonal observation policy, connection-scoped
  observation access, and a connection-owned effective endpoint/resource seam without topology
  proxy DSL.
- `ObservationRequirement`, `RequiredObservationProfile`, `EffectiveObservationStatus`,
  `InteractionDecisionCoordinator`, and `ForwardingDecision`: explicit observation intent,
  scenario-owned schema/capability requirements, immutable effective state, and one
  environment-scoped decision boundary.
- `ComponentDriver<C, O>`, `ComponentBoundDriver<C, O, T>`, component-scoped `DriverContext`,
  restricted `JournalContributions`, and `ComponentRuntime<O>`: runtime materialization SPI.
- `configuration.EnvironmentConfiguration` and `Secret<T>`: immutable external values and redacted
  secrets.
- the open `ScenarioEvent` read contract, framework-owned event envelopes, `JournalEntry`, `JournalSequence`,
  `ScenarioJournalSnapshot`, `JournalRenderer`, `EvidenceCodec<T>`, and `EvidenceSnapshot`:
  detached inspection/rendering contracts and the external typed-evidence copy boundary. Mutable
  journal storage is not public.
- `ConnectionObservations`, `InteractionSession`, `SessionId`, `FlowDirection`, and
  `InteractionRef`: protocol-neutral, connection-bound traffic identity and contribution boundary.
- `ProofSubjects`, `ProofSubjectRef`, `CorrelationKey`, `CorrelationContribution<T>`, and
  `CorrelationResult<T>`: environment-scoped opaque subject identity, secret-safe semantic keys,
  detached typed native references, and explicit cardinality.
- `SemanticControls`, `SemanticInteractionSelector<T>`, `SemanticHold`, and
  `SemanticPredecessorGuard`: one environment-owned control boundary for deterministic holds and
  exact subject-scoped predecessor enforcement.
- `ProofPlan`, `Proofs`, `ProofExecution`, `ProofResult`, and their closed stimulus, evaluation,
  outcome, and obligation-resolution values: one bounded frozen declaration, explicit
  activation/stimulus/evaluation sequence, and a detached fail-closed result for one environment
  execution.
- `EnvironmentLogging`, top-level `EnvironmentLoggingBuilder`, `EnvironmentDiagnostics`, and
  `EnvironmentStartException`: logging configuration, rendered journal views, and failure reporting.

Packages are grouped by domain owner, not by Java shape. The exact supported API/SPI and read-only
surface is maintained in the canonical architecture document and enforced from compiled bytecode,
including nested types.

Core validates component ID uniqueness, port ownership and direction, contract/interaction/protocol
compatibility, exactly one provider per required port, logging references, dependency cycles, and
complete provided-port materialization.

## Frozen proof-plan execution

`Environment.proofs()` owns at most one valid proof execution. A plan names one opaque primary
subject and declares every required prerequisite, routed observation profile, typed correlation,
prepared hold/guard terminal state, evidence item, and guard-owned causal relation. Declarations
are ordered, immutable after `build()`, capped at 256 items, and contain no payload, predicate,
adapter, throwable, or protocol object.

Activation validates exact environment ownership and static profile/schema/capability coverage,
samples fresh observation state, and arms all prepared controls before the evidence window becomes
`ACTIVE`. An execution may declare at most 256 semantic controls; this bound also caps public
completion delivery. Partial arming is rolled back without forwarding protected traffic. Unsupported runtime
coverage completes `INCONCLUSIVE`; malformed plans throw `ProofConfigurationException`; internal
activation failure completes `ERROR`. In every pre-stimulus outcome the stimulus callback is not
invoked. The observation owner records each allocated `InteractionRef` before journal publication
and proof callbacks. While the control activation transaction still owns its monitor, it captures
one per-session/direction ordinal watermark and retains the observation recording boundary until
the proof is `ACTIVE`. Only later observation identities belong to the proof window; callback
arrival order can neither admit pre-boundary traffic nor discard post-boundary traffic.

The environment consumes only framework-owned typed facts into a bounded current-state index. It
does not scan or duplicate the scenario journal and never infers correlation or causality from
journal sequence, time, await order, or map iteration. Required observation remains an obligation
for the whole evidence window, including sticky intermediate failure. Correlation remains owned by
the existing subject registry, and causal relations remain owned by predecessor-guard facts.

The terminal outcome has one linearization point. `PROVED` requires every item to be explicitly
`SATISFIED`; an explicit guard counterexample yields `VIOLATED`; missing, ambiguous, unsupported,
unreached, or timed-out coverage yields `INCONCLUSIVE`; trust loss yields `ERROR`. Later facts do
not change the primary outcome. The deadline crosses the same control/observation/correlation
boundary as explicit evaluation and always yields a typed `INCONCLUSIVE` evaluation gap unless an
earlier violation or error already won. Required observation refresh is single-flight and bounded
by that deadline; teardown never waits for a blocked provider, and a late refresh is discarded.
Every semantic-control transition separates deterministic internal actions from user-visible stage
completion. Each committed public root is accepted by an environment-owned dispatcher as a
separate daemon virtual-thread task; at most 768 roots exist for the 256-control lifetime bound.
There is no delivery queue and callback execution order is unspecified. A publication gate keeps
terminal callbacks behind immutable result publication, while close waits only for submission and
state handoff, never for user code. Blocking or failing dependents therefore cannot hold protocol
decisions, stimulus, proof finalization, unrelated roots, or teardown. At most 32 later
type-only diagnostics may be retained, and repeated evaluation/result access returns the same
immutable object. An unfinished active execution makes environment teardown fail. The deterministic
compact report is capped at 64 KiB and never renders payloads, evidence bytes, native references,
exception messages, or arbitrary opaque-reference `toString()` output. See
[ADR 0011](../docs/adr/0011-frozen-proof-plan-evaluation.md).

## Secret-safe diagnostics

`Environment.diagnostics()` is the only default environment report and returns an
`EnvironmentDiagnostics` instance that callers cannot construct from arbitrary text. It contains
typed state, a bounded safe journal rendering, and only `DiagnosticSource.redacted(...)` values.
The complete report and journal rendering are each limited to 256 KiB characters; at most 32
redacted sources are captured. One immutable lifecycle/component/connection/journal/source
snapshot is copied under the `EnvironmentRuntime` monitor; suppliers are invoked and the result is
rendered only after that monitor is released.

Driver text crosses the journal boundary only as `RedactedDiagnosticText`, created with an explicit
sanitizer over at most 16 KiB characters and retaining at most 4 KiB/64 lines. Sanitizer failure,
`null`, blank, or oversized output never falls back to raw input. `DiagnosticSource.sensitive(...)`
and `DiagnosticSource.unsupported(...)` classify content that the framework never invokes or
exports. `DiagnosticSource.redacted(...)` applies these bounds only after `Supplier.get()` returns;
the trusted driver remains responsible for bounding the acquisition itself. There is no raw or
sensitive attachment API.

`FailureDetails` retains only one bounded, normalized `String` containing the throwable type name.
It retains neither the throwable nor its `Class` object and never reads an exception message,
cause/suppressed message, stack trace, or `Throwable.toString()`. Component configuration and
arbitrary evidence/extension objects are not rendered. See
[`ADR 0010`](../docs/adr/0010-secret-safe-diagnostics.md) for the source inventory, prohibited data,
limits, and residual sanitizer limitations.

Public diagnostic metadata is validated before storage: component type/qualifier values are at
most 64 ASCII identifier characters; port names are at most 64 non-control characters; contract,
interaction, protocol, schema, checkpoint, disruption, diagnostic-source, and driver-resource
identifiers are at most 128 characters under their documented character sets. Diagnostic-source
names are exported only as stable 16-hex-character digest identities.

## Declarative component model

Concrete component classes declare their stable `ComponentType` and driver with
`@SystemComponent`. Core derives the component configuration and operations types from the direct
`AbstractComponent<C, O>` superclass, then derives `D` from `C extends ComponentConfig<D>`. A single
metadata boundary resolves these contracts through the driver hierarchy and validates all four
types, the target declared by `ComponentBoundDriver<C, O, T>` when present, the component
no-argument constructor, and the unique driver constructor accepting `D`. Testcontainers drivers
implement this explicit component-bound SPI; unrelated generic base-driver parameters have no
component-target meaning.

`new EnvironmentBuilder()` binds from a snapshot of system properties and environment variables.
`new EnvironmentBuilder(EnvironmentConfiguration)` accepts an explicit snapshot. Both expose
`component(ComponentClass.class)` and `component("qualifier", ComponentClass.class)`. Materialization
binds `C` and `D`, constructs the driver and component, initializes annotated ports, and only then
adds the exact returned component instance to the builder. `Environment` contains no component
factory, mutable declaration collections, or construction DSL. The builder delegates topology
construction to `EnvironmentTopology.of(...)` and validates logging before creating the runtime
facade.

Construction ends at `build(...)`: it passes immutable `EnvironmentTopology` and
`EnvironmentLogging` results to the selected facade constructor. Runtime execution never retains
the builder, mutable declaration lists, configuration binder, component materializer, or validator.
`EnvironmentTopology` is one concrete immutable snapshot, not an interface paired with a
construction-only implementation. Its static `of(...)` factory is the single owner of full
structural topology validation. It validates complete component initialization, atomically freezes
port declarations, and retains immutable component and connection snapshots before runtime
assembly. `EnvironmentBuilder` is the normal entry point and delegates to this boundary. The
driver-bearing runtime component view is package-private; public inspection returns
`List<Component>`.
`EnvironmentCreator<E>` is a separate functional interface so facade creation remains an explicit,
documented extension point rather than a nested builder implementation detail.

The lower-level `EnvironmentBuilder.component(...)` overloads accept an already materialized
configuration and `ComponentDriver<C, O>`. The explicit-`ComponentType` overload supports isolated
tests and programmatically built configurations without adding factory methods or constructor DSLs
to concrete component classes. Programmatically constructed component fixtures use `ComponentPortFactory`;
the component model itself exposes no port factory methods.

`ComponentFactory`, `ConnectionFactory`, reflection-backed `ComponentMetadata`, `ComponentInitializer`,
`PortDeclarations`, and `TopologyValidator` are package-private construction implementation details.
They are not retained by `Environment`, `EnvironmentTopology`, or `EnvironmentRuntime`.

`Connection<C>` is the immutable logical declaration. Its typed `ConnectionId` is derived
deterministically from both component and local port identities. Each canonical endpoint uses
`component-type[qualifier].local-port`, with empty brackets representing an absent qualifier.
Component type and qualifier are separate semantic fields; construction deliberately does not use
the flattened `ComponentId.toString()` or `ComponentId.value()` display form. Port names use
delimiter-safe percent encoding. The ID does not depend on contract identity alone, endpoint
values, mapped Docker ports, startup order, object identity, or hash codes. Several required ports
may target one provided port while retaining distinct IDs. `ConnectionDescriptor` derives or
validates the same canonical ID against its structured endpoint metadata.

`EnvironmentRuntime` creates one ordered runtime-connection registry from the validated topology.
The registry materializes each declaration exactly once, rejects duplicate IDs or required-port
materialization, and indexes by ID, required port, provider, and provided port. A
`RuntimeConnection<C>` owns its immutable descriptor, `DECLARED -> STARTING -> RUNNING -> STOPPING
-> STOPPED` lifecycle or terminal `FAILED` state, routing mode, observation requirement, effective
observation status, one-shot direct `EndpointBinding<C>`, and a separate effective consumer
binding. State transitions are centrally checked. A connection cannot become `RUNNING` until both
targets are available.

Drivers still publish both internal and external endpoint values as the direct binding.
`DriverContext.resolve(...)` reaches the required port's runtime connection and returns only the
internal value of its consumer binding. `ComponentRuntime` has no public binding or provided-port
resolution method. It only transfers its published bindings into a non-publicly-constructible,
environment-owned typed boundary used by `RuntimeConnectionRegistry`. The external direct form is
retained for JVM gateway routing. Public inspection returns detached immutable snapshots
containing semantic metadata, state, mode, observation requirement, effective observation status,
and separate direct/consumer availability; it never returns endpoint values, route implementations,
closeable resources, Testcontainers objects, mapped ports, aliases, or credentials.

`DIRECT` aliases the consumer target to the direct binding and allocates no route resource.
`ROUTED` selects a typed `ConnectionRouteProvider<C>` using immutable rules keyed by semantic
`Contract<C>` or one stable structured connection identity. A policy can contain several rules,
connection-specific rules take precedence, and unmatched connections remain `DIRECT`. This keeps
distinct contracts using the same Java class separate. The provider is invoked once per
`RuntimeConnection`, receives one immutable context containing its stable descriptor, typed direct
binding, `ObservationRequirement`, exact connection-scoped observation capability, optional
scenario-owned `RequiredObservationProfile`, and the environment-scoped
`InteractionDecisionCoordinator`, then returns a typed consumer binding plus an
optional connection-owned resource. The context exposes no journal, mutable runtime state,
topology mutation, socket, or container details. The sole unchecked conversion is confined to the
private contract-and-connection-validated routing boundary.

Provider fan-out preparation is atomic: the runtime prepares every route before publishing any
targeted connection as `RUNNING`. A later preparation failure closes prior routes in reverse order,
keeps the startup failure primary, and suppresses cleanup failures. Cleanup first removes consumer
availability for the full provider set, closes route resources in reverse order exactly once, then
invalidates direct targets before closing the provider. Route cleanup failure makes the affected
connection terminally `FAILED` without preventing remaining provider cleanup.

Route preparation and cleanup exceptions remain unchanged for the caller, including suppressed
failure ordering. Before those failures enter the environment journal, only type-only
`FailureDetails` is retained; route stage and connection identity remain structured event fields.
Connection, component, and environment rendering therefore share the same safe representation
without creating a second history.

`ROUTED` is not `OBSERVED`: access to a connection-bound capability records nothing by itself.
`ConnectionRouting` keeps `RoutingMode` at `DIRECT | ROUTED` and attaches the separate
`ObservationRequirement.DISABLED | OPTIONAL | REQUIRED` to a route rule. A route must report a
compatible `EffectiveObservationStatus`; required observation cannot bind a transparent route.
The required-profile routing overloads bind protocol-neutral evidence and native-reference schema
IDs, capabilities, and required transport features to the exact selected
`ConnectionId`. A provider consumes that profile during route preparation; core contains no
adapter-specific reference type.
Observation is `PENDING` before route preparation and `INACTIVE` after clean shutdown of a formerly
active route. Snapshots expose this state without exposing transport internals. The environment
constructs one thread-safe coordinator shared by all route contexts; its current serialized
decision is `FORWARD`. `ConnectionRouting` enters through the protected runtime construction seam
rather than the public topology DSL. Protocol framing, buffers, and sockets remain in the
Testcontainers adapter module.

`Environment.start()` starts providers before consumers when a consumer needs the provider's
runtime binding to materialize its driver. It attaches each runtime to the same component object.
`Environment.close()` closes component resources in reverse order and then closes shared driver
resources. A partial startup failure keeps the original failure primary and adds cleanup failures
as suppressed exceptions. Operations outside `RUNNING` fail with component identity, type, actual
state, and expected state.

Runtime-connection lifecycle and materialization failures are immutable typed `ScenarioEvent`
values. They retain the connection descriptor and frozen failure details without rendering
endpoint values. Logging thresholds affect only SLF4J emission; every connection event remains in
the journal. Topology failures occur before a runtime journal exists and therefore remain immediate
construction exceptions.

`journalSequence` is local storage/rendering order only. Diagnostic elapsed time and rendered log
order are not causal evidence. Checkpoint/barrier records likewise do not establish barrier
evaluation, cross-stream ordering, or happens-before relationships.
Connection lifecycle order, direct-target binding, and elapsed time likewise do not prove external
protocol ordering or causality.

Framework lifecycle, failure, and diagnostic events are created only by the runtime. Interaction,
checkpoint/barrier, and disruption contributions use closed immutable envelopes owned by core.
External modules define a typed `EvidenceCodec<T>` for their observation value. A route provider
opens an `InteractionSession` from the `ConnectionObservations` capability bound to its exact
runtime connection. The session accepts only flow direction, codec, and evidence; it allocates the
connection-bound `SessionId`, direction-local ordinal, and complete `InteractionRef`.

Every physical session receives a new connection-local session value. Ordinals begin at one and
increase independently for `CONSUMER_TO_PROVIDER` and `PROVIDER_TO_CONSUMER` within each session.
Identity allocation and submission to the journal are serialized per session direction.
The package-private environment journal separately serializes sequence allocation and insertion
through one synchronization boundary. The resulting global journal sequence remains
storage/rendering order only, not causal
order. Values from different connections, sessions, or directions are not comparable evidence of
ordering or causality. Explicit causal relations are outside this layer.

`Environment.proofSubjects()` exposes one narrow facade over the environment-owned correlation
state. `create()` allocates an opaque reference whose owner token and local value have no public
constructor or accessor. `arm(...)` accepts only a `CorrelationKey`: a namespaced/versioned schema
plus 16-64 bytes of domain-produced digest material copied on input and never returned or rendered.
Domains normalize and digest their source values before core sees the key. Core therefore contains
no protocol fields, raw selector strings, maps, unchecked casts, phone numbers, message content,
tokens, SQL parameters, or credentials.

An adapter or domain captures its immutable native reference as a
`CorrelationContribution<T>` through its own `EvidenceCodec<T>`. Capture retains only a detached
`EvidenceSnapshot`. After `InteractionSession.observe(...)` returns, the same session validates
that the reference belongs to a previously recorded interaction and publishes each contribution.
A typed facade lookup validates the requested schema before decoding a fresh copy. Native HTTP,
SMPP, or PostgreSQL reference types and schemas remain defined by their adapter modules; core never
flattens or interprets them.

Current correlation state is linearized by one environment-owned synchronization boundary:

- no distinct candidate is `MISSING`;
- exactly one distinct candidate is `UNIQUE`;
- a second distinct candidate or a key shared by subjects is terminal `AMBIGUOUS`;
- an exact duplicate is idempotent only while the same subject, key, `InteractionRef`, native
  schema, and encoded native reference match;
- retries and reconnects have distinct interaction/session identity and therefore cannot silently
  rebind a unique result;
- unmatched candidates are journaled as unassigned and are never retroactively selected after
  later arming;
- completion, rollback, and teardown do not erase or reclassify recorded facts; teardown rejects
  new creation, arming, and publication while preserving typed lookup.

Only `CorrelationResult.Unique<T>` exposes the recorded `InteractionRef` and decoded native
reference. Missing and ambiguous result types expose no candidate. No path selects first, last,
latest, earliest, next, or arrival-order candidates.

Native-flow semantic composition additionally preserves the originating `InteractionRef`, its
exact `SessionId` and `ConnectionId`, and its immutable snapshot. A held candidate may join that
contribution only on the same logical connection and physical gateway session; the two directions
may differ. Equal snapshot bytes on another connection or session do not establish identity.

A reached native-flow hold stores the exact resolution that caused the match. `release()` checks it
again under the correlation registry synchronization boundary immediately before changing the hold
to `RELEASING`; that check is the release linearization point. If the resolution is no longer sole
and unique, the hold fails with `CORRELATION_INVALIDATED`, requests `CLOSE_SESSION`, forwards no
held byte, and completes release exceptionally. A publication after that point cannot revoke an
already authorized release.

Semantic predecessor guards use that same typed selector and the same coordinator synchronization
boundary as holds. A guard is armed before stimulus with an exact subject, predecessor selector,
`CONFIRMED` or `FORWARDED` boundary, successor selector, and positive maximum duration. `CONFIRMED`
is established only by a matching complete confirmation interaction. `FORWARDED` is established
only by the exact permit's callback after successful write and flush.

The coordinator appends the decision fact before returning it. A predecessor-first order advances
through `PREDECESSOR_SATISFIED` and `SUCCESSOR_AUTHORIZED`; successful successor forwarding records
the exact relation and `SATISFIED`. A successor-first order records `VIOLATED` and returns
`CLOSE_SESSION` without waiting or forwarding bytes. Timeout and failure states remain enforceable
tombstones. Cancellation, route failure, REQUIRED observation failure, write outcome, and teardown
share the same total order. A later cleanup failure cannot replace a violation and is retained only
as a safe typed suppressed diagnostic. Missing or ambiguous native correlation does not select a
guard, and exact subject/session validation isolates concurrent subjects and reconnects.

The selector exposes typed codecs and matching but not `EvidenceSnapshot`; snapshot decoding,
registry state, coordinator locks, and gateway permits remain internal. Guard journal events retain
only safe identities, states, boundaries, decisions, relations, violations, and failure enums. See
[`ADR 0009`](../docs/adr/0009-semantic-predecessor-guards.md).

Core encodes and copies evidence before append; the caller-owned value, codec, and returned array
are not retained. Decoding is typed, schema-checked, and receives another copy, so snapshot access
cannot mutate storage. The core renderer handles each envelope explicitly and renders only
connection, session, flow, ordinal, interaction reference, schema identity, and encoded size; it
never renders the payload or calls an arbitrary payload `toString()`.

Component and connection contributions are deliberately separate. `DriverContext` does not expose
mutable journal storage; its `JournalContributions` sink contains only component-owned checkpoint and
disruption operations and cannot publish traffic. A component-scoped context resolves only
required ports owned by that component. Route providers receive neither mutable journal storage nor
runtime connection mutators. All observations still append to the same environment-owned history;
proof-subject creation, arming, and non-idempotent correlation publications use additional
core-owned immutable envelopes in that same history. The runtime keeps only a
thread-safe current-cardinality index, not a second event history. Journal sequence, diagnostic
time, wall-clock time, rendered order, sleeps, and unrelated stream ordinals never infer
correlation or causality.

The environment execution owns one package-private mutable `ScenarioJournal`. Its append method is
package-private and only `EnvironmentEventPublisher` receives it. The publisher constructs narrow
framework facts, validates contribution scope, freezes type-only failure metadata, and
appends exactly once at the existing pipeline point. `JournalSlf4jEmitter` consumes the returned
immutable stored entry only after append, owns logging thresholds, and treats `OFF` as no emission
rather than no history. Neither collaborator owns a second event list.

`ScenarioEvent` is a public open inspection contract; public framework record constructors and
client implementations create detached values but cannot append them to a runtime. New framework
facts therefore do not invalidate exhaustive switches over a sealed root because there is no
sealed root. `Environment.journalSnapshot()` is the supported authoritative read path.
`JournalRenderer` consumes only detached snapshots, handles every framework event explicitly and
unknown events through a payload-free type fallback, supports full and structured component
filtering, repeats the same prefix across multiline messages, and appends into one `StringBuilder`
so construction is linear in total output size.

The module contains no JUnit, Testcontainers, Docker image, or wait strategy dependency.
