# System Proof

System Proof is a typed Java framework for defining system-test topologies, starting their runtime
components, injecting a concrete environment into JUnit 5 tests, and retaining failure diagnostics.

## Modules

```text
system-proof-examples -> system-proof-junit5        -> system-proof-core
        |------------> system-proof-postgresql      -> system-proof-testcontainers -> system-proof-core
        |------------> system-proof-http            -> system-proof-testcontainers -> system-proof-core
        |------------> system-proof-smpp            -> system-proof-testcontainers -> system-proof-core
        |------------> system-proof-testcontainers  -> system-proof-core
        `-------------------------------------------> system-proof-core

system-proof-examples/apps
        `-> system-proof-ingestion-service

system-proof-examples/fixtures
        `-> ukarim-smscsim
```

- `system-proof-core`: typed components, ports, connections, lifecycle, logging, and diagnostics.
- `system-proof-junit5`: `@SystemProof`, `@EnvironmentDefinition`, environment injection, and
  failure artifacts.
- `system-proof-testcontainers`: container-backed drivers, runtime port bindings, and test-JVM
  interaction gateway routes.
- `system-proof-postgresql`: bounded plaintext PostgreSQL v3 observation, exact explicit-commit
  control, typed transaction evidence, write correlation, and independent durability preflight.
- `system-proof-http`: bounded plaintext HTTP/1.1 callback observation, tri-state Jasmin
  acknowledgement evidence, request correlation, and response control.
- `system-proof-smpp`: bounded plaintext SMPP 3.4 session observation, typed delivery
  acknowledgement evidence, delivery correlation, and response control.
- `system-proof-examples`: executable PostgreSQL and complete SMS-ingestion examples, including
  real PostgreSQL, HTTP, and SMPP evidence flows.
- `system-proof-examples/apps`: the reference ingestion SUT used only by the complete example.
- `system-proof-examples/fixtures`: reproducible third-party fixture adaptations used by examples.

Core is independent of JUnit and Testcontainers. The JUnit 5 module is independent of
Testcontainers. These boundaries are enforced by `CoreModuleBoundaryTest` and
`Junit5ModuleBoundaryTest` and recorded with the T1 baseline in
[`docs/adr/0001-t1-proof-contract.md`](docs/adr/0001-t1-proof-contract.md).

The PostgreSQL adapter is intentionally not a general proxy. Its characterized pgJDBC subset,
commit-success definition, plaintext/TLS boundary, memory limits, and durability preflight are in the
[`system-proof-postgresql` module](system-proof-postgresql/README.md) and
[`ADR 0005`](docs/adr/0005-postgresql-wire-evidence.md). The reference example composes that evidence
with HTTP and SMPP through one canonical SMS fingerprint and attributes one proof subject to one
transaction as recorded in [`ADR 0008`](docs/adr/0008-aml-subject-transaction-attribution.md).
Environment-owned semantic predecessor guards then enforce the two exact cross-connection
relations recorded in [`ADR 0009`](docs/adr/0009-semantic-predecessor-guards.md). They enforce
ordering. Frozen proof plans can now require those typed relations together with exact observation,
correlation, control, evidence, and prerequisite coverage and evaluate one controlled execution to
`PROVED`, `VIOLATED`, `INCONCLUSIVE`, or `ERROR`. The lifecycle, fail-closed precedence, and report
boundary are recorded in [`ADR 0011`](docs/adr/0011-frozen-proof-plan-evaluation.md). This is the
evaluation seam required by the later final T1 scenario; it does not itself claim that scenario.

The HTTP adapter is likewise a characterized, fail-closed subset rather than a general HTTP
proxy. Its framing limits, tri-state `ACK/Jasmin` acknowledgement contract, local exchange
identity, correlation boundary, and unsupported cases are in the
[`system-proof-http` module](system-proof-http/README.md) and
[`ADR 0006`](docs/adr/0006-http-callback-evidence.md). This evidence is one input to a later
cross-connection proof; it is not the final T1 proof.

The SMPP adapter is a characterized fail-closed subset of the pinned SMSCsim/Jasmin exchange. Its
PDU and session limits, request/response association, correlation boundary, and unsupported cases
are in the [`system-proof-smpp` module](system-proof-smpp/README.md) and
[`ADR 0007`](docs/adr/0007-smpp-evidence.md). This evidence is one input to a later
cross-connection proof; it is not the final T1 proof.

## Minimal test

```java
import io.github.jacekkardys.systemproof.junit.annotation.SystemProof;

final class ExampleIT {

    @SystemProof(
        value = ExampleEnvironment.class,
        title = "Stores and reads one value",
        description = "Exercises the database through the running example environment"
    )
    void exercisesBehavior(ExampleEnvironment environment) {
        environment.database().insert("value");

        assertThat(environment.database().values()).containsExactly("value");
    }
}
```

The method-level `@SystemProof` declaration is the JUnit test and points to a concrete environment
facade, so no separate `@Test` annotation is required. That facade declares exactly one static,
zero-argument `@EnvironmentDefinition` method returning its own type. System Proof validates the
closed topology before startup, starts dependencies in order, injects the exact returned object,
supports injection into the test, `@BeforeEach`, and `@AfterEach` method parameters, and closes
partial or complete startup in reverse order. An optional `title` becomes the test display name;
`title` and `description` are also published as JUnit report entries.

## Frozen proof evaluation

A scenario first creates its subject and declares any semantic controls. It then freezes the
complete plan, activates it, runs stimulus only after successful activation, and evaluates once:

```java
ProofPlan plan = ProofPlan.builder(
    "commit-before-callback",
    "Commit succeeds before the positive callback",
    subject,
    Duration.ofSeconds(30)
).prerequisite(
    "database-durability",
    environment.proofs().satisfiedPrerequisite()
).observation(
    "callback-observation",
    callbackConnection,
    callbackProfile
).correlation(
    "callback-correlation",
    callbackConnection,
    correlationKey,
    callbackNativeSchema
).control(
    "commit-before-callback-guard",
    guard,
    SemanticPredecessorGuardState.SATISFIED
).causalRelation(
    "commit-before-callback-relation",
    guard
).build();

ProofExecution execution = environment.proofs().activate(plan);
execution.runStimulus(() -> invokeSystemUnderTest());
ProofResult result = execution.evaluate().require(ProofOutcome.PROVED);
```

This is an explicit pre-1.0 API, not a final proof DSL. `PROVED` requires one successfully
completed stimulus and every declared item to be `SATISFIED`; evaluating before or during the
stimulus is rejected. Silence, timeout, missing or ambiguous correlation, unsupported coverage,
and an unreached control remain inconclusive. Framework trust failures are errors. An authoritative
early-successor guard violation remains a violation even if cleanup later fails. The compact result
report is detached, bounded, deterministic, and type-only for failures. Its resolutions retain safe
typed requirement descriptors and role-aware detached interaction provenance (`CORRELATION`,
`HOLD`, `PREDECESSOR`, or `SUCCESSOR`). Public semantic-control roots are delivered independently
on bounded daemon tasks after mandatory framework actions; arbitrary dependent execution order is
not a contract. Evidence belongs to an execution only when its observation-
allocated interaction identity is after the atomically captured activation watermark; delayed
callback delivery cannot move older traffic into the proof window, and post-watermark recording
cannot publish into a separate pre-`ACTIVE` gap. The proof deadline also bounds
required-provider refresh and is reported as a typed evaluation gap, never as successful proof.
The full journal and troubleshooting diagnostics remain separate.

## Component declarations

A component is a concrete class. `@SystemComponent` owns its static kind and driver metadata, while
the `AbstractComponent<C, O>` type arguments declare its component configuration and optional
runtime operations:

```java
@SystemComponent(
    type = "system-proof-smsc-simulator",
    driver = SmscTestcontainersDriver.class
)
@Getter
@Accessors(fluent = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SmscComponent
    extends AbstractComponent<SmscConfig, UkarimSmscOperations> {

    @PortContract("smpp")
    @Communication.Smpp
    private ProvidedPort<SmppEndpoint> smpp;
}
```

Component configuration appears once and contains service-facing runtime values. Its type parameter
associates a separate driver-only configuration:

```java
public interface SmscConfig extends ComponentConfig<SmscConfig.Driver> {
    @ConfigurationSource(
        provider = EnvironmentVariable.class,
        key = "SYSTEM_PROOF_EXAMPLE_SMSC_SYSTEM_ID",
        defaultValue = "sp-test"
    )
    String systemId();

    @ConfigurationSource(
        provider = EnvironmentVariable.class,
        key = "SYSTEM_PROOF_EXAMPLE_SMSC_PASSWORD",
        defaultValue = "password"
    )
    Secret<String> password();

    interface Driver extends DriverConfig {
        @ConfigurationSource(
            provider = EnvironmentVariable.class,
            key = "SYSTEM_PROOF_SMSC_SIMULATOR_IMAGE",
            defaultValue = "system-proof-ukarim-smscsim:local"
        )
        String image();

        @ConfigurationSource(provider = Literal.class, value = "2775")
        int smppPort();
    }
}
```

Environment assembly, its immutable topology result, and typed facade creation are exposed from
`io.github.jacekkardys.systemproof.environment`; the runtime facade and topology live in
`environment`, while external configuration snapshots live in `configuration`.

Every configuration method retains its `@ConfigurationSource` and validation annotations. The
environment builder binds both interfaces from one immutable `EnvironmentConfiguration`, constructs
the declared driver, creates the component, initializes annotated ports, and registers the component
only after all steps succeed:

```java
EnvironmentBuilder builder = new EnvironmentBuilder();
SmscComponent smsc = builder.component(SmscComponent.class);
JasminComponent jasmin = builder.component(JasminComponent.class);

EnvironmentBuilder isolated = new EnvironmentBuilder(configuration);
SmscComponent primary = isolated.component("primary", SmscComponent.class);
SmscComponent secondary = isolated.component("secondary", SmscComponent.class);
```

The builder returns the exact instance retained by the topology and later runtime. Component classes
do not know `ComponentFactory` or `EnvironmentBuilder`. Core derives and validates the component
configuration, operations, driver configuration, and driver through its generic hierarchy before
binding values. Runtime technologies that bind to one concrete component declare that target
through `ComponentBoundDriver<C, O, T>`; Testcontainers uses this explicit SPI. Tests and
programmatic configuration can pass an already prepared configuration and driver to a typed
`EnvironmentBuilder.component(...)` overload without adding factory methods to component classes.

`EnvironmentBuilder.build()` creates a plain `Environment`. A typed facade is created by supplying
its constructor after the builder has validated and frozen the topology and logging configuration:

```java
return builder.build((topology, logging) ->
    new ExampleEnvironment(topology, logging, smsc, jasmin)
);
```

The facade constructor accepts only the concrete immutable model `EnvironmentTopology` and
`EnvironmentLogging`. The topology constructor snapshots already validated model values; normal
application code should let `EnvironmentBuilder` perform validation and create it.
`Environment` and its runtime collaborators do not depend on the mutable construction DSL.
`EnvironmentCreator<E>` is the callback passed to `build(...)` when the caller needs a typed
`Environment` subclass. It runs once after topology and logging validation and should only invoke
that facade's constructor.

## Core package boundaries

The canonical package map, supported API/SPI whitelists, read-model definition, Java-public
technical exceptions, mutable-state ownership, sealed-hierarchy policy, and pre-1.0 compatibility
policy are in [Package and API architecture](docs/architecture/package-api-architecture.md).

Core packages are domain-owned. Mutable assembly and execution remain package-private in
`environment`; detached lifecycle/connection inspection lives in `environment.state`; `journal`
contains immutable vocabulary/read models; and `diagnostics` renders those models without owning a
second history. Route selection, provider endpoint lookup, proof-subject allocation, journal append,
redaction, logging emission, and cleanup are not public API.

## Runtime model

Each `AbstractComponent<C, O>` owns:

- a `ComponentType` and instance `ComponentId`;
- one immutable typed component configuration `C`;
- typed required and provided ports;
- one `ComponentDriver<C, O>`;
- an attached `ComponentRuntime<O>` while running.

Each logical `Connection<C>` is an immutable directional
`RequiredPort<C> -> ProvidedPort<C>` declaration. It validates contract, interaction, and protocol
compatibility and owns a deterministic `ConnectionId` derived from both component and local port
identities. Its canonical endpoint form is `component-type[qualifier].local-port`; empty brackets
mean that the component has no qualifier. Component type and qualifier are encoded as separate
semantic fields, so identity never depends on the flattened display form from
`ComponentId.toString()`. Port-name delimiters are percent-encoded, so distinct required ports
connected to one provided port remain distinct without using endpoint values, startup order,
object identity, hash codes, or mapped ports. For example, `client-a[].api` and
`client[a].api` are distinct endpoints even though both component IDs display as `client-a`.

Each environment runtime materializes every logical declaration exactly once as a typed
`RuntimeConnection<C>`. One environment-owned registry preserves topology declaration order,
indexes runtime connections by `ConnectionId`, required port, provider, and provided port, and owns
their lifecycle, routing mode, observation requirement, effective observation status, direct
provider target, and effective consumer target. A provider still publishes one typed
`EndpointBinding<C>`. The runtime retains it as `directTarget`, prepares the connection's
`consumerTarget`, and commits both only after every connection targeting that provider is ready.
Consumer resolution follows the required port to its `RuntimeConnection` and returns only the
consumer target's internal typed endpoint; it never resolves independently through the provider
runtime. `ComponentRuntime` exposes no public provider-binding lookup; it can only transfer
published bindings into an environment-owned boundary that external callers cannot construct.

The complete `EndpointBinding<C>` retains both the internal endpoint used for component-to-component
communication and the external test-host endpoint used by JVM gateway routing. Endpoint values
remain internal because they may contain credentials, aliases, or other secrets.
`Environment.runtimeConnections()` and `Environment.runtimeConnection(ConnectionId)` expose only
detached immutable snapshots with semantic metadata, lifecycle state, routing mode,
`ObservationRequirement`, `EffectiveObservationStatus`, and separate direct/consumer target
availability.

An `ObservationStatusProvider` is sampled after its route is committed but before a dependent
consumer starts. Dynamic status uses one environment-owned single-flight refresh. Its owner captures
stable route handles, releases the `EnvironmentRuntime`, connection and capability registries,
runtime connections, and semantic-control coordinator monitors, invokes each provider, then
atomically validates and publishes the cached statuses. A concurrent diagnostics or
runtime-connection read does not start another provider; it returns the last complete cached
snapshot. Cleanup never calls the provider, and a late result for a stopped or replaced route is
discarded. `FAILED` and `DEGRADED` cache states are terminal and cannot be reactivated by a later
`ACTIVE` sample.

`DIRECT` makes the consumer target the direct provider binding and creates no routing resource.
`ROUTED` invokes a typed `ConnectionRouteProvider<C>` independently for every matching connection.
An immutable routing policy can contain multiple rules keyed by semantic `Contract<C>` or a stable
structured `ConnectionId`; connection-specific rules take precedence and unmatched connections stay
direct. The provider receives one immutable `ConnectionRouteContext<C>` containing that
connection's stable descriptor, direct binding, observation requirement, connection-bound
observation capability, optional scenario-owned `RequiredObservationProfile`, and the one
environment-scoped decision coordinator. It then returns the
effective binding, effective observation-status provider, and an optional connection-owned
resource. All routes for one provider are prepared before any targeted connection becomes
`RUNNING`. Partial creation closes already prepared resources in reverse order and retains cleanup
failures as suppressed. Normal cleanup first makes consumer targets unavailable, then closes routes
in reverse order, and invalidates direct targets before provider cleanup completes. Route failure
diagnostics retain only failure type, lifecycle stage, and connection identity; the original
throwable and suppressed ordering returned to the caller remain unchanged.

`ROUTED` means only that the consumer receives an interposed endpoint; it does not claim that
traffic was observed. Observation is configured independently as `DISABLED`, `OPTIONAL`, or
`REQUIRED`. Disabled routes use the simple transparent relay. Optional routes either establish
`ACTIVE` protocol observation or report `UNSUPPORTED`; a later observation failure is
`DEGRADED`. Required routes must start `ACTIVE` and fail closed to `FAILED` rather than silently
relaying undecided bytes. Requested observation is `PENDING` before route preparation, and a
cleanly stopped formerly active route is `INACTIVE`.

A required observation profile belongs to the scenario/routing rule, not the adapter. It declares
protocol-neutral evidence and native-reference schema IDs, capabilities, and required features for
one selected `ConnectionId`. Route preparation compares it with the adapter-provided
profile before a listener or protocol session is opened. Different connections using one provider
may require different profiles.

The protected environment runtime seam accepts `ConnectionRouting` without adding route or proxy
declarations to the topology DSL. The Testcontainers `InteractionGateway` adds protocol framing
through a neutral adapter SPI. For every physical socket pair it opens exactly one
`InteractionSession`, two independent directional protocol streams, and two bounded byte buffers.
Each complete forwarding unit follows `frame -> record -> correlate -> permit -> forward`: typed
evidence is copied into the scenario journal, immutable adapter-produced correlation contributions
are published for the returned stable `InteractionRef`, and the captured interaction reaches the
shared coordinator only after its correlation result is visible. A forwarding permit completes the
decision handshake without moving original bytes, sockets, streams, or mutable buffers into core.
Only an authorized `FORWARD` writes and flushes the adapter-preserved original bytes, exactly once;
the gateway then reports success, write failure, or session abandonment. Units without correlation
contributions retain the same path. No unit prefix is sent before that boundary. Complete coalesced
units remain ordered; incomplete units remain buffered within explicit frame and aggregate limits.

## Semantic hold and release

A semantic hold is a deterministic decision at that observe-before-forward boundary. It is not a
sleep, a transport outage, or a pause after forwarding. Arm the one-shot control before producing
the stimulus, wait for its `reached` signal, make assertions while zero bytes of the selected unit
have been sent downstream, and explicitly release it:

```java
SemanticHold hold = environment.controls().arm(
    SemanticInteractionSelector.matching(
        connectionId,
        FlowDirection.CONSUMER_TO_PROVIDER,
        evidenceCodec,
        evidence -> evidence.matchesExpectedValue()
    ).forSubject(subject).through(
        correlationKey,
        nativeReferenceCodec,
        evidence -> evidence.nativeFlowReference()
    ), // optional native-flow constraint
    Duration.ofSeconds(10)
);

produceStimulus();
InteractionRef interaction = hold.reached().toCompletableFuture().join();
assertDownstreamHasReceivedNoSelectedBytes(interaction);
hold.release().toCompletableFuture().join();
```

The selector matches an exact `ConnectionId`, exact `FlowDirection`, evidence schema, typed value,
and optionally one uniquely correlated `ProofSubjectRef`. `through(...)` allows the hold to be
armed before traffic and joins later evidence to the subject's sole unique native reference only
when the contribution and held candidate belong to the same logical connection and exact physical
gateway session. Their directions may differ; equal native-reference bytes on another connection
or session never join. The two facts need not share one `InteractionRef`. Its codecs, predicate,
and extractor run synchronously and therefore must be pure, fast, non-blocking, and side-effect
free. A selector exception or overlapping matching holds fails closed; missing or ambiguous
subject correlation does not match. A selector candidate becomes held evidence only after the
coordinator reaches `REACHED_HELD`. Selector failure or overlap between active holds before that
state retains no `HOLD` provenance, leaves `HELD_INTERACTION` missing, completes `reached()`
exceptionally, and closes the affected session.

## Semantic predecessor guards

A predecessor guard is an invariant, not a manual hold. Arm it before stimulus with one exact
subject, predecessor selector and boundary, successor selector, and maximum duration:

```java
SemanticPredecessorGuard guard = environment.controls().guard(
    SemanticPredecessorGuardSpec.requiring(
        subject,
        SemanticPredecessorRequirement.confirmed(commitSucceededSelector),
        positiveHttpResponseSelector,
        Duration.ofSeconds(10)
    )
);
```

`CONFIRMED` is established by matching protocol-confirmation evidence such as
`CommitSucceeded`. `FORWARDED` is established only after the gateway has written and flushed the
exact predecessor bytes and called its permit's `forwarded()` callback. A `CommitAttempt`, observed
interaction, `FORWARD` decision, write start, failed write, abandonment, timestamp, or journal
position establishes neither boundary.

The shared coordinator is the one linearization point. Predecessor first authorizes the successor;
after successful successor forwarding the guard records an exact satisfied relation. Successor
first records a terminal violation, returns `CLOSE_SESSION`, and forwards zero successor bytes. It
does not wait for or accept a later predecessor. Matching is evaluated against the guard state at
the start of the current interaction: if an `ARMED` guard's predecessor and successor selectors
both match that one interaction, it is an early successor with successor-only provenance, never a
self-causal relation. Missing or ambiguous subject/native-reference
correlation does not match, and other subjects remain independent. The complete state, lifecycle,
journal, and safety contract is in [`ADR 0009`](docs/adr/0009-semantic-predecessor-guards.md).

Arming is accepted only for an exact connection owned by this environment whose routing rule
declares semantic-control capability: `ROUTED`, `REQUIRED` observation, a complete-unit protocol
adapter, and the forwarding-permit handshake. The protocol-aware `InteractionGateway.tcp(...)`
overloads declare that capability. The selector's evidence schema and optional native-flow schema
must also equal the active required profile for that exact connection. Direct, disabled, optional,
transparent/unsupported, profile-less, and legacy custom providers are rejected before a hold is
created. Pre-start arming validates the declaration; startup then samples exactly once and requires
the route to materialize the capability with `ACTIVE` observation before a dependent component
starts. A retained `SemanticControls` facade performs a new single-flight refresh immediately before
every running-environment `arm(...)` or `guard(...)`. If another refresh is already in progress, the
control operation fails closed instead of trusting an older `ACTIVE` cache. Refresh and control
registration are committed in that order under the runtime boundary, but that boundary alone does
not serialize asynchronous gateway failure. `SemanticControlCoordinator.observationFailed(...)`
first retains a terminal marker for the exact `ConnectionId` under the coordinator monitor, then
fails existing guards. `arm(...)` and `guard(...)` check the same marker under that same monitor
immediately before registration. If failure linearizes first, no stale `ACTIVE` refresh can create a
later control; if registration linearizes first, failure observes that control and applies its
existing terminal semantics. The marker never clears and does not affect another connection.

The lifecycle is `ARMED -> REACHED_HELD -> RELEASING -> FORWARDED`, with `CANCELLED`, `TIMED_OUT`,
and `FAILED` terminal alternatives. The hold duration begins at `REACHED_HELD`. Cancel, timeout, or
environment teardown closes only the affected physical session without implicitly forwarding the
retained unit or degrading an otherwise healthy route. `release()` completes only after the gateway
reports the result of its single write/flush attempt. Checked and unchecked output failures are
reported exactly once, never retried, and never produce a false `FORWARDED`, but the framework cannot
prove that the remote endpoint received no partial bytes.

For a reached `through(...)` hold, `release()` revalidates the exact subject, key, originating
`InteractionRef`, gateway session, connection, and native-reference snapshot that caused the match.
This synchronized registry check immediately before the `RELEASING` transition is the release
linearization point. If the candidate is no longer the subject's sole unique native flow, release
fails with `CORRELATION_INVALIDATED`, closes that physical session, forwards no held byte, and
completes exceptionally. A contribution published after the linearization point does not revoke an
already authorized release.

One directional pump processes units sequentially, so later same-session, same-direction units
cannot overtake a held unit. Opposite directions, other sessions, and other connections have their
own tasks and continue independently. The pump performs no further socket reads while held; later
traffic remains under TCP backpressure. The selected unit is at most `maximumFrameBytes`, and the
already-read directional buffer is at most `maximumBufferedBytes`. Including the retained
`ProtocolUnit` copy, the one write copy, and the fixed read chunk, the gateway-owned raw-byte storage
is bounded per held direction by `maximumBufferedBytes + 2 * maximumFrameBytes +
min(8192, maximumBufferedBytes)`, excluding array headers and typed adapter evidence. No unbounded
hold queue is introduced.

External values enter through immutable `EnvironmentConfiguration`. Each environment builder owns
that snapshot and binds the component and driver configuration interfaces declared by component
metadata. Secrets use `Secret<T>`, but configuration is excluded from diagnostics regardless of its
`toString()` implementation.

## Diagnostics

`Environment.diagnostics()` is secret-safe by policy. It combines typed state, one bounded
journal rendering, and at most 32 explicitly sanitized diagnostic sources. The complete result is
bounded to 256 KiB characters. The framework exposes no raw or sensitive attachment path. The
trust classifications, prohibited data, exact limits, and residual limitations are defined by
[`ADR 0010`](docs/adr/0010-secret-safe-diagnostics.md).

Capture copies one immutable lifecycle/component/connection/journal/source snapshot under the
environment monitor and invokes eligible suppliers only after releasing it. A redacted source is
bounded after `Supplier.get()` returns; its trusted driver must bound acquisition work and memory.
The Testcontainers adapter accepts only its own restricted container lifecycle. It disables the
upstream container logger, denies upstream full-log retrieval, exposes neither log consumers nor
log-based wait strategies, and uses non-log readiness probes. These enforced controls—not an empty
`getLogConsumers()` list—keep container stdout/stderr outside System Proof diagnostics.

Every environment execution owns exactly one package-private append-only `ScenarioJournal`. It is
the authoritative structured history and the only owner of sequence allocation, insertion order,
diagnostic-time capture, and snapshot copying. There is no public mutable journal or generic
`append(ScenarioEvent)` capability. The framework event vocabulary contains immutable envelopes for:

- framework environment, component, and runtime-connection lifecycle transitions, failures, and
  diagnostics;
- externally contributed interaction observations;
- proof-subject creation, safe key arming, and typed correlation candidate/cardinality facts;
- semantic hold and predecessor-guard state, decisions, relations, violations, and safe failure
  classifications;
- checkpoint or barrier records;
- disruption lifecycle records.

Protocol modules do not implement `ScenarioEvent` and do not place protocol fields in core. A route
provider obtains `ConnectionObservations` from its preparation context and opens a new
`InteractionSession` for each physical transport session. The session accepts only
`FlowDirection`, `EvidenceCodec<T>`, and typed evidence. It binds the logical `ConnectionId`,
allocates a connection-local `SessionId`, allocates a monotonic ordinal independently for each
session direction, creates the complete `InteractionRef`, and returns that reference after append.
The caller cannot supply a connection, session, ordinal, interaction reference, component, or
arbitrary event.

`ScenarioEvent` is a public open inspection contract. Client switches over it must include a
default branch, so future framework event additions do not invalidate those switches. Public record
constructors and client implementations create detached values but cannot publish them into an
environment; the environment-owned append path remains closed.

`Environment.proofSubjects()` is the only public correlation facade. It allocates an opaque
`ProofSubjectRef`, arms it with a namespaced/versioned `CorrelationKey` containing only defensively
copied digest material, and returns a typed `CorrelationResult<T>`. Results are explicitly
`MISSING`, `UNIQUE`, or terminal `AMBIGUOUS`; only `UNIQUE` exposes an `InteractionRef` and decoded
protocol-native reference. A typed lookup uses the adapter-owned `EvidenceCodec<T>` and resolves
independently in that codec's `EvidenceSchemaId` namespace. Core never interprets HTTP, SMPP,
PostgreSQL, SMS, token, or transaction fields.

Subjects belong to exactly one environment execution. Cross-environment use is rejected, new
creation/arming/publication stops at teardown, and existing results remain queryable afterward. One
distinct candidate is unique within its native-reference schema; a second interaction, retry,
reconnect, or different native snapshot in that schema is ambiguous. An exact duplicate is
idempotent only when subject, key, interaction, native schema, and encoded native reference all
match. Sharing one key across subjects makes every association ambiguous. Missing or ambiguous
cases never select the first, latest, earliest, or next journal entry.

The environment owns one thread-safe `InteractionDecisionCoordinator` shared by all route contexts.
It performs exact semantic-hold matching and state transitions under a short serialized boundary;
permit waits, public awaits, socket writes, flushes, and callbacks never hold that coordinator lock.
Traffic that matches no armed hold or predecessor guard receives an immediate forwarding permit.
Semantic holds and predecessor guards share the coordinator. The AML issue #13 investigation uses
that API to keep the direct `CommitSucceeded -> positive deliver_sm_resp` obligation separate from
the stronger HTTP-to-SMPP architectural hypothesis. Pinned stock Jasmin 0.11.0 deterministically
violates the direct obligation. The examples now also include a synchronous JVM reference relay
that reaches `PROVED` with the exact same direct plan; the held-commit state remains a separate
non-verdict characterization. See the
[`AML T1 investigation`](docs/investigations/aml-t1-jasmin-0.11.0.md).

Core invokes the codec synchronously, copies the encoded bytes into a private `EvidenceSnapshot`,
and retains neither the source value, codec, nor codec-produced array. Typed inspection uses the
same codec against a fresh byte-array copy. Mutable source arrays, collections, decoded values, and
adapters therefore cannot mutate stored history. No reflection, Java serialization, event
registry, or parallel evidence store is used.

Correlation native references use the same copy boundary. A `CorrelationContribution<T>` retains
only its safe key and detached `EvidenceSnapshot`; it never retains the source reference or codec.
The environment may keep a synchronized current-cardinality index, but every creation, arming, and
non-idempotent publication fact remains in the one environment journal, which is the only event
history.

Component-originated and connection-originated contributions are separate. The component-scoped
`JournalContributions` capability on `DriverContext` retains only checkpoints and disruption
lifecycle records; it cannot publish traffic observations. It cannot append environment/component
lifecycle or framework failure events, and it never exposes the mutable journal.
`DriverContext.log(...)` remains journal-backed and restricted to the driver-owned component, but
accepts only bounded `RedactedDiagnosticText` produced through an explicit sanitizer. There is no
arbitrary `String` overload or `safe(String)` factory. `Environment.journalSnapshot()` returns a
detached immutable snapshot for typed assertions.

Publication validates scope and identity, freezes type-only failure metadata, appends the immutable
event, and only then applies the logging threshold and emits through SLF4J. Journal events, loggers,
and renderers never retain or re-read a `Throwable`; messages, causes, suppressed messages, and
stack traces remain only in the programmatic exception chain. That chain is intentionally returned
to callers. If it escapes a test, JUnit or Surefire may independently render its messages and stack
traces in console or XML reports; those external reports are not covered by the System Proof
`environment.log` contract. Lifecycle stage and component,
connection, or resource identity remain structured event fields.

Textual environment logs are rendered views of one captured journal snapshot. They retain the
readable monotonic `T+HH:mm:ss.SSS` diagnostic timeline for framework events, connections,
components, explicitly sanitized driver text, and cleanup failures, but own no independent event
history. The restricted Testcontainers lifecycle has no container-text contribution path. Logging
thresholds control only SLF4J emission; lower-level events remain in the journal and therefore
remain available to failure diagnostics. `OFF` likewise suppresses emission, not storage.
`JournalRenderer` accepts
only detached immutable entries/snapshots, renders full or component-scoped views, repeats the same
prefix for every multiline entry, uses type-only fallback for unknown events, and bounds full
output to 256 KiB characters.

`journalSequence` is a one-based position local to one journal. It provides unique storage order
and deterministic rendering only. It is not a wall-clock or distributed sequence, an
`EvidencePosition`, or proof that one external event caused or happened before another.
Diagnostic timestamps and rendered or container log-line order likewise establish no causal
relationship. A checkpoint/barrier record and its position in the journal are also reported facts,
not proof of barrier evaluation, external ordering, or causality. An `InteractionRef` identifies
one observation through its connection-bound session, `CONSUMER_TO_PROVIDER` or
`PROVIDER_TO_CONSUMER` flow, and stream-local ordinal. Ordinals begin at one and are monotonic only
inside the same connection, session, and direction; values from different streams must not be
compared to infer causality. Explicit causal relations remain work for the later proof layer.
Runtime-connection lifecycle order and elapsed time have the same limitation: target binding
before another stored event is not proof of external protocol ordering or causality.
Correlation likewise depends only on explicit key equality, environment ownership, complete
`InteractionRef`, and typed native snapshot identity. Journal sequence, elapsed time, wall-clock
time, rendered order, and unrelated stream ordinals never resolve cardinality.

Failed JUnit tests write:

```text
target/system-proof-artifacts/<test-class>-<test-method>/environment.log
```

Set `system.proof.artifacts` to override the artifact root.

The default `environment.log` contains only `Environment.diagnostics()`. The framework does not
capture or write raw troubleshooting attachments. Sensitive or unsupported diagnostic suppliers
are never invoked by default report generation.

## Build

Java 21 and the Maven Wrapper are required:

```bash
./mvnw clean test
./mvnw clean verify
```

`clean test` runs unit tests without Docker. `clean verify` also runs the transactional ingestion
test, PostgreSQL example, container-boundary interaction gateway proof, and complete SMS-ingestion
topology through Failsafe. It requires Docker. The reference ingestion image and adapted
`ukarim/smscsim` fixture image are built during verification. The SMSC build fetches an exact
upstream commit and applies the reviewed patch stored in this repository. No prebuilt application
image or manually provisioned local tag is required.

## Continuous integration

The `Verify` workflow runs `./mvnw clean verify` with Java 21 and Docker on a GitHub-hosted Ubuntu
runner. The same job executes unit and architecture tests, the Docker integration tests including
the interaction gateway proof, builds the reference ingestion application and adapted SMSC
fixture, and runs the complete topology smoke. Third-party source, license, pin, and patch details
are recorded in
[`docs/third-party.md`](docs/third-party.md).
