# Package and API architecture

- Status: Canonical
- Scope: framework modules before 1.0
- Issue: [#41](https://github.com/JacekKardys/system-proof/issues/41)

## Decision

System Proof has no generic `model` namespace. A type's package identifies its domain owner, not
its Java shape, lifecycle phase, or visibility. A type is classified independently as supported
API, supported extension SPI, inspectable read-only model, or internal implementation.

The classification rule is:

> Put a type with the domain whose invariant gives it meaning and whose change would require that
> type to change. Keep lifecycle, resources, and mutable orchestration with that domain's internal
> owner. Java `public` does not by itself create compatibility support.

The removed umbrella could not satisfy one enforceable rule: it mixed lifecycle facades, mutable
declarations, execution exceptions, logging configuration, endpoint values, and detached runtime
snapshots. Domain-first packages make those differences explicit.

## Compatibility categories

| Category | Contract |
| --- | --- |
| Supported public API | Scenario authors may declare or invoke it. Its documented behavior is reviewed for compatibility. |
| Supported extension SPI | Driver, gateway, protocol, or framework adapters may implement or call it. Generic bounds and callback behavior are part of the contract. |
| Inspectable read-only model | Callers may retain and inspect detached immutable values. It owns no lifecycle, resources, provider endpoint lookup, or mutation capability. |
| Internal implementation | No compatibility support. It may be Java-public only where Java or JUnit requires cross-package or reflective access. |

Before 1.0, supported API and SPI may still change when the replacement is materially clearer.
Such a change is intentional, documented, and made without deprecated wrappers, aliases, or
duplicate models. Java-public internal types may change at any time.

## Current package map

| Package | Owner and responsibility |
| --- | --- |
| `communication` | Declarative communication annotations and built-in protocol semantics. |
| `component` | Component declarations, identities, and lifecycle values. |
| `configuration` | Component and driver configuration contracts, providers, validation, and redacted secrets. |
| `control` | Environment-scoped, protocol-neutral one-shot semantic traffic controls and their immutable state. |
| `diagnostics` | Stateless bounded secret-safe journal rendering. |
| `driver` | Supported component-driver extension SPI. |
| `endpoint` | Immutable endpoint addresses, bindings, and protocol-specific endpoint values. |
| `environment` | Environment facade and non-constructible safe diagnostics read model, logging configuration, validated assembly, routing SPI, and package-private execution. |
| `environment.state` | Detached immutable environment and runtime-connection state. |
| `journal` | Open event read contract, framework-owned fact vocabulary, immutable entries and snapshots; never storage. |
| `observation` | Observation policy/status, interaction identity, evidence, and forwarding decisions. |
| `proof` | Proof-subject/correlation contracts, frozen proof-plan declarations, and detached fail-closed results over observation values. |
| `topology` | Contracts, ports, protocol/interaction declarations, and logical connections. |
| `junit.annotation` | Supported JUnit declaration annotations. |
| `junit.internal` | Unsupported JUnit lifecycle implementation. |
| `testcontainers.component` | Restricted container specifications, enforced non-log startup/readiness, and runtime materialization. |
| `testcontainers.gateway` | Protocol adapters and the observe-before-forward gateway. |
| `postgresql` | Bounded PostgreSQL protocol observation, transaction evidence, write correlation, and durability checks. |
| `http` | Bounded HTTP/1.1 callback framing, exchange evidence, acknowledgement classification, and request correlation. |

Package-private types in `environment` own every mutable construction and execution concern:
component state and handles, connection bindings and routes, proof-subject allocation, journal
storage, classified diagnostics, SLF4J emission, and cleanup accumulation. Public read models own none of
those concerns.

`EnvironmentTopology.of(...)` is the single owner of full structural topology validation. It first
copies component and connection declarations, validates complete component initialization and those
exact immutable snapshots, atomically freezes component port declarations, and only then builds read
indexes. `EnvironmentBuilder` delegates to that boundary, while environment runtime assembly accepts
the resulting valid type without repeating topology validation. Every supported
`EnvironmentTopology` instance is structurally validated before it can reach environment execution.

## Dependency direction

The following is the actual direct core package-import graph. It is generated from main Java
sources and pinned exactly by `CoreArchitectureTest`; an added or removed edge requires this table
and its rationale to change together.

| Package | Direct core dependencies |
| --- | --- |
| `communication` | none |
| `component` | `configuration`, `driver`, `topology` |
| `configuration` | none |
| `control` | `observation`, `proof`, `topology` |
| `diagnostics` | `component`, `environment.state`, `journal`, `observation`, `proof`, `topology` |
| `driver` | `component`, `configuration`, `endpoint`, `environment`, `journal`, `topology` |
| `endpoint` | `configuration` |
| `environment` | `communication`, `component`, `configuration`, `control`, `diagnostics`, `driver`, `endpoint`, `environment.state`, `journal`, `observation`, `proof`, `topology` |
| `environment.state` | `observation`, `topology` |
| `journal` | `component`, `control`, `environment.state`, `observation`, `proof`, `topology` |
| `observation` | `topology` |
| `proof` | `control`, `journal`, `observation`, `topology` |
| `topology` | `component` |

There is no `journal -> diagnostics` edge and therefore no diagnostics/journal cycle. Diagnostics
renders journal facts. There is no `diagnostics -> environment` execution edge; its only
environment dependency is the detached `environment.state` read model. Logging configuration and
topology membership validation both belong to `environment`.

Five direct two-way relationships remain and are explicit technical exceptions:

- `component <-> driver`: a component declaration selects its typed driver, while the driver SPI
  starts a component and returns its runtime;
- `component <-> topology`: components own declared ports, while ports and connection identities
  retain their owning component identity;
- `driver <-> environment`: environment executes drivers; the only reverse edge is
  `ComponentRuntime -> RuntimeEndpointBindings`, the non-constructible transfer bridge that can
  publish bindings but cannot look them up.
- `control <-> proof`: subject-scoped controls use opaque proof identities, while a frozen proof
  declaration can require exact control references and terminal states without executing them.
- `journal <-> proof`: journal facts use proof identities and correlation cardinality, while
  detached proof diagnostics reuse the journal's bounded type-only `FailureDetails`; neither side
  owns mutable journal storage or proof execution.

The following directions are forbidden and executable tests enforce them:

- `observation -> proof`;
- `configuration -> component`, `driver`, or environment execution;
- `endpoint -> driver` or environment execution;
- `journal -> diagnostics` or `diagnostics -> environment` execution types;
- journal storage -> diagnostics, logging configuration, or SLF4J;
- core -> JUnit or Testcontainers;
- driver SPI -> Testcontainers;
- public API -> provider endpoint lookup or proof-subject allocation;
- JUnit public compatibility support -> `junit.internal`.

Neither core side depends on Testcontainers. Provider endpoint lookup remains inside environment
execution despite the explicitly guarded `RuntimeEndpointBindings` transfer edge.

The concrete PostgreSQL, HTTP, and SMPP adapters remain outside core and follow the module directions
`system-proof-postgresql -> system-proof-testcontainers -> system-proof-core` and
`system-proof-http -> system-proof-testcontainers -> system-proof-core`, and
`system-proof-smpp -> system-proof-testcontainers -> system-proof-core`. The examples module uses
the adapters only in test scope. Protocol parsing, mutable session state, buffers, and transport
internals are package-private and do not expand either core or gateway SPI.

## Supported core public API whitelist

The whitelist is type-specific. Nested types shown here are included; no package is supported as a
whole.

- Communication declarations: `Communication` and nested `Amqp`, `Http`, `JdbcPostgresql`,
  `Redis`, `Smpp`, and `Tcp` annotations.
- Component declarations: `Component`, `SystemComponent`.
- Configuration: `ConfigurationSource`, `EnvironmentConfiguration`, `EnvironmentVariable`,
  `Literal`, `Secret`.
- Semantic controls: `SemanticControls`, `SemanticHold`, `SemanticInteractionSelector`,
  `SemanticPredecessorGuard`, `SemanticPredecessorGuardSpec`, and
  `SemanticPredecessorRequirement`.
- Diagnostics rendering: `JournalRenderer`.
- Environment API: `Environment`, `EnvironmentBuilder`, `EnvironmentCreator`,
  `EnvironmentTopology`, `EnvironmentLogging`, `EnvironmentLoggingBuilder`, `ComponentPortFactory`,
  `ConnectionRouting`,
  `EnvironmentStartException`, `ComponentLifecycleException`.
- Journal severity: `LogLevel`, used by logging configuration and diagnostic facts.
- Proof access and execution: `ProofSubjects`, `Proofs`, `ProofPrerequisite`, `ProofPlan` and its
  `Builder`, `ProofExecution`, and `ProofConfigurationException`.
- Topology declarations: `Connection`, `Contract`, `DeclaredInteraction`, `DeclaredProtocol`,
  `InteractionSpec`, `ProtocolSpec`, `Port`, `PortContract`, `ProvidedPort`, `RequiredPort`, and
  `StartupPrerequisite`.

## Supported core extension SPI whitelist

- Component/configuration declaration SPI: `AbstractComponent`, `RuntimeConfig`, `DriverConfig`,
  `ComponentConfig`, `ConfigurationProvider`.
- Driver SPI: `ComponentDriver`, `ComponentBoundDriver`, `ComponentRuntime` and its `Builder`,
  `DriverContext`, `DriverResourceKey`, `DiagnosticSource`, `JournalContributions`, and
  `RedactedDiagnosticText` with its `Sanitizer`.
- Routing/session SPI: `ConnectionObservations`, `ConnectionRoute`, `ConnectionRouteContext`,
  `ConnectionRouteProvider`, `CorrelationContribution`, `InteractionSession`,
  `ObservationStatusProvider`, `SemanticControlRouteCapability`.
- Observation SPI: `EvidenceCodec`, `ForwardingPermit`, `InteractionDecisionCoordinator`.

Route selection, preparation, consumer-target access, observation-status extraction, and route
cleanup are not SPI. They remain package-private execution mechanics.

Observation-status callbacks are evaluated only from detached probe batches after the
`EnvironmentRuntime`, `RuntimeConnectionRegistry`, `RuntimeConnection`, capability-registry, and
semantic-control-coordinator monitors are released. One environment-owned single-flight serializes
dynamic batches. Its owner validates and atomically commits one complete cache; a concurrent
ordinary read returns the preceding complete cache, while `SemanticControls.arm(...)` and
`guard(...)` fail closed when they cannot own a fresh refresh. Startup commits before dependent
consumers run, terminal failed/degraded cache states cannot reactivate, stopped-lifecycle batches
are discarded, and replaced ownership is rejected, all without invoking the public SPI.

The runtime single-flight is not the linearization point for asynchronous required-observation
failure. `SemanticControlCoordinator` owns a terminal set of failed `ConnectionId` values.
`observationFailed(...)` adds to that set before processing existing controls; `arm(...)` and
`guard(...)` check it immediately before registration under the same coordinator monitor. This
orders failure against registration in either direction without evaluating a provider, selector,
codec, or other public callback as part of the marker operation. A stale provider result cannot
remove the marker, and unrelated connections remain independent.

## Inspectable core read-only model whitelist

- Component values: `ComponentId`, `ComponentType`, `ComponentState`.
- Endpoint values: `EndpointAddress`, `EndpointBinding`, `AmqpEndpoint`, `JdbcEndpoint`,
  `RedisEndpoint`, `SmppEndpoint`.
- Environment state: `EnvironmentState`, `ConnectionState`, `RoutingMode`,
  `RuntimeConnectionSnapshot`.
- Diagnostics: non-constructible `EnvironmentDiagnostics` and
  `DiagnosticSource.SafetyClassification`.
- Control: `SemanticHoldFailure`, `SemanticHoldRef`, `SemanticHoldState`,
  `SemanticPredecessorBoundary`, `SemanticPredecessorGuardFailure`,
  `SemanticPredecessorGuardRef`, `SemanticPredecessorGuardState`, and
  `SemanticPredecessorViolation`.
- Journal: `ScenarioEvent`, `FailureEvent`, every framework-owned event record and nested event enum,
  `FailureDetails`, `JournalEntry`, `JournalSequence`, `ScenarioJournalSnapshot`, `CheckpointId`,
  and `DisruptionId`.
- Observation: `ObservationRequirement`, `RequiredObservationProfile` and its `Capability` and
  `Feature` enums, `EffectiveObservationStatus`, `EvidenceSchemaId`,
  `EvidenceSnapshot`, `FlowDirection`, `ForwardingDecision`, `SessionId`, `InteractionRef`, and
  `RecordedInteraction`.
- Proof: `ProofSubjectRef`, `CorrelationKeySchema`, `CorrelationKey`, `CorrelationCardinality`,
  `CorrelationResult` and nested `Missing`, `Unique`, and `Ambiguous` results; `ProofPlanId`,
  `ProofObligationId`, frozen `ProofPlan.Requirement` records, `ProofExecutionState`, `ProofOutcome`,
  `ProofResolution`, `ProofResolutionReason`, `ProofRequirementKind`, `ProofEvidenceKind`,
  `ProofPrerequisiteStatus`, `ProofDiagnostic`, `ProofRequirementDescriptor` and its typed records,
  `ProofStimulusState`, `ProofStimulusResolution`, `ProofEvaluationState`,
  `ProofEvaluationResolution`, `ProofInteractionProvenance` and its `Role`,
  `ProofObligationResolution`, `ProofReport`, and `ProofResult`.
- Topology inspection: `CompatibilityResult`, `ConnectionDescriptor`, `ConnectionId`,
  `ConnectionRef`, `PortDirection`, `PortRef`.

Record canonical constructors are supported only where callers legitimately create declarations or
detached values. Records and value classes use value equality. `AbstractComponent`, `Component`,
environment facades, executions, runtimes, routes, and resources retain instance identity.

## Java-public internal exceptions

Only these core types remain Java-public without compatibility support:

| Type/member | Why Java-public | Guarded restriction |
| --- | --- | --- |
| `ConfigurationBinder` | Environment assembly crosses into the configuration owner. | Static `bind` only; no public constructor. |
| `ConfigurationValidator` | Environment assembly validates bound configuration. | Static `validate` only; no public constructor. |
| `ConfigurationValues` | Configuration records share fail-fast value checks. | `requireNonNull` and `requireText` only; no public constructor. |
| `RuntimeEndpointBindings` | `ComponentRuntime` transfers driver-published bindings across the driver/environment boundary. | No public constructor or lookup; public `publish` only. |
| `AbstractComponent.driver()` and `castOperations(...)` | Typed execution needs the declaration's driver and operations class without raw casts. | Exact method surface is pinned; neither exposes mutable execution state. |
| `ComponentRuntime.publishBindingsTo(...)` | Transfers already driver-owned bindings into the non-constructible environment boundary. | No environment/runtime lookup path is exposed. |

`EnvironmentRuntime`, its factory, assembly, lifecycle, inspector, component supervisor, connection
registry, proof registry, proof execution coordinator, protocol-neutral outcome evaluator,
proof evidence-window watermark tracker, proof current-state index, journal store, classified
diagnostics capture, emitter, and failure accumulator are package-private.
`EnvironmentTopology.runtimeComponents()` is package-private; public topology
inspection returns only `List<Component>` and logical connections. `EnvironmentLogging` exposes
only `logs()` and `defaults()` plus value methods; threshold lookup and `validateAgainst(...)` are
package-private. Its builder is the supported mutation boundary.

## JUnit whitelist

Supported API consists only of `SystemProof` and `EnvironmentDefinition`.

`EnvironmentLifecycleExtension`, `EnvironmentParameterResolver`, and
`SystemProofInvocationProvider` are Java-public only because `SystemProof` names them in
`@ExtendWith` and JUnit constructs them reflectively. Their exact constructors and callback methods
are guarded, but they are not supported SPI. All resolver, validator, reporter, shared-context,
running-environment, metadata, and failure-adapter collaborators are package-private in
`junit.internal`.

## Testcontainers whitelist

- Supported API: `ContainerDriver`, `ContainerPlan` and `Builder`, `PortBinding`,
  `InteractionGateway`, and `TcpEndpointAdapter`.
- Supported SPI: `ContainerDriver.OperationsFactory`, `ContainerDriver.PlanFactory`,
  `RuntimeEndpointFactory`, `StartedContainer`, `TestcontainersDriver`, `ProtocolAdapter`,
  `ProtocolAdapterException`, `ProtocolSession`, `ProtocolStream`, and
  `TcpEndpointAdapter.AddressReplacement`.
- Inspectable read-only model: `ProtocolDecodeResult` and nested results,
  `ProtocolFailureKind`, `ProtocolLimits`, and `ProtocolUnit`.
- Java-public internal exceptions: none. Plan inspection/validation and
  `TestcontainersDriver.networkAlias(...)` are package-private implementation details.

The Testcontainers surface depends on core contracts. Core and driver SPI never depend back on it.

## PostgreSQL whitelist

- Supported API: `PostgresqlProtocolAdapter`, `TransactionRef`,
  `PostgresqlDurabilityRequirements` and nested `Table`, `PostgresqlDurabilityResult` and nested
  `Setting` and `RelationStatus`, and `PostgresqlDurabilityVerifier`.
- Supported extension SPI: `PostgresqlWriteCorrelation` and `PostgresqlWriteInteraction`.
- Inspectable read-only model: `PostgresqlEvidence` and all nested evidence records/enums, plus
  `PostgresqlStatementShape` and nested `Kind`.
- Java-public internal exceptions: none.

`PostgresqlPublicSurfaceTest` pins this exact surface and rejects public parser, session, portal, or
buffer types. The module's main bytecode is also checked for JUnit, Spring, and Testcontainers
implementation dependencies.

## HTTP whitelist

- Supported API: `HttpProtocolAdapter`, `HttpProtocolLimits`, and `HttpExchangeRef`.
- Supported extension SPI: `HttpRequestCorrelation` and `HttpRequestInteraction`.
- Inspectable read-only model: `HttpEvidence` and all nested evidence records/enums.
- Java-public internal exceptions: none.

`HttpPublicSurfaceTest` pins this exact surface and rejects public parser, session, header, or
buffer types. The module's main bytecode is also checked for JUnit, Spring, and Testcontainers
implementation dependencies.

## SMPP whitelist

- Supported API: `SmppProtocolAdapter`, `SmppProtocolLimits`, and `SmppExchangeRef`.
- Supported extension SPI: `SmppDeliverCorrelation`, `SmppDeliverInteraction`, and nested
  `Characters`.
- Inspectable read-only model: `SmppEvidence` and all nested evidence records/enums.
- Java-public internal exceptions: none.

`SmppPublicSurfaceTest` pins this exact surface and rejects public parser, session-model, TLV, or
buffer types. The module's main bytecode is also checked for JUnit, Spring, and Testcontainers
implementation dependencies.

## Inventory ownership matrix

Types grouped in one row share the listed properties. Every externally visible framework type is
named by the module whitelists above or by the Java-public internal table.

| Types | Created by / consumed by | Mutation, lifecycle, resources | Equality and construction | Secret and `toString` policy | Reason to change |
| --- | --- | --- | --- | --- | --- |
| Communication annotations | Scenario component declarations / environment port discovery | None | Annotation values | Protocol IDs only | Communication declaration semantics |
| Component declarations and markers | Scenario authors and environment assembly / drivers and topology | Declaration initialization ends before execution; no runtime handles | Components use instance identity; IDs/types use value equality | No endpoint values or secrets | Component declaration contract |
| Configuration API and SPI | Scenario/environment sources / component and driver binders | Immutable snapshots; no resources | Provider/value semantics as documented | `Secret.toString()` is always redacted; no generated secret equality/toString | Configuration contract |
| Configuration Java-public internals | Environment assembly / configuration implementation | Stateless | No public constructors | Error text names fields, not secret values | Binding or validation implementation |
| Environment logging configuration | Scenario authors / environment publisher and SLF4J emitter | Immutable configuration; builder is mutable before `build()` | Value/configuration semantics | Threshold maps have no public accessors | Environment logging and membership policy |
| Diagnostics rendering | Inspector / users and SLF4J emitter | Immutable bounded environment result; renderer is stateless | Environment diagnostics has no public constructor or text factory | Typed safe facts, type-only failures, bounded redacted text; unknown event fallback renders only its type | Diagnostic presentation policy |
| Driver SPI | Adapter authors / environment component supervisor | `ComponentRuntime` may own one closeable resource; environment closes it | Runtime and resource keys use identity where ownership requires it | Log text requires bounded redaction; suppliers are redacted, opt-in sensitive, or unsupported | Component runtime extension contract |
| Endpoint values | Drivers / environment connection materialization | Immutable; no owned resources | Value equality | Passwords use `Secret`; endpoint values never appear in public runtime snapshots | Endpoint contract |
| Environment API | Scenario authors / JUnit and examples | `Environment` owns exactly one execution, one observation-refresh single-flight, and connection-scoped terminal observation-failure markers; lifecycle methods are final | Facades use identity; topology snapshots use structural/value views; concurrent ordinary reads may use the last complete cache; failure and control registration share the coordinator linearization point | Default diagnostics excludes raw/sensitive sources; exceptions render type-only facts; retained semantic controls require fresh fail-closed validation | Environment lifecycle or assembly contract |
| Routing/session SPI | Gateway/Testcontainers / environment connection execution | Route resources are connection-owned and closed internally | Route/session objects use execution identity; contribution metadata is detached | No public consumer-target getter or raw evidence rendering | Routing or observation extension contract |
| Environment state read models | Inspector / users, journal, diagnostics | Detached immutable; no resources or mutation | Value equality and defensive lists | Endpoint availability booleans only | Inspectable lifecycle state |
| Journal vocabulary and read models | Environment publisher/store / inspector, renderer, users | Immutable; storage is separate and package-private | Value equality; snapshots defensively copy | Failures are type-only; diagnostic text is bounded/redacted; evidence bytes are defensive copies | Auditable fact vocabulary |
| Observation contracts | Gateway/codecs / journal, proof, environment | Immutable except execution-owned session implementation | Structural identities and defensive evidence values | Evidence `toString` never emits bytes | Observation policy, evidence, or forwarding semantics |
| Proof contracts | Environment registry / scenario users and journal | Public facade exposes correlation, not allocation; registry is internal | Opaque subject identity; keys/results use value semantics | Digests/native references are not rendered as secrets | Proof-subject or correlation semantics |
| Topology contracts | Scenario/environment assembly / drivers and execution | Immutable after validated assembly | Connections/components preserve declared identity; descriptors/IDs use value equality | No runtime endpoint values | Logical topology semantics |
| JUnit annotations | Test authors / JUnit extensions | None | Annotation values | Metadata only | JUnit declaration contract |
| JUnit internal extensions | `@ExtendWith`/JUnit reflection / JUnit callbacks | Per-test shared context and lifecycle only | Internal identity | Only the bounded safe artifact is written; no raw attachment path | JUnit lifecycle implementation |
| Testcontainers API/SPI | Adapter authors and examples / environment routing and drivers | System Proof creates and owns the restricted container lifecycle; route resources have explicit owners | Plans and protocol results use documented value/identity semantics; arbitrary `GenericContainer` instances are rejected by construction | Container logging, full-log retrieval, log consumers, and log-based waits are unavailable; driver-authored journal text still requires bounded redaction | Container or protocol adapter contract |

## Event and sealed hierarchy evolution

`ScenarioEvent` is deliberately open. Framework releases may add immutable core fact records
without making an exhaustive client switch source-incompatible. A client type may implement the
read contract, but that grants no append, publication, contribution, or environment injection
capability. `JournalRenderer` handles unknown implementations through a type-only fallback and
never calls arbitrary payload `toString()`.

`FailureEvent`, `CorrelationResult`, `ProtocolDecodeResult`, and `ConnectionRef` remain
core-controlled sealed hierarchies. Their permitted implementations are inspectable read models,
not user extension points. Storage, mutation, and publication remain package-private when either
the open event vocabulary or a closed result vocabulary grows.

## Placement examples

- A new logical port identity belongs in `topology`.
- A detached connection status belongs in `environment.state`.
- A driver callback or runtime result belongs in `driver`.
- A gateway observation decision belongs in `observation`; a mutable coordinator implementation
  remains internal to `environment` or Testcontainers.
- A new stored fact belongs in `journal`; its publisher and mutable storage remain in `environment`.
- A text representation over an immutable snapshot belongs in `diagnostics`.
- A lifecycle exception created by environment execution belongs in `environment`, not in a value
  package.

## Enforcement

`CoreArchitectureTest` walks every class file recursively, including nested `$` classes. It compares
all public/protected types with the four exact, pairwise-disjoint whitelists; pins every public field
and constant, sensitive method/constructor surfaces, the open `ScenarioEvent` contract, and the
actual package graph; checks private runtime construction; forbids public route mechanics, provider
lookup, proof allocation, and journal mutation; and verifies one journal storage owner.
`Junit5ModuleBoundaryTest`, `TestcontainersPublicSurfaceTest`, `PostgresqlPublicSurfaceTest`,
`HttpPublicSurfaceTest`, and `SmppPublicSurfaceTest` apply exact categories and field/surface guards
to their modules. The examples module compiles against supported imports and rejects
internal/removed package usage.
