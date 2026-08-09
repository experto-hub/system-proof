package io.github.jacekkardys.systemproof;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.component.AbstractComponent;
import io.github.jacekkardys.systemproof.configuration.ConfigurationBinder;
import io.github.jacekkardys.systemproof.configuration.ConfigurationValidator;
import io.github.jacekkardys.systemproof.configuration.ConfigurationValues;
import io.github.jacekkardys.systemproof.driver.ComponentRuntime;
import io.github.jacekkardys.systemproof.endpoint.EndpointBinding;
import io.github.jacekkardys.systemproof.environment.ComponentLifecycleException;
import io.github.jacekkardys.systemproof.environment.ComponentPortFactory;
import io.github.jacekkardys.systemproof.environment.ConnectionRoute;
import io.github.jacekkardys.systemproof.environment.ConnectionRouteContext;
import io.github.jacekkardys.systemproof.environment.ConnectionRouting;
import io.github.jacekkardys.systemproof.environment.CorrelationContribution;
import io.github.jacekkardys.systemproof.environment.EnvironmentLogging;
import io.github.jacekkardys.systemproof.environment.EnvironmentLoggingBuilder;
import io.github.jacekkardys.systemproof.environment.EnvironmentDiagnostics;
import io.github.jacekkardys.systemproof.environment.EnvironmentTopology;
import io.github.jacekkardys.systemproof.environment.RuntimeEndpointBindings;
import io.github.jacekkardys.systemproof.diagnostics.JournalRenderer;
import io.github.jacekkardys.systemproof.journal.JournalEntry;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;
import io.github.jacekkardys.systemproof.observation.InteractionDecisionCoordinator;

class CoreArchitectureTest {
    private static final Path CLASSES = Path.of("target/classes");
    private static final Path SOURCES = Path.of("src/main/java");
    private static final String BASE_PATH = "io/github/jacekkardys/systemproof/";
    private static final String BASE_PACKAGE = "io.github.jacekkardys.systemproof.";
    private static final Pattern PACKAGE_DECLARATION =
        Pattern.compile("(?m)^package\\s+([\\w.]+);");
    private static final Pattern IMPORT_DECLARATION =
        Pattern.compile("(?m)^import\\s+(?:static\\s+)?([\\w.]+)(?:\\.\\*)?;");

    private static final Set<String> SUPPORTED_API = types("""
        communication.Communication
        communication.Communication$Amqp
        communication.Communication$Http
        communication.Communication$JdbcPostgresql
        communication.Communication$Redis
        communication.Communication$Smpp
        communication.Communication$Tcp
        component.Component
        component.SystemComponent
        control.SemanticControls
        control.SemanticHold
        control.SemanticInteractionSelector
        control.SemanticPredecessorGuard
        control.SemanticPredecessorGuardSpec
        control.SemanticPredecessorRequirement
        configuration.ConfigurationSource
        configuration.EnvironmentConfiguration
        configuration.EnvironmentVariable
        configuration.Literal
        configuration.Secret
        diagnostics.JournalRenderer
        environment.ComponentLifecycleException
        environment.ComponentPortFactory
        environment.ConnectionRouting
        environment.Environment
        environment.EnvironmentBuilder
        environment.EnvironmentCreator
        environment.EnvironmentLogging
        environment.EnvironmentLoggingBuilder
        environment.EnvironmentStartException
        environment.EnvironmentTopology
        journal.LogLevel
        proof.ProofConfigurationException
        proof.ProofExecution
        proof.ProofPlan
        proof.ProofPlan$Builder
        proof.ProofPrerequisite
        proof.ProofSubjects
        proof.Proofs
        topology.Connection
        topology.Contract
        topology.DeclaredInteraction
        topology.DeclaredProtocol
        topology.InteractionSpec
        topology.Port
        topology.PortContract
        topology.ProtocolSpec
        topology.ProvidedPort
        topology.RequiredPort
        topology.StartupPrerequisite
        """);

    private static final Set<String> SUPPORTED_SPI = types("""
        component.AbstractComponent
        configuration.DriverConfig
        configuration.RuntimeConfig
        configuration.ComponentConfig
        configuration.ConfigurationProvider
        driver.ComponentBoundDriver
        driver.ComponentDriver
        driver.ComponentRuntime
        driver.ComponentRuntime$Builder
        driver.DiagnosticSource
        driver.DriverContext
        driver.DriverResourceKey
        driver.JournalContributions
        journal.RedactedDiagnosticText
        journal.RedactedDiagnosticText$Sanitizer
        environment.ConnectionObservations
        environment.ConnectionRoute
        environment.ConnectionRouteContext
        environment.ConnectionRouteProvider
        environment.CorrelationContribution
        environment.InteractionSession
        environment.ObservationStatusProvider
        environment.SemanticControlRouteCapability
        observation.EvidenceCodec
        observation.ForwardingPermit
        observation.InteractionDecisionCoordinator
        """);

    private static final Set<String> READ_ONLY_MODEL = types("""
        component.ComponentId
        component.ComponentState
        component.ComponentType
        control.SemanticHoldFailure
        control.SemanticHoldRef
        control.SemanticHoldState
        control.SemanticPredecessorBoundary
        control.SemanticPredecessorGuardFailure
        control.SemanticPredecessorGuardRef
        control.SemanticPredecessorGuardState
        control.SemanticPredecessorViolation
        endpoint.AmqpEndpoint
        endpoint.EndpointAddress
        endpoint.EndpointBinding
        endpoint.JdbcEndpoint
        endpoint.RedisEndpoint
        endpoint.SmppEndpoint
        driver.DiagnosticSource$SafetyClassification
        environment.EnvironmentDiagnostics
        environment.state.ConnectionState
        environment.state.EnvironmentState
        environment.state.RoutingMode
        environment.state.RuntimeConnectionSnapshot
        journal.CheckpointEvent
        journal.CheckpointEvent$Kind
        journal.CheckpointEvent$Stage
        journal.CheckpointId
        journal.ComponentLifecycleEvent
        journal.ConnectionLifecycleEvent
        journal.CorrelationCandidateEvent
        journal.DiagnosticEvent
        journal.DiagnosticEvent$ComponentSubject
        journal.DiagnosticEvent$ConnectionSubject
        journal.DiagnosticEvent$EnvironmentSubject
        journal.DiagnosticEvent$Subject
        journal.DisruptionId
        journal.DisruptionLifecycleEvent
        journal.DisruptionLifecycleEvent$Stage
        journal.EnvironmentLifecycleEvent
        journal.FailureDetails
        journal.FailureEvent
        journal.FailureEvent$ComponentCleanup
        journal.FailureEvent$ComponentStartup
        journal.FailureEvent$ConnectionCleanup
        journal.FailureEvent$ConnectionMaterialization
        journal.FailureEvent$DriverResourceCleanup
        journal.FailureEvent$EnvironmentStartup
        journal.InteractionObservationEvent
        journal.JournalEntry
        journal.JournalSequence
        journal.ProofSubjectArmedEvent
        journal.ProofSubjectCreatedEvent
        journal.ScenarioEvent
        journal.ScenarioJournalSnapshot
        journal.SemanticHoldEvent
        journal.SemanticPredecessorGuardEvent
        journal.SemanticPredecessorGuardEvent$Kind
        observation.EffectiveObservationStatus
        observation.EvidenceSchemaId
        observation.EvidenceSnapshot
        observation.FlowDirection
        observation.ForwardingDecision
        observation.InteractionRef
        observation.ObservationRequirement
        observation.RequiredObservationProfile
        observation.RequiredObservationProfile$Capability
        observation.RequiredObservationProfile$Feature
        observation.RecordedInteraction
        observation.SessionId
        proof.CorrelationCardinality
        proof.CorrelationKey
        proof.CorrelationKeySchema
        proof.CorrelationResult
        proof.CorrelationResult$Ambiguous
        proof.CorrelationResult$Missing
        proof.CorrelationResult$Unique
        proof.ProofDiagnostic
        proof.ProofEvaluationResolution
        proof.ProofEvaluationState
        proof.ProofEvidenceKind
        proof.ProofExecutionState
        proof.ProofFailureStage
        proof.ProofInteractionProvenance
        proof.ProofInteractionProvenance$Role
        proof.ProofObligationId
        proof.ProofObligationResolution
        proof.ProofOutcome
        proof.ProofPlan$CausalRelation
        proof.ProofPlan$Correlation
        proof.ProofPlan$GuardControl
        proof.ProofPlan$GuardEvidence
        proof.ProofPlan$HoldControl
        proof.ProofPlan$HoldEvidence
        proof.ProofPlan$Observation
        proof.ProofPlan$Prerequisite
        proof.ProofPlan$Requirement
        proof.ProofPlanId
        proof.ProofPrerequisiteStatus
        proof.ProofReport
        proof.ProofRequirementDescriptor
        proof.ProofRequirementDescriptor$CausalRelation
        proof.ProofRequirementDescriptor$Correlation
        proof.ProofRequirementDescriptor$GuardControl
        proof.ProofRequirementDescriptor$GuardEvidence
        proof.ProofRequirementDescriptor$HoldControl
        proof.ProofRequirementDescriptor$HoldEvidence
        proof.ProofRequirementDescriptor$Observation
        proof.ProofRequirementDescriptor$Prerequisite
        proof.ProofRequirementKind
        proof.ProofResolution
        proof.ProofResolutionReason
        proof.ProofResult
        proof.ProofStimulusResolution
        proof.ProofStimulusState
        proof.ProofSubjectRef
        topology.CompatibilityResult
        topology.ConnectionDescriptor
        topology.ConnectionId
        topology.ConnectionRef
        topology.PortDirection
        topology.PortRef
        """);

    private static final Set<String> JAVA_PUBLIC_INTERNAL = types("""
        configuration.ConfigurationBinder
        configuration.ConfigurationValidator
        configuration.ConfigurationValues
        environment.RuntimeEndpointBindings
        """);

    private static final Set<String> PUBLIC_FIELDS = lines("""
        component.ComponentState#DECLARED:component.ComponentState
        component.ComponentState#FAILED:component.ComponentState
        component.ComponentState#RUNNING:component.ComponentState
        component.ComponentState#STARTING:component.ComponentState
        component.ComponentState#STOPPED:component.ComponentState
        component.ComponentState#STOPPING:component.ComponentState
        control.SemanticHoldFailure#AMBIGUOUS_MATCH:control.SemanticHoldFailure
        control.SemanticHoldFailure#CORRELATION_INVALIDATED:control.SemanticHoldFailure
        control.SemanticHoldFailure#INTERNAL_FAILURE:control.SemanticHoldFailure
        control.SemanticHoldFailure#SELECTOR_EVALUATION:control.SemanticHoldFailure
        control.SemanticHoldFailure#SESSION_ABANDONED:control.SemanticHoldFailure
        control.SemanticHoldFailure#WRITE_FAILURE:control.SemanticHoldFailure
        control.SemanticHoldState#ARMED:control.SemanticHoldState
        control.SemanticHoldState#CANCELLED:control.SemanticHoldState
        control.SemanticHoldState#DECLARED:control.SemanticHoldState
        control.SemanticHoldState#FAILED:control.SemanticHoldState
        control.SemanticHoldState#FORWARDED:control.SemanticHoldState
        control.SemanticHoldState#REACHED_HELD:control.SemanticHoldState
        control.SemanticHoldState#RELEASING:control.SemanticHoldState
        control.SemanticHoldState#TIMED_OUT:control.SemanticHoldState
        control.SemanticPredecessorBoundary#CONFIRMED:control.SemanticPredecessorBoundary
        control.SemanticPredecessorBoundary#FORWARDED:control.SemanticPredecessorBoundary
        control.SemanticPredecessorGuardFailure#CORRELATION_INVALIDATED:control.SemanticPredecessorGuardFailure
        control.SemanticPredecessorGuardFailure#INTERNAL_FAILURE:control.SemanticPredecessorGuardFailure
        control.SemanticPredecessorGuardFailure#REQUIRED_OBSERVATION_FAILURE:control.SemanticPredecessorGuardFailure
        control.SemanticPredecessorGuardFailure#SELECTOR_EVALUATION:control.SemanticPredecessorGuardFailure
        control.SemanticPredecessorGuardFailure#SESSION_ABANDONED:control.SemanticPredecessorGuardFailure
        control.SemanticPredecessorGuardFailure#WRITE_FAILURE:control.SemanticPredecessorGuardFailure
        control.SemanticPredecessorGuardState#ARMED:control.SemanticPredecessorGuardState
        control.SemanticPredecessorGuardState#CANCELLED:control.SemanticPredecessorGuardState
        control.SemanticPredecessorGuardState#DECLARED:control.SemanticPredecessorGuardState
        control.SemanticPredecessorGuardState#FAILED:control.SemanticPredecessorGuardState
        control.SemanticPredecessorGuardState#PREDECESSOR_OBSERVED:control.SemanticPredecessorGuardState
        control.SemanticPredecessorGuardState#PREDECESSOR_SATISFIED:control.SemanticPredecessorGuardState
        control.SemanticPredecessorGuardState#SATISFIED:control.SemanticPredecessorGuardState
        control.SemanticPredecessorGuardState#SUCCESSOR_AUTHORIZED:control.SemanticPredecessorGuardState
        control.SemanticPredecessorGuardState#TIMED_OUT:control.SemanticPredecessorGuardState
        control.SemanticPredecessorGuardState#VIOLATED:control.SemanticPredecessorGuardState
        control.SemanticPredecessorViolation#PREDECESSOR_NOT_ESTABLISHED:control.SemanticPredecessorViolation
        observation.RequiredObservationProfile$Capability#CORRELATION_CONTRIBUTIONS:observation.RequiredObservationProfile$Capability
        observation.RequiredObservationProfile$Capability#SEMANTIC_CONTROL:observation.RequiredObservationProfile$Capability
        observation.RequiredObservationProfile$Feature#ENCRYPTED_TRANSPORT:observation.RequiredObservationProfile$Feature
        observation.RequiredObservationProfile$Feature#GENERAL_PIPELINING:observation.RequiredObservationProfile$Feature
        configuration.ConfigurationSource#UNSET:java.lang.String
        driver.DiagnosticSource$SafetyClassification#OPT_IN_SENSITIVE:driver.DiagnosticSource$SafetyClassification
        driver.DiagnosticSource$SafetyClassification#REDACTED_TEXT:driver.DiagnosticSource$SafetyClassification
        driver.DiagnosticSource$SafetyClassification#UNSUPPORTED_FOR_EXPORT:driver.DiagnosticSource$SafetyClassification
        environment.state.ConnectionState#DECLARED:environment.state.ConnectionState
        environment.state.ConnectionState#FAILED:environment.state.ConnectionState
        environment.state.ConnectionState#RUNNING:environment.state.ConnectionState
        environment.state.ConnectionState#STARTING:environment.state.ConnectionState
        environment.state.ConnectionState#STOPPED:environment.state.ConnectionState
        environment.state.ConnectionState#STOPPING:environment.state.ConnectionState
        environment.state.EnvironmentState#DECLARED:environment.state.EnvironmentState
        environment.state.EnvironmentState#FAILED:environment.state.EnvironmentState
        environment.state.EnvironmentState#RUNNING:environment.state.EnvironmentState
        environment.state.EnvironmentState#STARTING:environment.state.EnvironmentState
        environment.state.EnvironmentState#STOPPED:environment.state.EnvironmentState
        environment.state.EnvironmentState#STOPPING:environment.state.EnvironmentState
        environment.state.RoutingMode#DIRECT:environment.state.RoutingMode
        environment.state.RoutingMode#ROUTED:environment.state.RoutingMode
        journal.CheckpointEvent$Kind#BARRIER:journal.CheckpointEvent$Kind
        journal.CheckpointEvent$Kind#CHECKPOINT:journal.CheckpointEvent$Kind
        journal.CheckpointEvent$Stage#CLEARED:journal.CheckpointEvent$Stage
        journal.CheckpointEvent$Stage#DECLARED:journal.CheckpointEvent$Stage
        journal.CheckpointEvent$Stage#OBSERVED:journal.CheckpointEvent$Stage
        journal.DiagnosticEvent$EnvironmentSubject#INSTANCE:journal.DiagnosticEvent$EnvironmentSubject
        journal.DisruptionLifecycleEvent$Stage#ACTIVE:journal.DisruptionLifecycleEvent$Stage
        journal.DisruptionLifecycleEvent$Stage#CLEARED:journal.DisruptionLifecycleEvent$Stage
        journal.DisruptionLifecycleEvent$Stage#DECLARED:journal.DisruptionLifecycleEvent$Stage
        journal.DisruptionLifecycleEvent$Stage#FAILED:journal.DisruptionLifecycleEvent$Stage
        journal.JournalSequence#FIRST_VALUE:long
        journal.LogLevel#DEBUG:journal.LogLevel
        journal.LogLevel#ERROR:journal.LogLevel
        journal.LogLevel#INFO:journal.LogLevel
        journal.LogLevel#OFF:journal.LogLevel
        journal.LogLevel#TRACE:journal.LogLevel
        journal.LogLevel#WARN:journal.LogLevel
        journal.SemanticPredecessorGuardEvent$Kind#DECISION:journal.SemanticPredecessorGuardEvent$Kind
        journal.SemanticPredecessorGuardEvent$Kind#RELATION:journal.SemanticPredecessorGuardEvent$Kind
        journal.SemanticPredecessorGuardEvent$Kind#STATE:journal.SemanticPredecessorGuardEvent$Kind
        journal.SemanticPredecessorGuardEvent$Kind#SUPPRESSED_FAILURE:journal.SemanticPredecessorGuardEvent$Kind
        journal.SemanticPredecessorGuardEvent$Kind#TERMINAL:journal.SemanticPredecessorGuardEvent$Kind
        journal.SemanticPredecessorGuardEvent$Kind#VIOLATION:journal.SemanticPredecessorGuardEvent$Kind
        observation.EffectiveObservationStatus#ACTIVE:observation.EffectiveObservationStatus
        observation.EffectiveObservationStatus#DEGRADED:observation.EffectiveObservationStatus
        observation.EffectiveObservationStatus#DISABLED:observation.EffectiveObservationStatus
        observation.EffectiveObservationStatus#FAILED:observation.EffectiveObservationStatus
        observation.EffectiveObservationStatus#INACTIVE:observation.EffectiveObservationStatus
        observation.EffectiveObservationStatus#PENDING:observation.EffectiveObservationStatus
        observation.EffectiveObservationStatus#UNSUPPORTED:observation.EffectiveObservationStatus
        observation.FlowDirection#CONSUMER_TO_PROVIDER:observation.FlowDirection
        observation.FlowDirection#PROVIDER_TO_CONSUMER:observation.FlowDirection
        observation.ForwardingDecision#FORWARD:observation.ForwardingDecision
        observation.ForwardingDecision#CLOSE_SESSION:observation.ForwardingDecision
        observation.InteractionRef#FIRST_ORDINAL:long
        observation.ObservationRequirement#DISABLED:observation.ObservationRequirement
        observation.ObservationRequirement#OPTIONAL:observation.ObservationRequirement
        observation.ObservationRequirement#REQUIRED:observation.ObservationRequirement
        observation.SessionId#FIRST_VALUE:long
        proof.CorrelationCardinality#AMBIGUOUS:proof.CorrelationCardinality
        proof.CorrelationCardinality#MISSING:proof.CorrelationCardinality
        proof.CorrelationCardinality#UNIQUE:proof.CorrelationCardinality
        proof.ProofEvaluationState#COMPLETED:proof.ProofEvaluationState
        proof.ProofEvaluationState#FAILED:proof.ProofEvaluationState
        proof.ProofEvaluationState#NOT_STARTED:proof.ProofEvaluationState
        proof.ProofEvaluationState#RUNNING:proof.ProofEvaluationState
        proof.ProofEvidenceKind#HELD_INTERACTION:proof.ProofEvidenceKind
        proof.ProofEvidenceKind#PREDECESSOR_INTERACTION:proof.ProofEvidenceKind
        proof.ProofEvidenceKind#SUCCESSOR_INTERACTION:proof.ProofEvidenceKind
        proof.ProofExecutionState#ACTIVATING:proof.ProofExecutionState
        proof.ProofExecutionState#ACTIVE:proof.ProofExecutionState
        proof.ProofExecutionState#COMPLETED:proof.ProofExecutionState
        proof.ProofExecutionState#DRAFT:proof.ProofExecutionState
        proof.ProofExecutionState#EVALUATING:proof.ProofExecutionState
        proof.ProofFailureStage#ACTIVATION:proof.ProofFailureStage
        proof.ProofFailureStage#CLEANUP:proof.ProofFailureStage
        proof.ProofFailureStage#CONTROL:proof.ProofFailureStage
        proof.ProofFailureStage#CORRELATION:proof.ProofFailureStage
        proof.ProofFailureStage#EVALUATION:proof.ProofFailureStage
        proof.ProofFailureStage#GATEWAY:proof.ProofFailureStage
        proof.ProofFailureStage#JOURNAL:proof.ProofFailureStage
        proof.ProofFailureStage#OBSERVATION:proof.ProofFailureStage
        proof.ProofFailureStage#STIMULUS:proof.ProofFailureStage
        proof.ProofFailureStage#TEARDOWN:proof.ProofFailureStage
        proof.ProofInteractionProvenance$Role#CORRELATION:proof.ProofInteractionProvenance$Role
        proof.ProofInteractionProvenance$Role#HOLD:proof.ProofInteractionProvenance$Role
        proof.ProofInteractionProvenance$Role#PREDECESSOR:proof.ProofInteractionProvenance$Role
        proof.ProofInteractionProvenance$Role#SUCCESSOR:proof.ProofInteractionProvenance$Role
        proof.ProofOutcome#ERROR:proof.ProofOutcome
        proof.ProofOutcome#INCONCLUSIVE:proof.ProofOutcome
        proof.ProofOutcome#PROVED:proof.ProofOutcome
        proof.ProofOutcome#VIOLATED:proof.ProofOutcome
        proof.ProofPrerequisiteStatus#FAILED:proof.ProofPrerequisiteStatus
        proof.ProofPrerequisiteStatus#SATISFIED:proof.ProofPrerequisiteStatus
        proof.ProofPrerequisiteStatus#UNSUPPORTED:proof.ProofPrerequisiteStatus
        proof.ProofRequirementKind#CAUSAL_RELATION:proof.ProofRequirementKind
        proof.ProofRequirementKind#CONTROL:proof.ProofRequirementKind
        proof.ProofRequirementKind#CORRELATION:proof.ProofRequirementKind
        proof.ProofRequirementKind#EVIDENCE:proof.ProofRequirementKind
        proof.ProofRequirementKind#OBSERVATION:proof.ProofRequirementKind
        proof.ProofRequirementKind#PREREQUISITE:proof.ProofRequirementKind
        proof.ProofResolution#AMBIGUOUS:proof.ProofResolution
        proof.ProofResolution#FAILED:proof.ProofResolution
        proof.ProofResolution#MISSING:proof.ProofResolution
        proof.ProofResolution#NOT_EVALUATED:proof.ProofResolution
        proof.ProofResolution#SATISFIED:proof.ProofResolution
        proof.ProofResolution#TIMED_OUT:proof.ProofResolution
        proof.ProofResolution#UNREACHED:proof.ProofResolution
        proof.ProofResolution#UNSUPPORTED:proof.ProofResolution
        proof.ProofResolution#VIOLATED:proof.ProofResolution
        proof.ProofResolutionReason#ACTIVATION_NOT_REACHED:proof.ProofResolutionReason
        proof.ProofResolutionReason#CAUSAL_RELATION_ESTABLISHED:proof.ProofResolutionReason
        proof.ProofResolutionReason#CAUSAL_RELATION_UNREACHED:proof.ProofResolutionReason
        proof.ProofResolutionReason#CAUSAL_RELATION_VIOLATED:proof.ProofResolutionReason
        proof.ProofResolutionReason#CONTROL_CORRELATION_INVALIDATED:proof.ProofResolutionReason
        proof.ProofResolutionReason#CONTROL_FAILED:proof.ProofResolutionReason
        proof.ProofResolutionReason#CONTROL_MATCH_AMBIGUOUS:proof.ProofResolutionReason
        proof.ProofResolutionReason#CONTROL_REACHED_EXPECTED_STATE:proof.ProofResolutionReason
        proof.ProofResolutionReason#CONTROL_SELECTOR_FAILED:proof.ProofResolutionReason
        proof.ProofResolutionReason#CONTROL_SESSION_ENDED:proof.ProofResolutionReason
        proof.ProofResolutionReason#CONTROL_TIMED_OUT:proof.ProofResolutionReason
        proof.ProofResolutionReason#CONTROL_UNREACHED:proof.ProofResolutionReason
        proof.ProofResolutionReason#CORRELATION_AMBIGUOUS:proof.ProofResolutionReason
        proof.ProofResolutionReason#CORRELATION_MISSING:proof.ProofResolutionReason
        proof.ProofResolutionReason#CORRELATION_UNIQUE:proof.ProofResolutionReason
        proof.ProofResolutionReason#DEADLINE_EXPIRED:proof.ProofResolutionReason
        proof.ProofResolutionReason#EVALUATION_COMPLETED:proof.ProofResolutionReason
        proof.ProofResolutionReason#EVALUATION_FAILED:proof.ProofResolutionReason
        proof.ProofResolutionReason#EVALUATION_NOT_REACHED:proof.ProofResolutionReason
        proof.ProofResolutionReason#EVIDENCE_MISSING:proof.ProofResolutionReason
        proof.ProofResolutionReason#EVIDENCE_PRESENT:proof.ProofResolutionReason
        proof.ProofResolutionReason#NOT_EVALUATED_AFTER_TERMINAL_OUTCOME:proof.ProofResolutionReason
        proof.ProofResolutionReason#OBSERVATION_ACTIVE:proof.ProofResolutionReason
        proof.ProofResolutionReason#OBSERVATION_FAILED:proof.ProofResolutionReason
        proof.ProofResolutionReason#OBSERVATION_LOST:proof.ProofResolutionReason
        proof.ProofResolutionReason#OBSERVATION_UNSUPPORTED:proof.ProofResolutionReason
        proof.ProofResolutionReason#PREREQUISITE_FAILED:proof.ProofResolutionReason
        proof.ProofResolutionReason#PREREQUISITE_SATISFIED:proof.ProofResolutionReason
        proof.ProofResolutionReason#PREREQUISITE_UNSUPPORTED:proof.ProofResolutionReason
        proof.ProofResolutionReason#STIMULUS_COMPLETED:proof.ProofResolutionReason
        proof.ProofResolutionReason#STIMULUS_FAILED:proof.ProofResolutionReason
        proof.ProofResolutionReason#STIMULUS_NOT_COMPLETED:proof.ProofResolutionReason
        proof.ProofStimulusState#COMPLETED:proof.ProofStimulusState
        proof.ProofStimulusState#FAILED:proof.ProofStimulusState
        proof.ProofStimulusState#NOT_STARTED:proof.ProofStimulusState
        proof.ProofStimulusState#RUNNING:proof.ProofStimulusState
        topology.PortDirection#PROVIDED:topology.PortDirection
        topology.PortDirection#REQUIRED:topology.PortDirection
        """);

    private static final Set<String> PACKAGE_DEPENDENCY_EDGES = lines("""
        component -> configuration
        component -> driver
        component -> topology
        control -> observation
        control -> proof
        control -> topology
        diagnostics -> component
        diagnostics -> environment.state
        diagnostics -> journal
        diagnostics -> observation
        diagnostics -> topology
        driver -> component
        driver -> configuration
        driver -> endpoint
        driver -> environment
        driver -> journal
        driver -> topology
        endpoint -> configuration
        environment -> communication
        environment -> component
        environment -> configuration
        environment -> control
        environment -> diagnostics
        environment -> driver
        environment -> endpoint
        environment -> environment.state
        environment -> journal
        environment -> observation
        environment -> proof
        environment -> topology
        environment.state -> observation
        environment.state -> topology
        journal -> component
        journal -> control
        journal -> environment.state
        journal -> observation
        journal -> proof
        journal -> topology
        observation -> topology
        proof -> observation
        proof -> control
        proof -> journal
        proof -> topology
        topology -> component
        """);

    @Test
    void shouldClassifyEveryExternallyVisibleTypeIncludingNestedTypes() throws IOException {
        assertPairwiseDisjoint(
            SUPPORTED_API,
            SUPPORTED_SPI,
            READ_ONLY_MODEL,
            JAVA_PUBLIC_INTERNAL
        );

        Set<String> classified = new TreeSet<>();
        classified.addAll(SUPPORTED_API);
        classified.addAll(SUPPORTED_SPI);
        classified.addAll(READ_ONLY_MODEL);
        classified.addAll(JAVA_PUBLIC_INTERNAL);

        Set<String> actual = externallyVisibleTypes();
        assertThat(actual).containsExactlyElementsOf(classified);
        assertThat(actual).anyMatch(name -> name.contains("$"));
        assertThat(JAVA_PUBLIC_INTERNAL).noneMatch(name -> name.contains("$"));
    }

    @Test
    void shouldPinEveryExternallyVisibleFieldAndConstant() throws IOException {
        assertThat(externallyVisibleFields()).containsExactlyInAnyOrderElementsOf(PUBLIC_FIELDS);
    }

    @Test
    void shouldPinTheActualPackageDependencyGraph() throws IOException {
        assertThat(packageDependencyEdges())
            .containsExactlyInAnyOrderElementsOf(PACKAGE_DEPENDENCY_EDGES);
        assertThat(PACKAGE_DEPENDENCY_EDGES)
            .doesNotContain(
                "diagnostics -> environment",
                "journal -> diagnostics"
            )
            .contains(
                "component -> driver",
                "driver -> component",
                "component -> topology",
                "topology -> component",
                "driver -> environment",
                "environment -> driver"
            );
    }

    @Test
    void shouldUseOnlyDomainOwnedTopLevelPackages() throws IOException {
        assertThat(topLevelPackageDirectories())
            .containsExactlyInAnyOrder(
                "communication",
                "component",
                "configuration",
                "control",
                "diagnostics",
                "driver",
                "endpoint",
                "environment",
                "journal",
                "observation",
                "proof",
                "topology"
            );
    }

    @Test
    void shouldKeepEnvironmentExecutionAndJournalMutationInternal() throws Exception {
        Class<?> runtime = loadType("environment.EnvironmentRuntime");
        assertThat(Modifier.isPublic(runtime.getModifiers())).isFalse();
        assertThat(runtime.getDeclaredConstructors())
            .allSatisfy(constructor ->
                assertThat(Modifier.isPrivate(constructor.getModifiers())).isTrue()
            );
        assertThat(externallyVisibleMethods(runtime)).isEmpty();

        Class<?> runtimeFactory = loadType("environment.EnvironmentRuntimeFactory");
        assertThat(Modifier.isPublic(runtimeFactory.getModifiers())).isFalse();
        assertThat(runtimeFactory.getDeclaredConstructors())
            .allSatisfy(constructor ->
                assertThat(Modifier.isPrivate(constructor.getModifiers())).isTrue()
            );
        assertThat(externallyVisibleMethods(runtimeFactory)).isEmpty();

        Class<?> storage = loadType("environment.ScenarioJournal");
        assertThat(Modifier.isPublic(storage.getModifiers())).isFalse();
        assertThat(externallyVisibleConstructors(storage)).isEmpty();
        assertThat(externallyVisibleMethods(storage)).isEmpty();

        assertThatThrownBy(() -> Class.forName(BASE_PACKAGE + "journal.ScenarioJournal"))
            .isInstanceOf(ClassNotFoundException.class);
        assertThat(classFiles("").stream()
            .map(CoreArchitectureTest::loadType)
            .filter(CoreArchitectureTest::isExternallyVisible)
            .flatMap(type -> Arrays.stream(type.getDeclaredMethods()))
            .filter(CoreArchitectureTest::isExternallyVisible)
            .filter(method -> method.getName().equals("append")
                || method.getName().equals("publish"))
            .filter(method -> Arrays.stream(method.getParameterTypes())
                .anyMatch(parameter -> parameter == ScenarioEvent.class
                    || parameter == JournalEntry.class)))
            .isEmpty();
    }

    @Test
    void shouldPinJavaPublicTechnicalBridgesAndSensitiveSupportedMembers() {
        assertThat(methodKeys(ConfigurationBinder.class))
            .containsExactly("bind(java.lang.Class,configuration.EnvironmentConfiguration):java.lang.Object");
        assertThat(methodKeys(ConfigurationValidator.class))
            .containsExactly("validate(java.lang.Object):java.lang.Object");
        assertThat(methodKeys(ConfigurationValues.class))
            .containsExactly(
                "requireNonNull(java.lang.Object,java.lang.String):java.lang.Object",
                "requireText(java.lang.String,java.lang.String):java.lang.String"
            );
        assertThat(methodKeys(RuntimeEndpointBindings.class))
            .containsExactly("publish(topology.ProvidedPort,endpoint.EndpointBinding):void");
        assertThat(List.of(
            ConfigurationBinder.class,
            ConfigurationValidator.class,
            ConfigurationValues.class,
            RuntimeEndpointBindings.class
        )).allSatisfy(type -> assertThat(externallyVisibleConstructors(type)).isEmpty());

        assertThat(methodKeys(AbstractComponent.class))
            .containsExactly(
                "castOperations(java.lang.Object):java.lang.Object",
                "configuration():configuration.RuntimeConfig",
                "driver():driver.ComponentDriver",
                "id():component.ComponentId",
                "ports():java.util.List",
                "type():component.ComponentType"
            );
        assertThat(methodKeys(ComponentPortFactory.class))
            .containsExactly(
                "provides(component.AbstractComponent,java.lang.String,topology.Contract,"
                    + "topology.InteractionSpec,topology.ProtocolSpec):topology.ProvidedPort",
                "requires(component.AbstractComponent,java.lang.String,topology.Contract,"
                    + "topology.InteractionSpec,topology.ProtocolSpec):topology.RequiredPort",
                "requiresAtStartup(component.AbstractComponent,java.lang.String,topology.Contract,"
                    + "topology.InteractionSpec,topology.ProtocolSpec):topology.RequiredPort"
            );
        assertThat(methodKeys(ComponentRuntime.class))
            .containsExactly(
                "close():void",
                "diagnostics():java.util.List",
                "materializes(topology.ProvidedPort):boolean",
                "operations():java.lang.Object",
                "publishBindingsTo(environment.RuntimeEndpointBindings):void",
                "runtime():driver.ComponentRuntime$Builder",
                "runtime(java.lang.AutoCloseable):driver.ComponentRuntime$Builder"
            );
        assertThat(methodKeys(InteractionDecisionCoordinator.class))
            .containsExactly(
                "observationFailed(topology.ConnectionId):void",
                "permit(observation.RecordedInteraction):observation.ForwardingPermit"
            );
        assertThat(methodKeys(EnvironmentTopology.class))
            .containsExactly(
                "components():java.util.List",
                "connection(topology.ConnectionId):topology.ConnectionRef",
                "connectionFrom(topology.RequiredPort):topology.ConnectionRef",
                "connections():java.util.List",
                "contains(component.Component):boolean",
                "equals(java.lang.Object):boolean",
                "hashCode():int",
                "of(java.util.List,java.util.List):environment.EnvironmentTopology",
                "toString():java.lang.String"
            )
            .doesNotContain("runtimeComponents():java.util.List");
        assertThat(Modifier.isFinal(EnvironmentTopology.class.getModifiers())).isTrue();
        assertThat(EnvironmentTopology.class.getDeclaredConstructors())
            .hasSize(1)
            .allSatisfy(constructor ->
                assertThat(Modifier.isPrivate(constructor.getModifiers())).isTrue()
            );
        assertThat(Arrays.stream(EnvironmentTopology.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> Modifier.isStatic(method.getModifiers()))
            .filter(method -> method.getReturnType() == EnvironmentTopology.class)
            .map(Method::getName))
            .containsExactly("of");
        assertThat(methodKeys(EnvironmentLogging.class))
            .containsExactly(
                "defaults():environment.EnvironmentLogging",
                "equals(java.lang.Object):boolean",
                "hashCode():int",
                "logs():environment.EnvironmentLoggingBuilder",
                "toString():java.lang.String"
            )
            .noneMatch(key -> key.startsWith("validateAgainst("));
        assertThat(externallyVisibleConstructors(EnvironmentLogging.class)).isEmpty();
        assertThat(methodKeys(EnvironmentLoggingBuilder.class))
            .containsExactly(
                "build():environment.EnvironmentLogging",
                "componentLevel(component.Component,journal.LogLevel):environment.EnvironmentLoggingBuilder",
                "connectionLevel(topology.RequiredPort,topology.ProvidedPort,journal.LogLevel):environment.EnvironmentLoggingBuilder",
                "defaultComponentLevel(journal.LogLevel):environment.EnvironmentLoggingBuilder",
                "defaultConnectionLevel(journal.LogLevel):environment.EnvironmentLoggingBuilder",
                "frameworkLevel(journal.LogLevel):environment.EnvironmentLoggingBuilder",
                "info(component.Component[]):environment.EnvironmentLoggingBuilder",
                "warnByDefault():environment.EnvironmentLoggingBuilder"
            );
        assertThat(externallyVisibleConstructors(EnvironmentLoggingBuilder.class)).hasSize(1);
        assertThat(methodKeys(JournalRenderer.class))
            .containsExactly(
                "render(journal.ScenarioJournalSnapshot):java.lang.String",
                "renderComponent(journal.ScenarioJournalSnapshot,component.ComponentId):java.lang.String",
                "renderLines(journal.JournalEntry):java.util.List"
            );
        assertThat(externallyVisibleConstructors(JournalRenderer.class)).hasSize(1);
        assertThat(methodKeys(EnvironmentDiagnostics.class))
            .containsExactly(
                "content():java.lang.String",
                "equals(java.lang.Object):boolean",
                "hashCode():int",
                "toString():java.lang.String"
            );
        assertThat(externallyVisibleConstructors(EnvironmentDiagnostics.class)).isEmpty();
        assertThat(ScenarioEvent.class.isSealed()).isFalse();
        assertThat(externallyVisibleConstructors(ComponentLifecycleException.class)).isEmpty();
    }

    @Test
    void shouldKeepRouteExecutionProofAllocationAndProviderLookupInternal()
        throws Exception {
        Class<?> selection = loadType("environment.ConnectionRouting$Selection");
        assertThat(isExternallyVisible(selection)).isFalse();
        assertThat(externallyVisibleConstructors(selection)).isEmpty();
        assertThat(externallyVisibleMethods(selection)).isEmpty();

        assertThat(methodKeys(ConnectionRouting.class))
            .allMatch(key -> key.startsWith("direct(")
                || key.startsWith("routed(")
                || key.startsWith("withRoute("));
        assertThat(methodKeys(ConnectionRoute.class))
            .allMatch(key -> key.startsWith("routed("));
        assertThat(methodKeys(ConnectionRouteContext.class))
            .extracting(key -> key.substring(0, key.indexOf('(')))
            .containsExactlyInAnyOrder(
                "connection",
                "observations",
                "observationRequirement",
                "requiredObservationProfile",
                "coordinator",
                "directTarget"
            );
        assertThat(methodKeys(CorrelationContribution.class))
            .extracting(key -> key.substring(0, key.indexOf('(')))
            .containsExactlyInAnyOrder(
                "capture",
                "key",
                "nativeReferenceSchema",
                "encodedSize",
                "equals",
                "hashCode",
                "toString"
            );

        assertThatThrownBy(() -> Class.forName(BASE_PACKAGE + "proof.ProofSubjectScope"))
            .isInstanceOf(ClassNotFoundException.class);
        assertThat(externallyVisibleMethods(
            RuntimeEndpointBindings.class,
            ConnectionRouting.class,
            ConnectionRoute.class
        )).noneMatch(method ->
            method.getName().equals("select")
                || method.getName().equals("prepare")
                || method.getName().equals("consumerTarget")
                || method.getReturnType().equals(EndpointBinding.class)
        );
    }

    @Test
    void shouldPinExplicitProofExecutionSurfaceAndInternalizeEvaluationState()
        throws Exception {
        Class<?> proofs = loadType("proof.Proofs");
        Class<?> execution = loadType("proof.ProofExecution");
        Class<?> plan = loadType("proof.ProofPlan");
        Class<?> builder = loadType("proof.ProofPlan$Builder");
        Class<?> result = loadType("proof.ProofResult");
        assertThat(methodKeys(proofs)).containsExactly(
            "activate(proof.ProofPlan):proof.ProofExecution",
            "failedPrerequisite(java.lang.Throwable):proof.ProofPrerequisite",
            "satisfiedPrerequisite():proof.ProofPrerequisite",
            "unsupportedPrerequisite():proof.ProofPrerequisite"
        );
        assertThat(methodKeys(execution)).containsExactly(
            "evaluate():proof.ProofResult",
            "result():proof.ProofResult",
            "runStimulus(java.lang.Runnable):void",
            "state():proof.ProofExecutionState"
        );
        assertThat(methodKeys(plan)).containsExactly(
            "builder(java.lang.String,java.lang.String,proof.ProofSubjectRef,java.time.Duration):proof.ProofPlan$Builder",
            "deadline():java.time.Duration",
            "id():proof.ProofPlanId",
            "primarySubject():proof.ProofSubjectRef",
            "requirements():java.util.List",
            "title():java.lang.String",
            "toString():java.lang.String"
        );
        assertThat(methodKeys(builder)).containsExactly(
            "build():proof.ProofPlan",
            "causalRelation(java.lang.String,control.SemanticPredecessorGuard):proof.ProofPlan$Builder",
            "control(java.lang.String,control.SemanticHold,control.SemanticHoldState):proof.ProofPlan$Builder",
            "control(java.lang.String,control.SemanticPredecessorGuard,control.SemanticPredecessorGuardState):proof.ProofPlan$Builder",
            "correlation(java.lang.String,topology.ConnectionId,proof.CorrelationKey,observation.EvidenceSchemaId):proof.ProofPlan$Builder",
            "evidence(java.lang.String,control.SemanticHold):proof.ProofPlan$Builder",
            "evidence(java.lang.String,control.SemanticPredecessorGuard,proof.ProofEvidenceKind):proof.ProofPlan$Builder",
            "observation(java.lang.String,topology.ConnectionId,observation.RequiredObservationProfile):proof.ProofPlan$Builder",
            "prerequisite(java.lang.String,proof.ProofPrerequisite):proof.ProofPlan$Builder"
        );
        assertThat(methodKeys(result)).containsExactly(
            "decisiveResolution():java.util.Optional",
            "evaluation():proof.ProofEvaluationResolution",
            "outcome():proof.ProofOutcome",
            "planId():proof.ProofPlanId",
            "primaryFailure():java.util.Optional",
            "primarySubject():proof.ProofSubjectRef",
            "report():proof.ProofReport",
            "require(proof.ProofOutcome):proof.ProofResult",
            "resolutions():java.util.List",
            "secondaryDiagnostics():java.util.List",
            "stimulus():proof.ProofStimulusResolution",
            "title():java.lang.String",
            "toString():java.lang.String",
            "unresolved():java.util.List"
        );
        assertThat(externallyVisibleMethods(proofs, execution, plan, builder, result))
            .allSatisfy(method -> {
                assertThat(method.getReturnType())
                    .isNotEqualTo(io.github.jacekkardys.systemproof.journal
                        .ScenarioJournalSnapshot.class);
                assertThat(method.getParameterTypes())
                    .doesNotContain(io.github.jacekkardys.systemproof.journal
                        .ScenarioJournalSnapshot.class);
            });

        assertThat(List.of(
            "environment.ProofExecutionCoordinator",
            "environment.ProofOutcomeEvaluator",
            "environment.ProofFactObserver",
            "environment.ProofObservationListener"
        )).allSatisfy(name -> assertThat(
            Modifier.isPublic(loadType(name).getModifiers())
        ).isFalse());
    }

    @Test
    void shouldKeepOneJournalStorageOwnerIndependentOfRenderingAndSlf4j()
        throws IOException {
        Path storage = CLASSES.resolve(BASE_PATH + "environment/ScenarioJournal.class");
        assertThat(readBytecode(storage))
            .doesNotContain(
                BASE_PATH + "diagnostics/",
                "org/slf4j/",
                "EnvironmentLogging"
            );

        assertThat(classFiles("environment").stream()
            .map(CoreArchitectureTest::loadType)
            .flatMap(type -> Arrays.stream(type.getDeclaredFields()))
            .filter(field -> Collection.class.isAssignableFrom(field.getType())
                || Map.class.isAssignableFrom(field.getType()))
            .filter(field -> {
                String fieldType = field.getGenericType().getTypeName();
                return fieldType.contains("journal.JournalEntry")
                    || fieldType.contains("journal.ScenarioEvent");
            }))
            .extracting(field -> field.getDeclaringClass().getName())
            .containsExactly(BASE_PACKAGE + "environment.ScenarioJournal");
    }

    @Test
    void shouldEnforceDependencyDirection() throws IOException {
        assertThat(classFiles(""))
            .allSatisfy(path -> assertThat(readBytecode(path))
                .doesNotContain(
                    "org/junit/",
                    "org/testcontainers/",
                    BASE_PATH + "junit/",
                    BASE_PATH + "testcontainers/"
                ));

        assertThat(classFiles("configuration"))
            .allSatisfy(path -> assertThat(readBytecode(path))
                .doesNotContain(
                    BASE_PATH + "component/",
                    BASE_PATH + "driver/",
                    BASE_PATH + "environment/"
                ));
        assertThat(classFiles("endpoint"))
            .allSatisfy(path -> assertThat(readBytecode(path))
                .doesNotContain(
                    BASE_PATH + "driver/",
                    BASE_PATH + "environment/"
                ));

        assertThat(classFiles("observation"))
            .allSatisfy(path -> assertThat(readBytecode(path))
                .doesNotContain(BASE_PATH + "proof/"));
        assertThat(classFiles("control"))
            .allSatisfy(path -> assertThat(readBytecode(path))
                .doesNotContain(
                    BASE_PATH + "environment/",
                    "org/junit/",
                    "org/testcontainers/"
                ));
        assertThat(classFiles("proof"))
            .anySatisfy(path -> assertThat(readBytecode(path))
                .contains(BASE_PATH + "observation/"));

        for (String packageName : List.of("journal", "diagnostics")) {
            assertThat(classFiles(packageName))
                .allSatisfy(path -> assertThat(readBytecode(path))
                    .doesNotContain(
                        BASE_PATH + "environment/EnvironmentExecution",
                        BASE_PATH + "environment/EnvironmentRuntime",
                        BASE_PATH + "environment/ScenarioJournal",
                        BASE_PATH + "environment/RuntimeConnectionRegistry",
                        BASE_PATH + "environment/ProofSubjectRegistry",
                        BASE_PATH + "engine/execution/"
                    ));
        }

        assertThat(classFiles("driver"))
            .allSatisfy(path -> assertThat(readBytecode(path))
                .doesNotContain(
                    "org/testcontainers/",
                    BASE_PATH + "testcontainers/"
                ));
    }

    private static Set<String> externallyVisibleTypes() throws IOException {
        return classFiles("").stream()
            .map(CoreArchitectureTest::loadType)
            .filter(CoreArchitectureTest::isExternallyVisible)
            .map(type -> type.getName().substring(BASE_PACKAGE.length()))
            .collect(Collectors.toCollection(TreeSet::new));
    }

    private static Set<String> externallyVisibleFields() throws IOException {
        return externallyVisibleTypes().stream()
            .map(CoreArchitectureTest::loadType)
            .flatMap(type -> Arrays.stream(type.getDeclaredFields()))
            .filter(CoreArchitectureTest::isExternallyVisible)
            .map(CoreArchitectureTest::fieldKey)
            .collect(Collectors.toCollection(TreeSet::new));
    }

    private static String fieldKey(Field field) {
        return shortTypeName(field.getDeclaringClass()) + "#" + field.getName() + ":"
            + shortTypeName(field.getType());
    }

    private static Set<String> packageDependencyEdges() throws IOException {
        List<Path> sources;
        try (Stream<Path> paths = Files.walk(SOURCES)) {
            sources = paths.filter(path -> path.toString().endsWith(".java")).toList();
        }

        Map<String, String> owners = new HashMap<>();
        for (Path source : sources) {
            if (source.getFileName().toString().equals("package-info.java")) {
                continue;
            }
            String packageName = packageName(source);
            String simpleName = source.getFileName().toString().replaceFirst("\\.java$", "");
            owners.put(packageName + "." + simpleName, packageName);
        }

        Set<String> edges = new TreeSet<>();
        for (Path source : sources) {
            String content = Files.readString(source);
            String sourcePackage = packageName(content);
            var imports = IMPORT_DECLARATION.matcher(content);
            while (imports.find()) {
                String importedName = imports.group(1);
                owners.entrySet().stream()
                    .filter(entry -> importedName.equals(entry.getKey())
                        || importedName.startsWith(entry.getKey() + "."))
                    .max(Map.Entry.comparingByKey((left, right) ->
                        Integer.compare(left.length(), right.length())))
                    .map(Map.Entry::getValue)
                    .filter(targetPackage -> !targetPackage.equals(sourcePackage))
                    .ifPresent(targetPackage -> edges.add(
                        relativePackage(sourcePackage) + " -> " + relativePackage(targetPackage)
                    ));
            }
        }
        return edges;
    }

    private static String packageName(Path source) throws IOException {
        return packageName(Files.readString(source));
    }

    private static String packageName(String source) {
        var declaration = PACKAGE_DECLARATION.matcher(source);
        if (!declaration.find()) {
            throw new IllegalStateException("Java source has no package declaration");
        }
        return declaration.group(1);
    }

    private static String relativePackage(String packageName) {
        if (!packageName.startsWith(BASE_PACKAGE)) {
            throw new IllegalArgumentException("Package is outside core: " + packageName);
        }
        return packageName.substring(BASE_PACKAGE.length());
    }

    private static Set<String> topLevelPackageDirectories() throws IOException {
        Path root = CLASSES.resolve(BASE_PATH);
        try (Stream<Path> paths = Files.list(root)) {
            return paths
                .filter(Files::isDirectory)
                .map(path -> path.getFileName().toString())
                .collect(Collectors.toCollection(TreeSet::new));
        }
    }

    private static Set<String> methodKeys(Class<?> type) {
        return Arrays.stream(type.getDeclaredMethods())
            .filter(CoreArchitectureTest::isExternallyVisible)
            .map(CoreArchitectureTest::methodKey)
            .collect(Collectors.toCollection(TreeSet::new));
    }

    private static String methodKey(Method method) {
        String parameters = Arrays.stream(method.getParameterTypes())
            .map(CoreArchitectureTest::shortTypeName)
            .collect(Collectors.joining(","));
        return method.getName() + "(" + parameters + "):"
            + shortTypeName(method.getReturnType());
    }

    private static String shortTypeName(Class<?> type) {
        if (type.isArray()) {
            return shortTypeName(type.getComponentType()) + "[]";
        }
        String name = type.getName();
        return name.startsWith(BASE_PACKAGE)
            ? name.substring(BASE_PACKAGE.length())
            : name;
    }

    private static List<Constructor<?>> externallyVisibleConstructors(Class<?> type) {
        return Arrays.stream(type.getDeclaredConstructors())
            .filter(CoreArchitectureTest::isExternallyVisible)
            .toList();
    }

    private static List<Method> externallyVisibleMethods(Class<?>... types) {
        return Arrays.stream(types)
            .flatMap(type -> Arrays.stream(type.getDeclaredMethods()))
            .filter(CoreArchitectureTest::isExternallyVisible)
            .toList();
    }

    private static boolean isExternallyVisible(java.lang.reflect.Member member) {
        return isExternallyVisible(member.getModifiers());
    }

    private static boolean isExternallyVisible(Class<?> type) {
        if (!isExternallyVisible(type.getModifiers())) {
            return false;
        }
        Class<?> enclosing = type.getEnclosingClass();
        return enclosing == null || isExternallyVisible(enclosing);
    }

    private static boolean isExternallyVisible(int modifiers) {
        return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
    }

    @SafeVarargs
    private static void assertPairwiseDisjoint(Set<String>... categories) {
        Set<String> seen = new HashSet<>();
        for (Set<String> category : categories) {
            assertThat(category).allSatisfy(type -> assertThat(seen.add(type)).isTrue());
        }
    }

    private static Set<String> types(String names) {
        return Arrays.stream(names.strip().split("\\s+"))
            .filter(name -> !name.isBlank())
            .collect(Collectors.toUnmodifiableSet());
    }

    private static Set<String> lines(String values) {
        return values.lines()
            .map(String::strip)
            .filter(value -> !value.isBlank())
            .collect(Collectors.toUnmodifiableSet());
    }

    private static List<Path> classFiles(String packageName) throws IOException {
        Path root = CLASSES.resolve(BASE_PATH + packageName.replace('.', '/'));
        try (Stream<Path> classes = Files.walk(root)) {
            return classes
                .filter(path -> path.toString().endsWith(".class"))
                .filter(path -> !path.getFileName().toString().equals("package-info.class"))
                .toList();
        }
    }

    private static Class<?> loadType(String relativeName) {
        try {
            return Class.forName(BASE_PACKAGE + relativeName);
        } catch (ClassNotFoundException exception) {
            throw new IllegalStateException("Cannot load type " + relativeName, exception);
        }
    }

    private static Class<?> loadType(Path path) {
        String binaryName = CLASSES.relativize(path).toString()
            .replace('/', '.')
            .replace('\\', '.');
        binaryName = binaryName.substring(0, binaryName.length() - ".class".length());
        try {
            return Class.forName(binaryName);
        } catch (ClassNotFoundException exception) {
            throw new IllegalStateException("Cannot load type " + binaryName, exception);
        }
    }

    private static String readBytecode(Path path) {
        try {
            return new String(Files.readAllBytes(path), StandardCharsets.ISO_8859_1);
        } catch (IOException exception) {
            throw new IllegalStateException("Cannot inspect " + path, exception);
        }
    }
}
