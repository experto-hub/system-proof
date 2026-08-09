package io.github.jacekkardys.systemproof.environment;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import io.github.jacekkardys.systemproof.diagnostics.JournalRenderer;
import io.github.jacekkardys.systemproof.journal.CheckpointEvent;
import io.github.jacekkardys.systemproof.journal.CheckpointId;
import io.github.jacekkardys.systemproof.journal.ComponentLifecycleEvent;
import io.github.jacekkardys.systemproof.journal.ConnectionLifecycleEvent;
import io.github.jacekkardys.systemproof.journal.CorrelationCandidateEvent;
import io.github.jacekkardys.systemproof.journal.DiagnosticEvent;
import io.github.jacekkardys.systemproof.journal.DisruptionId;
import io.github.jacekkardys.systemproof.journal.DisruptionLifecycleEvent;
import io.github.jacekkardys.systemproof.journal.EnvironmentLifecycleEvent;
import io.github.jacekkardys.systemproof.journal.FailureEvent;
import io.github.jacekkardys.systemproof.journal.FailureDetails;
import io.github.jacekkardys.systemproof.journal.InteractionObservationEvent;
import io.github.jacekkardys.systemproof.journal.JournalEntry;
import io.github.jacekkardys.systemproof.journal.ProofSubjectArmedEvent;
import io.github.jacekkardys.systemproof.journal.ProofSubjectCreatedEvent;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;
import io.github.jacekkardys.systemproof.journal.SemanticHoldEvent;
import io.github.jacekkardys.systemproof.journal.SemanticPredecessorGuardEvent;
import io.github.jacekkardys.systemproof.component.Component;
import io.github.jacekkardys.systemproof.component.ComponentState;
import io.github.jacekkardys.systemproof.environment.state.EnvironmentState;
import io.github.jacekkardys.systemproof.journal.LogLevel;
import io.github.jacekkardys.systemproof.journal.RedactedDiagnosticText;
import io.github.jacekkardys.systemproof.environment.state.ConnectionState;
import io.github.jacekkardys.systemproof.environment.state.RoutingMode;
import io.github.jacekkardys.systemproof.topology.ConnectionDescriptor;
import io.github.jacekkardys.systemproof.topology.ConnectionRef;
import io.github.jacekkardys.systemproof.observation.EvidenceSnapshot;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.proof.CorrelationCardinality;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.control.SemanticHoldFailure;
import io.github.jacekkardys.systemproof.control.SemanticHoldRef;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorBoundary;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardFailure;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardRef;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorViolation;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.EvidenceSchemaId;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/**
 * Builds and publishes framework-owned facts through the single environment journal.
 * Proof-critical publication commits journal storage, matching runtime state, and the typed proof
 * fact before attempting non-authoritative best-effort diagnostic emission. Non-fatal append
 * failures enter the proof journal-failure path; fatal JVM failures bypass it and propagate.
 */
final class EnvironmentEventPublisher {
    private final ScenarioJournal journal;
    private final JournalSlf4jEmitter emitter;
    private final ProofFactObserver proofFacts;

    EnvironmentEventPublisher(
        ScenarioJournal journal,
        EnvironmentLogging logging
    ) {
        this(
            journal,
            new JournalSlf4jEmitter(logging, new JournalRenderer()),
            ProofFactObserver.NONE
        );
    }

    EnvironmentEventPublisher(
        ScenarioJournal journal,
        JournalSlf4jEmitter emitter
    ) {
        this(journal, emitter, ProofFactObserver.NONE);
    }

    EnvironmentEventPublisher(
        ScenarioJournal journal,
        JournalSlf4jEmitter emitter,
        ProofFactObserver proofFacts
    ) {
        this.journal = Objects.requireNonNull(journal, "journal must not be null");
        this.emitter = Objects.requireNonNull(emitter, "emitter must not be null");
        this.proofFacts = Objects.requireNonNull(proofFacts, "proofFacts must not be null");
    }

    void environmentLifecycle(EnvironmentState state) {
        LogLevel level = state == EnvironmentState.FAILED ? LogLevel.ERROR : LogLevel.INFO;
        emitter.framework(append(new EnvironmentLifecycleEvent(state)), level);
    }

    void componentLifecycle(Component component, ComponentState state) {
        Objects.requireNonNull(component, "component must not be null");
        LogLevel level = state == ComponentState.FAILED ? LogLevel.ERROR : LogLevel.INFO;
        emitter.component(
            append(new ComponentLifecycleEvent(component.id(), state)),
            component,
            level
        );
    }

    void connectionLifecycle(
        ConnectionRef connection,
        ConnectionDescriptor descriptor,
        ConnectionState state,
        RoutingMode routingMode,
        boolean directTargetAvailable,
        boolean consumerTargetAvailable
    ) {
        Objects.requireNonNull(connection, "connection must not be null");
        LogLevel level = state == ConnectionState.FAILED ? LogLevel.ERROR : LogLevel.INFO;
        emitter.connection(
            append(new ConnectionLifecycleEvent(
                descriptor,
                state,
                routingMode,
                directTargetAvailable,
                consumerTargetAvailable
            )),
            connection,
            level
        );
    }

    void environmentStartupFailure(Throwable failure) {
        emitter.framework(
            append(new FailureEvent.EnvironmentStartup(FailureDetails.from(failure))),
            LogLevel.ERROR
        );
    }

    void componentStartupFailure(Component component, Throwable failure) {
        Objects.requireNonNull(component, "component must not be null");
        emitter.component(
            append(new FailureEvent.ComponentStartup(
                component.id(),
                FailureDetails.from(failure)
            )),
            component,
            LogLevel.ERROR
        );
    }

    void componentCleanupFailure(Component component, Throwable failure) {
        Objects.requireNonNull(component, "component must not be null");
        emitter.component(
            append(new FailureEvent.ComponentCleanup(
                component.id(),
                FailureDetails.from(failure)
            )),
            component,
            LogLevel.ERROR
        );
    }

    void connectionMaterializationFailure(ConnectionRef connection, Throwable failure) {
        Objects.requireNonNull(connection, "connection must not be null");
        emitter.connection(
            append(new FailureEvent.ConnectionMaterialization(
                connection.id(),
                FailureDetails.from(failure)
            )),
            connection,
            LogLevel.ERROR
        );
    }

    void connectionCleanupFailure(ConnectionRef connection, Throwable failure) {
        Objects.requireNonNull(connection, "connection must not be null");
        emitter.connection(
            append(new FailureEvent.ConnectionCleanup(
                connection.id(),
                FailureDetails.from(failure)
            )),
            connection,
            LogLevel.ERROR
        );
    }

    void driverResourceCleanupFailure(String resourceName, Throwable failure) {
        emitter.framework(
            append(new FailureEvent.DriverResourceCleanup(
                resourceName,
                FailureDetails.from(failure)
            )),
            LogLevel.ERROR
        );
    }

    void component(
        Component component,
        LogLevel level,
        RedactedDiagnosticText message
    ) {
        Objects.requireNonNull(component, "component must not be null");
        emitter.component(
            append(new DiagnosticEvent(
                new DiagnosticEvent.ComponentSubject(component.id()),
                level,
                message
            )),
            component,
            level
        );
    }

    void interaction(
        ConnectionRef connection,
        InteractionRef interactionRef,
        EvidenceSnapshot evidence
    ) {
        Objects.requireNonNull(connection, "connection must not be null");
        Objects.requireNonNull(interactionRef, "interactionRef must not be null");
        Objects.requireNonNull(evidence, "evidence must not be null");
        if (!connection.id().equals(interactionRef.connectionId())) {
            throw new IllegalArgumentException(
                "Interaction reference connection '" + interactionRef.connectionId()
                    + "' does not match bound connection '" + connection.id() + "'"
            );
        }
        emitter.connection(
            append(new InteractionObservationEvent(interactionRef, evidence)),
            connection,
            LogLevel.INFO
        );
    }

    void proofSubjectCreated(ProofSubjectRef proofSubject, Runnable stateCommit) {
        JournalEntry entry = commitProofCritical(
            new ProofSubjectCreatedEvent(proofSubject),
            stateCommit
        );
        emitFrameworkBestEffort(entry, LogLevel.INFO);
    }

    void proofSubjectArmed(
        ProofSubjectRef proofSubject,
        CorrelationKey key,
        boolean sharedKey,
        Runnable stateCommit
    ) {
        JournalEntry entry = commitProofCritical(
            new ProofSubjectArmedEvent(proofSubject, key, sharedKey),
            stateCommit
        );
        emitFrameworkBestEffort(entry, LogLevel.INFO);
    }

    void correlationCandidate(
        Optional<ProofSubjectRef> proofSubject,
        CorrelationKey key,
        InteractionRef interactionRef,
        EvidenceSnapshot nativeReference,
        CorrelationCardinality cardinality,
        Runnable stateCommit
    ) {
        JournalEntry entry = commitProofCritical(
            new CorrelationCandidateEvent(
                proofSubject,
                key,
                interactionRef,
                nativeReference,
                cardinality
            ),
            stateCommit
        );
        emitFrameworkBestEffort(
            entry,
            cardinality == CorrelationCardinality.AMBIGUOUS
                ? LogLevel.WARN
                : LogLevel.INFO
        );
    }

    void semanticHold(
        SemanticHoldRef holdRef,
        SemanticHoldState state,
        ConnectionId connectionId,
        FlowDirection direction,
        EvidenceSchemaId evidenceSchema,
        Optional<ProofSubjectRef> proofSubject,
        Optional<InteractionRef> interactionRef,
        Optional<SemanticHoldFailure> failure,
        Runnable stateCommit
    ) {
        LogLevel level = state == SemanticHoldState.FAILED
            ? LogLevel.WARN
            : LogLevel.INFO;
        JournalEntry entry = commitProofCritical(
            new SemanticHoldEvent(
                holdRef,
                state,
                connectionId,
                direction,
                evidenceSchema,
                proofSubject,
                interactionRef,
                failure
            ),
            stateCommit
        );
        emitFrameworkBestEffort(entry, level);
    }

    void semanticPredecessorGuard(
        SemanticPredecessorGuardRef guardRef,
        SemanticPredecessorGuardEvent.Kind kind,
        ProofSubjectRef proofSubject,
        SemanticPredecessorGuardState state,
        SemanticPredecessorBoundary requiredBoundary,
        Optional<InteractionRef> predecessor,
        Optional<InteractionRef> successor,
        Optional<ForwardingDecision> decision,
        Optional<SemanticPredecessorViolation> violation,
        Optional<SemanticPredecessorGuardFailure> failure,
        Runnable stateCommit
    ) {
        LogLevel level = state == SemanticPredecessorGuardState.VIOLATED
            || state == SemanticPredecessorGuardState.FAILED
            || kind == SemanticPredecessorGuardEvent.Kind.VIOLATION
                ? LogLevel.WARN
                : LogLevel.INFO;
        JournalEntry entry = commitProofCritical(
            new SemanticPredecessorGuardEvent(
                guardRef,
                kind,
                proofSubject,
                state,
                requiredBoundary,
                predecessor,
                successor,
                decision,
                violation,
                failure
            ),
            stateCommit
        );
        emitFrameworkBestEffort(entry, level);
    }

    <T> T proofFactBatch(Supplier<T> action) {
        return proofFacts.factBatch(action);
    }

    <T> T proofFactBatch(
        Supplier<T> action,
        Consumer<ProofFactObserver.FinalizationHandoff> handoffConsumer
    ) {
        return proofFacts.factBatch(action, handoffConsumer);
    }

    void checkpoint(
        Component component,
        CheckpointId checkpointId,
        CheckpointEvent.Kind kind,
        CheckpointEvent.Stage stage
    ) {
        Objects.requireNonNull(component, "component must not be null");
        emitter.component(
            append(new CheckpointEvent(component.id(), checkpointId, kind, stage)),
            component,
            LogLevel.INFO
        );
    }

    void disruption(
        Component component,
        DisruptionId disruptionId,
        DisruptionLifecycleEvent.Stage stage
    ) {
        Objects.requireNonNull(component, "component must not be null");
        LogLevel level = stage == DisruptionLifecycleEvent.Stage.FAILED
            ? LogLevel.WARN
            : LogLevel.INFO;
        emitter.component(
            append(new DisruptionLifecycleEvent(component.id(), disruptionId, stage)),
            component,
            level
        );
    }

    private JournalEntry append(ScenarioEvent event) {
        JournalEntry entry;
        try {
            entry = journal.append(event);
        } catch (RuntimeException | Error failure) {
            ProofFactObserver.rethrowFatalJvmFailure(failure);
            proofFacts.journalFailure(failure);
            throw failure;
        }
        proofFacts.fact(event);
        return entry;
    }

    /**
     * Stores one proof-critical event before committing its matching current-state mutation and
     * proof fact. Diagnostic rendering is deliberately outside that authoritative boundary.
     */
    private JournalEntry commitProofCritical(ScenarioEvent event, Runnable stateCommit) {
        JournalEntry entry;
        try {
            entry = journal.append(event);
        } catch (RuntimeException | Error failure) {
            ProofFactObserver.rethrowFatalJvmFailure(failure);
            proofFacts.journalFailure(failure);
            throw failure;
        }
        Objects.requireNonNull(stateCommit, "stateCommit must not be null").run();
        proofFacts.fact(event);
        return entry;
    }

    private void emitFrameworkBestEffort(JournalEntry entry, LogLevel level) {
        try {
            emitter.framework(entry, level);
        } catch (RuntimeException | Error failure) {
            ProofFactObserver.rethrowFatalJvmFailure(failure);
            // Stored proof facts and current state are authoritative; diagnostics are not.
        }
    }
}
