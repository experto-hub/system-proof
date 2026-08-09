package io.github.jacekkardys.systemproof.environment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import io.github.jacekkardys.systemproof.control.SemanticHoldFailure;
import io.github.jacekkardys.systemproof.control.SemanticHoldRef;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardFailure;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardRef;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.environment.state.RuntimeConnectionSnapshot;
import io.github.jacekkardys.systemproof.journal.CorrelationCandidateEvent;
import io.github.jacekkardys.systemproof.journal.FailureDetails;
import io.github.jacekkardys.systemproof.journal.FailureEvent;
import io.github.jacekkardys.systemproof.journal.ProofSubjectArmedEvent;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;
import io.github.jacekkardys.systemproof.journal.SemanticHoldEvent;
import io.github.jacekkardys.systemproof.journal.SemanticPredecessorGuardEvent;
import io.github.jacekkardys.systemproof.observation.EffectiveObservationStatus;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.proof.CorrelationCardinality;
import io.github.jacekkardys.systemproof.proof.ProofConfigurationException;
import io.github.jacekkardys.systemproof.proof.ProofDiagnostic;
import io.github.jacekkardys.systemproof.proof.ProofEvidenceKind;
import io.github.jacekkardys.systemproof.proof.ProofEvaluationResolution;
import io.github.jacekkardys.systemproof.proof.ProofEvaluationState;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofExecutionState;
import io.github.jacekkardys.systemproof.proof.ProofFailureStage;
import io.github.jacekkardys.systemproof.proof.ProofInteractionProvenance;
import io.github.jacekkardys.systemproof.proof.ProofObligationResolution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofPrerequisite;
import io.github.jacekkardys.systemproof.proof.ProofPrerequisiteStatus;
import io.github.jacekkardys.systemproof.proof.ProofRequirementKind;
import io.github.jacekkardys.systemproof.proof.ProofRequirementDescriptor;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.proof.ProofStimulusResolution;
import io.github.jacekkardys.systemproof.proof.ProofStimulusState;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/**
 * One environment-owned proof activation, typed current-state index, evaluator, and terminal
 * outcome linearization point.
 *
 * <p>The global nested order is semantic controls, the authoritative-operation boundary, proof
 * subjects, journal publication, then this proof-evaluation monitor. A participant may enter at a
 * later level, but never acquires an earlier one. The authoritative action and correlation
 * mutation run outside this monitor; only their detached complete fact batch enters proof state.
 * Completion delivery runs after every framework monitor and immutable-result gate is released.
 */
final class ProofExecutionCoordinator
    implements ProofFactObserver, ProofObservationListener {

    private static final int MAX_SECONDARY_DIAGNOSTICS = 32;
    private static final Comparator<ProofDiagnostic> DIAGNOSTIC_ORDER = Comparator
        .comparing((ProofDiagnostic value) -> value.stage().ordinal())
        .thenComparing(value -> value.failure().failureType());

    private final Object prerequisiteOwner = new Object();
    private final DeadlineScheduler deadlineScheduler;
    private final ProofOutcomeEvaluator outcomeEvaluator;
    private final BoundaryObserver boundaryObserver;
    private final Object authoritativeOperationBoundary = new Object();
    private final ThreadLocal<List<ScenarioEvent>> activeFactBatch = new ThreadLocal<>();
    private ProofSubjectRegistry proofSubjects;
    private SemanticControlCoordinator controls;
    private RuntimeConnectionRegistry connections;
    private final Set<ConnectionId> failedRequiredObservations = new LinkedHashSet<>();
    private ExecutionRecord execution;
    private boolean bound;
    private boolean closed;
    private boolean activationReserved;

    ProofExecutionCoordinator() {
        this(new SystemDeadlineScheduler(), ProofOutcomeEvaluator.failClosed());
    }

    ProofExecutionCoordinator(DeadlineScheduler deadlineScheduler) {
        this(deadlineScheduler, ProofOutcomeEvaluator.failClosed());
    }

    ProofExecutionCoordinator(
        DeadlineScheduler deadlineScheduler,
        ProofOutcomeEvaluator outcomeEvaluator
    ) {
        this(deadlineScheduler, outcomeEvaluator, BoundaryObserver.NONE);
    }

    ProofExecutionCoordinator(
        DeadlineScheduler deadlineScheduler,
        ProofOutcomeEvaluator outcomeEvaluator,
        BoundaryObserver boundaryObserver
    ) {
        this.deadlineScheduler = Objects.requireNonNull(
            deadlineScheduler,
            "deadlineScheduler must not be null"
        );
        this.outcomeEvaluator = Objects.requireNonNull(
            outcomeEvaluator,
            "outcomeEvaluator must not be null"
        );
        this.boundaryObserver = Objects.requireNonNull(
            boundaryObserver,
            "boundaryObserver must not be null"
        );
    }

    synchronized void bind(
        ProofSubjectRegistry proofSubjects,
        SemanticControlCoordinator controls,
        RuntimeConnectionRegistry connections
    ) {
        if (bound) {
            throw new IllegalStateException("Proof execution coordinator is already bound");
        }
        this.proofSubjects = Objects.requireNonNull(
            proofSubjects,
            "proofSubjects must not be null"
        );
        this.controls = Objects.requireNonNull(controls, "controls must not be null");
        this.connections = Objects.requireNonNull(connections, "connections must not be null");
        bound = true;
    }

    ProofPrerequisite prerequisite(ProofPrerequisiteStatus status, Throwable failure) {
        status = Objects.requireNonNull(status, "status must not be null");
        if ((status == ProofPrerequisiteStatus.FAILED) != (failure != null)) {
            throw new IllegalArgumentException(
                "Only a FAILED proof prerequisite requires a failure"
            );
        }
        return new RuntimeProofPrerequisite(
            prerequisiteOwner,
            status,
            failure == null ? Optional.empty() : Optional.of(FailureDetails.from(failure))
        );
    }

    ProofExecution activate(
        ProofPlan plan,
        ObservationRefresher refreshObservation
    ) {
        plan = Objects.requireNonNull(plan, "plan must not be null");
        refreshObservation = Objects.requireNonNull(
            refreshObservation,
            "refreshObservation must not be null"
        );
        synchronized (this) {
            requireBound();
            if (closed) {
                throw new IllegalStateException(
                    "Environment execution is complete and cannot activate a proof plan"
                );
            }
            if (execution != null || activationReserved) {
                throw new ProofConfigurationException(
                    "An environment execution accepts exactly one proof execution"
                );
            }
            activationReserved = true;
        }
        ControlMetadata controlMetadata;
        try {
            controlMetadata = captureControlMetadata(plan);
        } catch (RuntimeException | Error failure) {
            rethrowFatal(failure);
            synchronized (this) {
                activationReserved = false;
            }
            throw failure;
        }
        ExecutionRecord record;
        synchronized (this) {
            record = new ExecutionRecord(
                plan,
                this,
                refreshObservation,
                controlMetadata
            );
            execution = record;
            activationReserved = false;
            record.state = ProofExecutionState.ACTIVATING;
        }

        ActivationControls activationControls;
        try {
            activationControls = validateStaticPlan(record, controlMetadata);
        } catch (ProofConfigurationException failure) {
            discardInvalidExecution(record);
            throw failure;
        } catch (IllegalArgumentException failure) {
            discardInvalidExecution(record);
            throw new ProofConfigurationException(
                "Proof plan '" + plan.id()
                    + "' is incompatible with this environment execution"
            );
        } catch (RuntimeException | Error failure) {
            rethrowFatal(failure);
            complete(
                record,
                ProofOutcome.ERROR,
                new ProofDiagnostic(
                    ProofFailureStage.ACTIVATION,
                    FailureDetails.from(failure)
                )
            );
            return record.handle;
        }
        synchronized (this) {
            record.activationControls = activationControls;
        }

        try {
            seedPrerequisites(record);
        } catch (RuntimeException | Error failure) {
            rethrowFatal(failure);
            completeAndCancelPrepared(
                record,
                activationControls,
                ProofOutcome.ERROR,
                new ProofDiagnostic(
                    ProofFailureStage.ACTIVATION,
                    FailureDetails.from(failure)
                )
            );
            return record.handle;
        }
        RequirementState unsupported = first(record, ProofResolution.UNSUPPORTED);
        if (unsupported != null) {
            completeAndCancelPrepared(
                record,
                activationControls,
                ProofOutcome.INCONCLUSIVE,
                null
            );
            return record.handle;
        }
        RequirementState failedPrerequisite = first(record, ProofResolution.FAILED);
        if (failedPrerequisite != null) {
            completeAndCancelPrepared(
                record,
                activationControls,
                ProofOutcome.ERROR,
                new ProofDiagnostic(
                    ProofFailureStage.ACTIVATION,
                    prerequisiteFailure(failedPrerequisite.requirement)
                )
            );
            return record.handle;
        }

        try {
            joinObservationRefresh(
                refreshObservation.refresh(record.requiredObservationConnections)
            );
        } catch (RuntimeException | Error failure) {
            rethrowFatal(failure);
            completeAndCancelPrepared(
                record,
                activationControls,
                ProofOutcome.ERROR,
                new ProofDiagnostic(
                    ProofFailureStage.OBSERVATION,
                    FailureDetails.from(failure)
                )
            );
            return record.handle;
        }
        if (isComplete(record)) {
            return record.handle;
        }

        RequirementState unavailableObservation;
        try {
            unavailableObservation = seedObservations(record);
        } catch (RuntimeException | Error failure) {
            rethrowFatal(failure);
            completeAndCancelPrepared(
                record,
                activationControls,
                ProofOutcome.ERROR,
                new ProofDiagnostic(
                    ProofFailureStage.OBSERVATION,
                    FailureDetails.from(failure)
                )
            );
            return record.handle;
        }
        if (unavailableObservation != null) {
            if (unavailableObservation.resolution == ProofResolution.FAILED) {
                completeAndCancelPrepared(
                    record,
                    activationControls,
                    ProofOutcome.ERROR,
                    diagnostic(ProofFailureStage.OBSERVATION, new ObservationFailure())
                );
            } else {
                completeAndCancelPrepared(
                    record,
                    activationControls,
                    ProofOutcome.INCONCLUSIVE,
                    null
                );
            }
            return record.handle;
        }

        synchronized (this) {
            seedActiveObligations(record, activationControls);
        }
        try {
            controls.activatePreparedControls(
                activationControls.holds,
                activationControls.guards,
                () -> activateAtControlBoundary(record)
            );
        } catch (RuntimeException | Error failure) {
            rethrowFatal(failure);
            complete(
                record,
                ProofOutcome.ERROR,
                new ProofDiagnostic(
                    ProofFailureStage.ACTIVATION,
                    FailureDetails.from(failure)
                )
            );
            return record.handle;
        }
        if (isComplete(record)) {
            return record.handle;
        }

        synchronized (this) {
            if (record.outcome != null) {
                return record.handle;
            }
            record.deadlineInstalling = true;
        }
        try {
            DeadlineTask scheduled = deadlineScheduler.schedule(
                record.plan.deadline(),
                () -> deadlineExpired(record)
            );
            synchronized (this) {
                record.deadlineTask = scheduled;
                record.deadlineInstalling = false;
            }
            finalizePending(record);
        } catch (RuntimeException | Error failure) {
            rethrowFatal(failure);
            synchronized (this) {
                record.deadlineInstalling = false;
            }
            complete(
                record,
                ProofOutcome.ERROR,
                new ProofDiagnostic(
                    ProofFailureStage.ACTIVATION,
                    FailureDetails.from(failure)
                )
            );
        }
        return record.handle;
    }

    private Predicate<InteractionRef> activateAtControlBoundary(ExecutionRecord record) {
        ProofEvidenceWindowTracker.EvidenceWindow evidenceWindow =
            connections.openProofEvidenceWindow(window -> {
                boundaryObserver.evidenceWindowCaptured();
                synchronized (this) {
                    if (record.state == ProofExecutionState.ACTIVATING) {
                        record.evidenceWindow = window;
                        record.state = ProofExecutionState.ACTIVE;
                        record.activationReached = true;
                    }
                }
            });
        return interaction -> connections.isWithinProofEvidenceWindow(
            evidenceWindow,
            interaction
        );
    }

    @Override
    public void fact(ScenarioEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        List<ScenarioEvent> batch = activeFactBatch.get();
        if (batch != null) {
            batch.add(event);
            return;
        }
        synchronized (authoritativeOperationBoundary) {
            applyFacts(List.of(event), false);
        }
    }

    @Override
    public <T> T factBatch(Supplier<T> action) {
        action = Objects.requireNonNull(action, "action must not be null");
        if (activeFactBatch.get() != null) {
            return action.get();
        }

        boundaryObserver.beforeAuthoritativeOperationBoundary();
        ExecutionRecord[] selected = new ExecutionRecord[1];
        try {
            synchronized (authoritativeOperationBoundary) {
                List<ScenarioEvent> batch = new ArrayList<>();
                activeFactBatch.set(batch);
                try {
                    return action.get();
                } finally {
                    activeFactBatch.remove();
                    selected[0] = applyFacts(batch, true);
                }
            }
        } finally {
            if (selected[0] != null) {
                try {
                    boundaryObserver.authoritativeOutcomeSelectedBeforeFinalization();
                } finally {
                    synchronized (this) {
                        selected[0].authoritativeOutcomeBoundaryPending = false;
                    }
                }
            }
        }
    }

    private ExecutionRecord applyFacts(
        List<ScenarioEvent> events,
        boolean authoritativeBatch
    ) {
        synchronized (this) {
            ExecutionRecord record = execution;
            ExecutionRecord selected = null;
            boolean batchOpened = authoritativeBatch
                && record != null
                && record.outcome == null
                && record.state != ProofExecutionState.COMPLETED;
            if (batchOpened) {
                record.factBatchActive = true;
            }
            try {
                for (ScenarioEvent event : events) {
                    PendingCompletion decisiveBefore = record == null
                        ? null
                        : record.pendingCompletion;
                    List<RequirementStateSnapshot> statesBefore = decisiveBefore == null
                        ? List.of()
                        : snapshotStates(record);
                    int secondaryBefore = record == null
                        ? 0
                        : record.secondaryDiagnostics.size();
                    try {
                        applyFactLocked(event);
                    } catch (RuntimeException | Error evaluatorFailure) {
                        rethrowFatal(evaluatorFailure);
                        if (execution != null && execution.outcome == null) {
                            completeLocked(
                                execution,
                                ProofOutcome.ERROR,
                                new ProofDiagnostic(
                                    ProofFailureStage.EVALUATION,
                                    FailureDetails.from(evaluatorFailure)
                                )
                            );
                        }
                    }
                    if (decisiveBefore != null
                        && !compatibleWith(
                            decisiveBefore.outcome(),
                            record.states
                        )) {
                        restoreStates(record, statesBefore);
                        record.pendingCompletion = decisiveBefore;
                        if (record.secondaryDiagnostics.size() == secondaryBefore) {
                            retainIncompatibleBatchFact(record, event);
                        }
                    }
                }
            } finally {
                if (batchOpened) {
                    if (completeFactBatchLocked(record)) {
                        record.authoritativeOutcomeBoundaryPending = true;
                        selected = record;
                    }
                }
            }
            return selected;
        }
    }

    private void applyFactLocked(ScenarioEvent event) {
        if (execution == null || execution.state == ProofExecutionState.COMPLETED) {
            return;
        }
        if (execution.outcome != null) {
            retainSecondary(execution, event);
            return;
        }
        if (execution.state == ProofExecutionState.ACTIVATING) {
            if (event instanceof FailureEvent failure && relevantFailure(execution, failure)) {
                completeLocked(
                    execution,
                    ProofOutcome.ERROR,
                    new ProofDiagnostic(failureStage(failure), failure.failure())
                );
            }
            return;
        }
        if (execution.state != ProofExecutionState.ACTIVE) {
            return;
        }
        switch (event) {
            case CorrelationCandidateEvent candidate ->
                applyCorrelation(execution, candidate);
            case ProofSubjectArmedEvent armed ->
                applyCorrelationInvalidation(execution, armed);
            case SemanticHoldEvent hold -> applyHold(execution, hold);
            case SemanticPredecessorGuardEvent guard -> applyGuard(execution, guard);
            case FailureEvent failure -> {
                if (relevantFailure(execution, failure)) {
                    completeLocked(
                        execution,
                        ProofOutcome.ERROR,
                        new ProofDiagnostic(failureStage(failure), failure.failure())
                    );
                }
            }
            default -> {
                // Unrelated typed journal facts do not affect proof current state.
            }
        }
    }

    @Override
    public void journalFailure(Throwable failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        synchronized (authoritativeOperationBoundary) {
            journalFailureAtBoundary(failure);
        }
    }

    private void journalFailureAtBoundary(Throwable failure) {
        synchronized (this) {
            if (execution == null) {
                return;
            }
            if (execution.state == ProofExecutionState.COMPLETED) {
                return;
            }
            if (execution.outcome != null) {
                addSecondary(
                    execution,
                    new ProofDiagnostic(ProofFailureStage.JOURNAL, FailureDetails.from(failure))
                );
                return;
            }
            completeLocked(
                execution,
                ProofOutcome.ERROR,
                new ProofDiagnostic(ProofFailureStage.JOURNAL, FailureDetails.from(failure))
            );
        }
    }

    @Override
    public void observationChanged(RuntimeConnectionSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        synchronized (authoritativeOperationBoundary) {
            observationChangedAtBoundary(snapshot);
        }
    }

    private void observationChangedAtBoundary(RuntimeConnectionSnapshot snapshot) {
        synchronized (this) {
            if (execution == null) {
                return;
            }
            if (execution.state == ProofExecutionState.COMPLETED) {
                return;
            }
            if (execution.outcome != null) {
                if (observationState(execution, snapshot.id()) != null
                    && snapshot.effectiveObservationStatus()
                    != EffectiveObservationStatus.ACTIVE) {
                    addSecondary(
                        execution,
                        diagnostic(ProofFailureStage.OBSERVATION, new ObservationFailure())
                    );
                }
                return;
            }
            if (execution.state != ProofExecutionState.ACTIVE
                && execution.state != ProofExecutionState.ACTIVATING) {
                return;
            }
            RequirementState observation = execution.observations.get(snapshot.id());
            if (observation == null || snapshot.effectiveObservationStatus()
                == EffectiveObservationStatus.ACTIVE) {
                return;
            }
            switch (snapshot.effectiveObservationStatus()) {
                case FAILED, DISABLED, PENDING -> {
                    observation.set(
                        ProofResolution.FAILED,
                        ProofResolutionReason.OBSERVATION_FAILED,
                        Optional.of(snapshot.id()),
                        List.of()
                    );
                    completeLocked(
                        execution,
                        ProofOutcome.ERROR,
                        diagnostic(ProofFailureStage.OBSERVATION, new ObservationFailure())
                    );
                }
                case UNSUPPORTED -> {
                    observation.set(
                        ProofResolution.UNSUPPORTED,
                        ProofResolutionReason.OBSERVATION_UNSUPPORTED,
                        Optional.of(snapshot.id()),
                        List.of()
                    );
                    completeLocked(execution, ProofOutcome.INCONCLUSIVE, null);
                }
                case DEGRADED, INACTIVE -> {
                    observation.set(
                        ProofResolution.MISSING,
                        ProofResolutionReason.OBSERVATION_LOST,
                        Optional.of(snapshot.id()),
                        List.of()
                    );
                    completeLocked(execution, ProofOutcome.INCONCLUSIVE, null);
                }
                case ACTIVE -> throw new IllegalStateException("ACTIVE was handled earlier");
            }
        }
    }

    @Override
    public void requiredObservationFailed(ConnectionId connectionId) {
        connectionId = Objects.requireNonNull(
            connectionId,
            "connectionId must not be null"
        );
        synchronized (authoritativeOperationBoundary) {
            requiredObservationFailedAtBoundary(connectionId);
        }
    }

    private void requiredObservationFailedAtBoundary(ConnectionId connectionId) {
        synchronized (this) {
            failedRequiredObservations.add(connectionId);
            if (execution == null) {
                return;
            }
            if (execution.state == ProofExecutionState.COMPLETED) {
                return;
            }
            if (execution.outcome != null) {
                if (observationState(execution, connectionId) != null) {
                    addSecondary(
                        execution,
                        diagnostic(ProofFailureStage.OBSERVATION, new ObservationFailure())
                    );
                }
                return;
            }
            RequirementState observation = observationState(
                execution,
                connectionId
            );
            if (observation == null) {
                return;
            }
            observation.set(
                ProofResolution.FAILED,
                ProofResolutionReason.OBSERVATION_FAILED,
                Optional.of(connectionId),
                List.of()
            );
            completeLocked(
                execution,
                ProofOutcome.ERROR,
                diagnostic(
                    ProofFailureStage.OBSERVATION,
                    new ObservationFailure()
                )
            );
        }
    }

    @Override
    public void finalizePending() {
        ExecutionRecord record;
        synchronized (this) {
            record = execution;
        }
        if (record != null) {
            finalizePending(record);
        }
    }

    Throwable completeExecution() {
        Throwable unfinished = null;
        synchronized (this) {
            if (closed) {
                return null;
            }
            closed = true;
            if (execution != null && execution.state != ProofExecutionState.COMPLETED
                && execution.outcome == null) {
                completeLocked(
                    execution,
                    ProofOutcome.ERROR,
                    diagnostic(ProofFailureStage.TEARDOWN, new UnfinishedProofExecution())
                );
                unfinished = new IllegalStateException(
                    "Environment closed with an unfinished active proof execution"
                );
            }
        }
        if (execution != null) {
            finalizePending(execution);
        }
        try {
            deadlineScheduler.close();
        } catch (RuntimeException | Error failure) {
            rethrowFatal(failure);
            if (execution != null) {
                addSecondarySafely(
                    execution,
                    diagnostic(ProofFailureStage.CLEANUP, failure)
                );
            }
        }
        return unfinished;
    }

    private ControlMetadata captureControlMetadata(ProofPlan plan) {
        Map<SemanticHoldRef, SemanticControlCoordinator.HoldDeclaration> holds =
            new HashMap<>();
        Map<SemanticPredecessorGuardRef, SemanticControlCoordinator.GuardDeclaration> guards =
            new HashMap<>();
        for (ProofPlan.Requirement requirement : plan.requirements()) {
            switch (requirement) {
                case ProofPlan.HoldControl value -> holds.computeIfAbsent(
                    value.holdRef(),
                    controls::holdDeclaration
                );
                case ProofPlan.HoldEvidence value -> holds.computeIfAbsent(
                    value.holdRef(),
                    controls::holdDeclaration
                );
                case ProofPlan.GuardControl value -> guards.computeIfAbsent(
                    value.guardRef(),
                    controls::guardDeclaration
                );
                case ProofPlan.GuardEvidence value -> guards.computeIfAbsent(
                    value.guardRef(),
                    controls::guardDeclaration
                );
                case ProofPlan.CausalRelation value -> guards.computeIfAbsent(
                    value.guardRef(),
                    controls::guardDeclaration
                );
                default -> {
                    // Non-control descriptors are completely plan-owned.
                }
            }
        }
        return new ControlMetadata(holds, guards);
    }

    private ActivationControls validateStaticPlan(
        ExecutionRecord record,
        ControlMetadata controlMetadata
    ) {
        proofSubjects.validateSubject(record.plan.primarySubject());
        Map<ConnectionId, ProofPlan.Observation> observations = new LinkedHashMap<>();
        List<SemanticHoldRef> holds = new ArrayList<>();
        List<ConnectionId> holdConnections = new ArrayList<>();
        List<SemanticPredecessorGuardRef> guards = new ArrayList<>();

        for (ProofPlan.Requirement requirement : record.plan.requirements()) {
            switch (requirement) {
                case ProofPlan.Prerequisite prerequisite ->
                    requirePrerequisite(prerequisite.prerequisite());
                case ProofPlan.Observation observation -> {
                    connections.validateProofObservation(
                        observation.connectionId(),
                        observation.profile()
                    );
                    observations.put(observation.connectionId(), observation);
                }
                case ProofPlan.Correlation correlation -> {
                    proofSubjects.validateSubjectFlow(
                        record.plan.primarySubject(),
                        correlation.key()
                    );
                    connections.validateProofCorrelation(
                        correlation.connectionId(),
                        correlation.nativeReferenceSchema()
                    );
                }
                case ProofPlan.HoldControl control -> {
                    SemanticControlCoordinator.HoldDeclaration declaration =
                        controlMetadata.hold(control.holdRef());
                    if (declaration.state() != SemanticHoldState.DECLARED
                        || declaration.proofSubject()
                            .filter(record.plan.primarySubject()::equals).isEmpty()) {
                        throw new ProofConfigurationException(
                            "Required semantic hold must be DECLARED for the primary subject"
                        );
                    }
                    holds.add(control.holdRef());
                    holdConnections.add(declaration.connectionId());
                }
                case ProofPlan.GuardControl control -> {
                    SemanticControlCoordinator.GuardDeclaration declaration =
                        controlMetadata.guard(control.guardRef());
                    if (declaration.state() != SemanticPredecessorGuardState.DECLARED
                        || !declaration.subject().equals(record.plan.primarySubject())) {
                        throw new ProofConfigurationException(
                            "Required predecessor guard must be DECLARED for the primary subject"
                        );
                    }
                    guards.add(control.guardRef());
                }
                case ProofPlan.HoldEvidence evidence -> controlMetadata.hold(
                    evidence.holdRef()
                );
                case ProofPlan.GuardEvidence evidence -> controlMetadata.guard(
                    evidence.guardRef()
                );
                case ProofPlan.CausalRelation relation -> controlMetadata.guard(
                    relation.guardRef()
                );
            }
        }

        for (ProofPlan.Requirement requirement : record.plan.requirements()) {
            switch (requirement) {
                case ProofPlan.Correlation correlation -> requireObservation(
                    observations,
                    correlation.connectionId(),
                    correlation.id().toString()
                );
                case ProofPlan.HoldControl control -> requireObservation(
                    observations,
                    controlMetadata.hold(control.holdRef()).connectionId(),
                    control.id().toString()
                );
                case ProofPlan.GuardControl control -> {
                    SemanticControlCoordinator.GuardDeclaration declaration =
                        controlMetadata.guard(control.guardRef());
                    requireObservation(
                        observations,
                        declaration.predecessorConnectionId(),
                        control.id().toString()
                    );
                    requireObservation(
                        observations,
                        declaration.successorConnectionId(),
                        control.id().toString()
                    );
                }
                default -> {
                    // Other requirements refer to already validated controls or prerequisites.
                }
            }
        }
        return new ActivationControls(
            List.copyOf(holds),
            List.copyOf(holdConnections),
            List.copyOf(guards)
        );
    }

    private void seedPrerequisites(ExecutionRecord record) {
        synchronized (this) {
            for (RequirementState state : record.states) {
                if (!(state.requirement instanceof ProofPlan.Prerequisite requirement)) {
                    continue;
                }
                RuntimeProofPrerequisite prerequisite = requirePrerequisite(
                    requirement.prerequisite()
                );
                switch (prerequisite.status) {
                    case SATISFIED -> state.set(
                        ProofResolution.SATISFIED,
                        ProofResolutionReason.PREREQUISITE_SATISFIED,
                        Optional.empty(),
                        List.of()
                    );
                    case UNSUPPORTED -> state.set(
                        ProofResolution.UNSUPPORTED,
                        ProofResolutionReason.PREREQUISITE_UNSUPPORTED,
                        Optional.empty(),
                        List.of()
                    );
                    case FAILED -> state.set(
                        ProofResolution.FAILED,
                        ProofResolutionReason.PREREQUISITE_FAILED,
                        Optional.empty(),
                        List.of()
                    );
                }
            }
        }
    }

    private RequirementState seedObservations(ExecutionRecord record) {
        List<ObservationSeed> seeds = new ArrayList<>();
        for (RequirementState state : record.states) {
            if (state.requirement instanceof ProofPlan.Observation observation) {
                seeds.add(new ObservationSeed(
                    state,
                    connections.snapshot(observation.connectionId())
                ));
            }
        }
        synchronized (this) {
            for (ObservationSeed seed : seeds) {
                RequirementState state = seed.state();
                RuntimeConnectionSnapshot snapshot = seed.snapshot();
                if (failedRequiredObservations.contains(snapshot.id())) {
                    state.set(
                        ProofResolution.FAILED,
                        ProofResolutionReason.OBSERVATION_FAILED,
                        Optional.of(snapshot.id()),
                        List.of()
                    );
                    record.observations.put(snapshot.id(), state);
                    return state;
                }
                switch (snapshot.effectiveObservationStatus()) {
                    case ACTIVE -> state.set(
                        ProofResolution.SATISFIED,
                        ProofResolutionReason.OBSERVATION_ACTIVE,
                        Optional.of(snapshot.id()),
                        List.of()
                    );
                    case UNSUPPORTED -> state.set(
                        ProofResolution.UNSUPPORTED,
                        ProofResolutionReason.OBSERVATION_UNSUPPORTED,
                        Optional.of(snapshot.id()),
                        List.of()
                    );
                    case DEGRADED, INACTIVE -> state.set(
                        ProofResolution.MISSING,
                        ProofResolutionReason.OBSERVATION_LOST,
                        Optional.of(snapshot.id()),
                        List.of()
                    );
                    case DISABLED, PENDING, FAILED -> state.set(
                        ProofResolution.FAILED,
                        ProofResolutionReason.OBSERVATION_FAILED,
                        Optional.of(snapshot.id()),
                        List.of()
                    );
                }
                record.observations.put(snapshot.id(), state);
                if (state.resolution != ProofResolution.SATISFIED) {
                    return state;
                }
            }
            return null;
        }
    }

    private void seedActiveObligations(
        ExecutionRecord record,
        ActivationControls activationControls
    ) {
        for (RequirementState state : record.states) {
            switch (state.requirement) {
                case ProofPlan.Prerequisite ignored -> {
                    // Seeded before the evidence window.
                }
                case ProofPlan.Observation ignored -> {
                    // Seeded before the evidence window.
                }
                case ProofPlan.Correlation correlation -> {
                    state.set(
                        ProofResolution.MISSING,
                        ProofResolutionReason.CORRELATION_MISSING,
                        Optional.of(correlation.connectionId()),
                        List.of()
                    );
                    record.correlations.add(state);
                }
                case ProofPlan.HoldControl control -> {
                    state.set(
                        ProofResolution.UNREACHED,
                        ProofResolutionReason.CONTROL_UNREACHED,
                        Optional.of(activationControls.connectionFor(control.holdRef())),
                        List.of()
                    );
                    record.holdControls.put(control.holdRef(), state);
                }
                case ProofPlan.GuardControl control -> {
                    state.set(
                        ProofResolution.UNREACHED,
                        ProofResolutionReason.CONTROL_UNREACHED,
                        Optional.empty(),
                        List.of()
                    );
                    record.guardControls.put(control.guardRef(), state);
                }
                case ProofPlan.HoldEvidence evidence -> {
                    ProofRequirementDescriptor.HoldEvidence descriptor =
                        (ProofRequirementDescriptor.HoldEvidence) state.descriptor;
                    state.set(
                        ProofResolution.MISSING,
                        ProofResolutionReason.EVIDENCE_MISSING,
                        Optional.of(descriptor.connectionId()),
                        List.of()
                    );
                    record.holdEvidence.put(evidence.holdRef(), state);
                }
                case ProofPlan.GuardEvidence evidence -> {
                    ProofRequirementDescriptor.GuardEvidence descriptor =
                        (ProofRequirementDescriptor.GuardEvidence) state.descriptor;
                    state.set(
                        ProofResolution.MISSING,
                        ProofResolutionReason.EVIDENCE_MISSING,
                        Optional.of(descriptor.connectionId()),
                        List.of()
                    );
                    record.guardEvidence.computeIfAbsent(
                        evidence.guardRef(),
                        ignored -> new HashMap<>()
                    ).put(evidence.evidenceKind(), state);
                }
                case ProofPlan.CausalRelation relation -> {
                    state.set(
                        ProofResolution.UNREACHED,
                        ProofResolutionReason.CAUSAL_RELATION_UNREACHED,
                        Optional.empty(),
                        List.of()
                    );
                    record.relations.put(relation.guardRef(), state);
                }
            }
        }
    }

    private void applyCorrelation(ExecutionRecord record, CorrelationCandidateEvent event) {
        if (!isWithinEvidenceWindow(record, event.interactionRef())) {
            return;
        }
        for (RequirementState state : record.correlations) {
            ProofPlan.Correlation correlation = (ProofPlan.Correlation) state.requirement;
            if (!correlation.key().equals(event.key())
                || !correlation.connectionId().equals(event.interactionRef().connectionId())
                || !correlation.nativeReferenceSchema().equals(
                    event.nativeReference().schemaId()
                )) {
                continue;
            }
            switch (event.cardinality()) {
                case UNIQUE -> {
                    if (event.proofSubject().filter(record.plan.primarySubject()::equals).isPresent()) {
                        record.acceptedCorrelations.put(state, event.interactionRef());
                        state.set(
                            ProofResolution.SATISFIED,
                            ProofResolutionReason.CORRELATION_UNIQUE,
                            Optional.of(correlation.connectionId()),
                            List.of(ProofInteractionProvenance.correlation(
                                event.interactionRef()
                            ))
                        );
                    }
                }
                case AMBIGUOUS -> state.set(
                    ProofResolution.AMBIGUOUS,
                    ProofResolutionReason.CORRELATION_AMBIGUOUS,
                    Optional.of(correlation.connectionId()),
                    List.of()
                );
                case MISSING -> state.set(
                    ProofResolution.MISSING,
                    ProofResolutionReason.CORRELATION_MISSING,
                    Optional.of(correlation.connectionId()),
                    List.of()
                );
            }
        }
    }

    private void applyCorrelationInvalidation(
        ExecutionRecord record,
        ProofSubjectArmedEvent event
    ) {
        if (!event.sharedKey()) {
            return;
        }
        for (RequirementState state : record.correlations) {
            ProofPlan.Correlation correlation = (ProofPlan.Correlation) state.requirement;
            if (correlation.key().equals(event.key())) {
                state.set(
                    ProofResolution.AMBIGUOUS,
                    ProofResolutionReason.CORRELATION_AMBIGUOUS,
                    Optional.of(correlation.connectionId()),
                    List.of()
                );
            }
        }
    }

    private void applyHold(ExecutionRecord record, SemanticHoldEvent event) {
        if (event.interactionRef()
            .filter(reference -> !isWithinEvidenceWindow(record, reference))
            .isPresent()) {
            return;
        }
        RequirementState control = record.holdControls.get(event.holdRef());
        RequirementState evidence = record.holdEvidence.get(event.holdRef());
        if (control == null && evidence == null) {
            return;
        }
        List<ProofInteractionProvenance> provenance = event.interactionRef()
            .map(ProofInteractionProvenance::hold)
            .stream()
            .toList();
        if (evidence != null) {
            if (event.interactionRef().isPresent()) {
                evidence.set(
                    ProofResolution.SATISFIED,
                    ProofResolutionReason.EVIDENCE_PRESENT,
                    Optional.of(event.connectionId()),
                    provenance
                );
            } else if (isPreReachTerminal(event)) {
                evidence.set(
                    ProofResolution.MISSING,
                    ProofResolutionReason.EVIDENCE_MISSING,
                    Optional.of(event.connectionId()),
                    List.of()
                );
            }
        }
        if (control == null) {
            return;
        }
        switch (event.state()) {
            case DECLARED, ARMED, REACHED_HELD, RELEASING -> {
                // Still unresolved; evidence may already be present.
            }
            case FORWARDED -> control.set(
                ProofResolution.SATISFIED,
                ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE,
                Optional.of(event.connectionId()),
                provenance
            );
            case CANCELLED -> control.set(
                ProofResolution.UNREACHED,
                ProofResolutionReason.CONTROL_UNREACHED,
                Optional.of(event.connectionId()),
                provenance
            );
            case TIMED_OUT -> {
                control.set(
                    ProofResolution.TIMED_OUT,
                    ProofResolutionReason.CONTROL_TIMED_OUT,
                    Optional.of(event.connectionId()),
                    provenance
                );
                completeLocked(record, ProofOutcome.INCONCLUSIVE, null);
            }
            case FAILED -> applyHoldFailure(record, control, event, provenance);
        }
    }

    private void applyHoldFailure(
        ExecutionRecord record,
        RequirementState control,
        SemanticHoldEvent event,
        List<ProofInteractionProvenance> provenance
    ) {
        SemanticHoldFailure failure = event.failure().orElse(SemanticHoldFailure.INTERNAL_FAILURE);
        switch (failure) {
            case AMBIGUOUS_MATCH -> {
                control.set(
                    ProofResolution.AMBIGUOUS,
                    ProofResolutionReason.CONTROL_MATCH_AMBIGUOUS,
                    Optional.of(event.connectionId()),
                    List.of()
                );
                completeLocked(record, ProofOutcome.INCONCLUSIVE, null);
            }
            case CORRELATION_INVALIDATED -> {
                control.set(
                    ProofResolution.AMBIGUOUS,
                    ProofResolutionReason.CONTROL_CORRELATION_INVALIDATED,
                    Optional.of(event.connectionId()),
                    provenance
                );
                completeLocked(record, ProofOutcome.INCONCLUSIVE, null);
            }
            case SESSION_ABANDONED -> {
                control.set(
                    ProofResolution.MISSING,
                    ProofResolutionReason.CONTROL_SESSION_ENDED,
                    Optional.of(event.connectionId()),
                    provenance
                );
                completeLocked(record, ProofOutcome.INCONCLUSIVE, null);
            }
            case SELECTOR_EVALUATION -> {
                control.set(
                    ProofResolution.FAILED,
                    ProofResolutionReason.CONTROL_SELECTOR_FAILED,
                    Optional.of(event.connectionId()),
                    List.of()
                );
                completeLocked(
                    record,
                    ProofOutcome.ERROR,
                    diagnostic(ProofFailureStage.CONTROL, new ControlFailure())
                );
            }
            case WRITE_FAILURE, INTERNAL_FAILURE -> {
                control.set(
                    ProofResolution.FAILED,
                    ProofResolutionReason.CONTROL_FAILED,
                    Optional.of(event.connectionId()),
                    provenance
                );
                completeLocked(
                    record,
                    ProofOutcome.ERROR,
                    diagnostic(ProofFailureStage.CONTROL, new ControlFailure())
                );
            }
        }
    }

    private static boolean isPreReachTerminal(SemanticHoldEvent event) {
        return event.state() == SemanticHoldState.CANCELLED
            || event.state() == SemanticHoldState.FAILED;
    }

    private void applyGuard(ExecutionRecord record, SemanticPredecessorGuardEvent event) {
        if (event.predecessor()
            .or(() -> event.successor())
            .filter(reference -> !isWithinEvidenceWindow(record, reference))
            .isPresent()
            || event.successor()
                .filter(reference -> !isWithinEvidenceWindow(record, reference))
                .isPresent()) {
            return;
        }
        RequirementState control = record.guardControls.get(event.guardRef());
        Map<ProofEvidenceKind, RequirementState> evidence = record.guardEvidence.get(
            event.guardRef()
        );
        RequirementState relation = record.relations.get(event.guardRef());
        if (control == null && evidence == null && relation == null) {
            return;
        }
        List<ProofInteractionProvenance> provenance = guardProvenance(event);
        if (evidence != null) {
            event.predecessor().ifPresent(reference -> setEvidence(
                evidence.get(ProofEvidenceKind.PREDECESSOR_INTERACTION),
                ProofInteractionProvenance.predecessor(reference)
            ));
            event.successor().ifPresent(reference -> setEvidence(
                evidence.get(ProofEvidenceKind.SUCCESSOR_INTERACTION),
                ProofInteractionProvenance.successor(reference)
            ));
        }
        if (event.kind() == SemanticPredecessorGuardEvent.Kind.RELATION && relation != null) {
            relation.set(
                ProofResolution.SATISFIED,
                ProofResolutionReason.CAUSAL_RELATION_ESTABLISHED,
                Optional.empty(),
                provenance
            );
        }
        if (event.kind() == SemanticPredecessorGuardEvent.Kind.TERMINAL) {
            if (event.state() == SemanticPredecessorGuardState.SATISFIED) {
                if (control != null) {
                    control.set(
                        ProofResolution.SATISFIED,
                        ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE,
                        Optional.empty(),
                        provenance
                    );
                }
                if (relation != null) {
                    relation.set(
                        ProofResolution.SATISFIED,
                        ProofResolutionReason.CAUSAL_RELATION_ESTABLISHED,
                        Optional.empty(),
                        provenance
                    );
                }
                return;
            }
            if (event.state() == SemanticPredecessorGuardState.VIOLATED) {
                Optional<ConnectionId> successorConnection = event.successor()
                    .map(InteractionRef::connectionId);
                if (control != null) {
                    control.set(
                        ProofResolution.VIOLATED,
                        ProofResolutionReason.CAUSAL_RELATION_VIOLATED,
                        successorConnection,
                        provenance
                    );
                }
                if (relation != null) {
                    relation.set(
                        ProofResolution.VIOLATED,
                        ProofResolutionReason.CAUSAL_RELATION_VIOLATED,
                        successorConnection,
                        provenance
                    );
                }
                completeLocked(record, ProofOutcome.VIOLATED, null);
                return;
            }
        }
        if (event.kind() == SemanticPredecessorGuardEvent.Kind.VIOLATION) {
            Optional<ConnectionId> successorConnection = event.successor()
                .map(InteractionRef::connectionId);
            if (control != null) {
                control.set(
                    ProofResolution.VIOLATED,
                    ProofResolutionReason.CAUSAL_RELATION_VIOLATED,
                    successorConnection,
                    provenance
                );
            }
            if (relation != null) {
                relation.set(
                    ProofResolution.VIOLATED,
                    ProofResolutionReason.CAUSAL_RELATION_VIOLATED,
                    successorConnection,
                    provenance
                );
            }
            completeLocked(record, ProofOutcome.VIOLATED, null);
            return;
        }
        if (control == null || event.kind() != SemanticPredecessorGuardEvent.Kind.STATE) {
            return;
        }
        switch (event.state()) {
            case DECLARED, ARMED, PREDECESSOR_OBSERVED, PREDECESSOR_SATISFIED,
                 SUCCESSOR_AUTHORIZED -> {
                // Still unresolved.
            }
            case SATISFIED -> control.set(
                ProofResolution.SATISFIED,
                ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE,
                Optional.empty(),
                provenance
            );
            case VIOLATED -> {
                // The explicit VIOLATION fact is the authoritative terminal counterexample.
            }
            case CANCELLED -> control.set(
                ProofResolution.UNREACHED,
                ProofResolutionReason.CONTROL_UNREACHED,
                Optional.empty(),
                provenance
            );
            case TIMED_OUT -> {
                control.set(
                    ProofResolution.TIMED_OUT,
                    ProofResolutionReason.CONTROL_TIMED_OUT,
                    Optional.empty(),
                    provenance
                );
                completeLocked(record, ProofOutcome.INCONCLUSIVE, null);
            }
            case FAILED -> applyGuardFailure(record, control, event, provenance);
        }
    }

    private void applyGuardFailure(
        ExecutionRecord record,
        RequirementState control,
        SemanticPredecessorGuardEvent event,
        List<ProofInteractionProvenance> provenance
    ) {
        SemanticPredecessorGuardFailure failure = event.failure()
            .orElse(SemanticPredecessorGuardFailure.INTERNAL_FAILURE);
        switch (failure) {
            case CORRELATION_INVALIDATED -> {
                control.set(
                    ProofResolution.AMBIGUOUS,
                    ProofResolutionReason.CONTROL_CORRELATION_INVALIDATED,
                    Optional.empty(),
                    provenance
                );
                completeLocked(record, ProofOutcome.INCONCLUSIVE, null);
            }
            case SESSION_ABANDONED -> {
                control.set(
                    ProofResolution.MISSING,
                    ProofResolutionReason.CONTROL_SESSION_ENDED,
                    Optional.empty(),
                    provenance
                );
                completeLocked(record, ProofOutcome.INCONCLUSIVE, null);
            }
            case SELECTOR_EVALUATION -> {
                control.set(
                    ProofResolution.FAILED,
                    ProofResolutionReason.CONTROL_SELECTOR_FAILED,
                    Optional.empty(),
                    provenance
                );
                if (record.state != ProofExecutionState.ACTIVATING) {
                    completeLocked(
                        record,
                        ProofOutcome.ERROR,
                        diagnostic(ProofFailureStage.CONTROL, new ControlFailure())
                    );
                }
            }
            case WRITE_FAILURE, REQUIRED_OBSERVATION_FAILURE, INTERNAL_FAILURE -> {
                control.set(
                    ProofResolution.FAILED,
                    ProofResolutionReason.CONTROL_FAILED,
                    Optional.empty(),
                    provenance
                );
                if (record.state != ProofExecutionState.ACTIVATING) {
                    completeLocked(
                        record,
                        ProofOutcome.ERROR,
                        diagnostic(ProofFailureStage.CONTROL, new ControlFailure())
                    );
                }
            }
        }
    }

    private void deadlineExpired(ExecutionRecord record) {
        controls.withRequiredObservationBoundary(() ->
            proofSubjects.withCorrelationBoundary(
                record.plan.primarySubject(),
                correlationRequirements(record),
                snapshots -> deadlineLocked(record, snapshots)
            )
        );
        finalizePending(record);
    }

    private void deadlineLocked(
        ExecutionRecord record,
        List<ProofSubjectRegistry.CorrelationSnapshot> snapshots
    ) {
        boundaryObserver.deadlineBoundaryReached();
        synchronized (this) {
            if (record != execution || record.state != ProofExecutionState.ACTIVE
                || record.outcome != null) {
                return;
            }
            try {
                applyCorrelationSnapshots(record, snapshots);
                record.primaryEvaluation = new ProofEvaluationResolution(
                    record.evaluationState,
                    ProofResolution.TIMED_OUT,
                    ProofResolutionReason.DEADLINE_EXPIRED
                );
                if (record.stimulusLifecycle != StimulusLifecycle.COMPLETED) {
                    record.stimulusTerminal = new ProofStimulusResolution(
                        record.stimulusLifecycle == StimulusLifecycle.RUNNING
                            ? ProofStimulusState.RUNNING
                            : ProofStimulusState.NOT_STARTED,
                        ProofResolution.TIMED_OUT,
                        ProofResolutionReason.DEADLINE_EXPIRED
                    );
                }
                completeLocked(record, ProofOutcome.INCONCLUSIVE, null);
            } catch (RuntimeException | Error failure) {
                rethrowFatal(failure);
                record.evaluationState = ProofEvaluationState.FAILED;
                record.primaryEvaluation = new ProofEvaluationResolution(
                    ProofEvaluationState.FAILED,
                    ProofResolution.FAILED,
                    ProofResolutionReason.EVALUATION_FAILED
                );
                completeLocked(
                    record,
                    ProofOutcome.ERROR,
                    new ProofDiagnostic(
                        ProofFailureStage.EVALUATION,
                        FailureDetails.from(failure)
                    )
                );
            }
        }
    }

    private void runStimulus(ExecutionRecord record, Runnable stimulus) {
        stimulus = Objects.requireNonNull(stimulus, "stimulus must not be null");
        synchronized (this) {
            requireRecord(record);
            if (record.stimulusLifecycle != StimulusLifecycle.NOT_STARTED) {
                throw new IllegalStateException("Proof stimulus can be attempted only once");
            }
            if (record.outcome != null) {
                return;
            }
            if (record.state != ProofExecutionState.ACTIVE) {
                throw new IllegalStateException(
                    "Proof stimulus requires an ACTIVE execution"
                );
            }
            record.stimulusLifecycle = StimulusLifecycle.RUNNING;
        }
        try {
            stimulus.run();
            synchronized (this) {
                if (record.outcome == null) {
                    record.stimulusLifecycle = StimulusLifecycle.COMPLETED;
                }
            }
        } catch (RuntimeException | Error failure) {
            rethrowFatal(failure);
            synchronized (this) {
                if (record.outcome == null) {
                    record.stimulusLifecycle = StimulusLifecycle.FAILED;
                    record.stimulusTerminal = new ProofStimulusResolution(
                        ProofStimulusState.FAILED,
                        ProofResolution.FAILED,
                        ProofResolutionReason.STIMULUS_FAILED
                    );
                    completeLocked(
                        record,
                        ProofOutcome.ERROR,
                        new ProofDiagnostic(
                            ProofFailureStage.STIMULUS,
                            FailureDetails.from(failure)
                        )
                    );
                } else if (record.state != ProofExecutionState.COMPLETED) {
                    addSecondary(
                        record,
                        new ProofDiagnostic(
                            ProofFailureStage.STIMULUS,
                            FailureDetails.from(failure)
                        )
                    );
                }
            }
            finalizePending(record);
        }
    }

    private ProofResult evaluate(ExecutionRecord record) {
        boolean startEvaluation;
        synchronized (this) {
            requireRecord(record);
            if (record.outcome != null) {
                startEvaluation = false;
            } else {
                if (record.state != ProofExecutionState.ACTIVE) {
                    throw new IllegalStateException(
                        "Proof execution cannot be evaluated from state " + record.state
                    );
                }
                if (record.stimulusLifecycle != StimulusLifecycle.COMPLETED) {
                    throw new IllegalStateException(
                        "Proof evaluation requires one successfully completed stimulus"
                    );
                }
                startEvaluation = record.evaluationState == ProofEvaluationState.NOT_STARTED;
                if (startEvaluation) {
                    record.evaluationState = ProofEvaluationState.RUNNING;
                }
            }
        }
        if (startEvaluation) {
            CompletionStage<Void> refresh;
            try {
                refresh = Objects.requireNonNull(
                    record.refreshObservation.refresh(record.requiredObservationConnections),
                    "Proof observation refresher must return a completion stage"
                );
            } catch (RuntimeException | Error failure) {
                rethrowFatal(failure);
                observationRefreshCompleted(record, failure);
                refresh = null;
            }
            if (refresh != null) {
                refresh.whenComplete((ignored, failure) ->
                    observationRefreshCompleted(record, unwrapCompletionFailure(failure))
                );
            }
        }
        finalizePending(record);
        return awaitResult(record);
    }

    private void observationRefreshCompleted(ExecutionRecord record, Throwable failure) {
        if (failure == null) {
            evaluateAtAuthoritativeBoundary(record);
        } else {
            controls.withRequiredObservationBoundary(() -> {
                synchronized (this) {
                    if (record.outcome != null) {
                        return;
                    }
                    record.evaluationState = ProofEvaluationState.FAILED;
                    record.primaryEvaluation = new ProofEvaluationResolution(
                        ProofEvaluationState.FAILED,
                        ProofResolution.FAILED,
                        ProofResolutionReason.EVALUATION_FAILED
                    );
                    completeLocked(
                        record,
                        ProofOutcome.ERROR,
                        new ProofDiagnostic(
                            ProofFailureStage.OBSERVATION,
                            FailureDetails.from(failure)
                        )
                    );
                }
            });
        }
        finalizePending(record);
    }

    private void evaluateAtAuthoritativeBoundary(ExecutionRecord record) {
        controls.withRequiredObservationBoundary(() ->
            proofSubjects.withCorrelationBoundary(
                record.plan.primarySubject(),
                correlationRequirements(record),
                snapshots -> evaluateLocked(record, snapshots)
            )
        );
    }

    private void evaluateLocked(
        ExecutionRecord record,
        List<ProofSubjectRegistry.CorrelationSnapshot> snapshots
    ) {
        boundaryObserver.evaluationBoundaryReached();
        synchronized (this) {
            if (record.outcome != null) {
                return;
            }
            try {
                applyCorrelationSnapshots(record, snapshots);
                ProofOutcome outcome = outcomeEvaluator.evaluate(
                    record.states.stream().map(value -> value.resolution).toList()
                );
                record.evaluationState = ProofEvaluationState.COMPLETED;
                record.primaryEvaluation = new ProofEvaluationResolution(
                    ProofEvaluationState.COMPLETED,
                    ProofResolution.SATISFIED,
                    ProofResolutionReason.EVALUATION_COMPLETED
                );
                completeLocked(record, outcome, evaluationFailure(outcome));
            } catch (RuntimeException | Error evaluatorFailure) {
                rethrowFatal(evaluatorFailure);
                record.evaluationState = ProofEvaluationState.FAILED;
                record.primaryEvaluation = new ProofEvaluationResolution(
                    ProofEvaluationState.FAILED,
                    ProofResolution.FAILED,
                    ProofResolutionReason.EVALUATION_FAILED
                );
                completeLocked(
                    record,
                    ProofOutcome.ERROR,
                    new ProofDiagnostic(
                        ProofFailureStage.EVALUATION,
                        FailureDetails.from(evaluatorFailure)
                    )
                );
            }
        }
    }

    private static ProofDiagnostic evaluationFailure(ProofOutcome outcome) {
        return outcome == ProofOutcome.ERROR
            ? diagnostic(ProofFailureStage.EVALUATION, new EvaluationFailure())
            : null;
    }

    private static List<ProofSubjectRegistry.CorrelationRequirement> correlationRequirements(
        ExecutionRecord record
    ) {
        return record.correlations.stream()
            .map(state -> {
                ProofPlan.Correlation value = (ProofPlan.Correlation) state.requirement;
                return new ProofSubjectRegistry.CorrelationRequirement(
                    value.key(),
                    value.connectionId(),
                    value.nativeReferenceSchema(),
                    Optional.ofNullable(record.acceptedCorrelations.get(state))
                );
            })
            .toList();
    }

    private static void applyCorrelationSnapshots(
        ExecutionRecord record,
        List<ProofSubjectRegistry.CorrelationSnapshot> snapshots
    ) {
        if (snapshots.size() != record.correlations.size()) {
            throw new IllegalStateException(
                "Correlation snapshot count changed at the evaluation boundary"
            );
        }
        for (int index = 0; index < snapshots.size(); index++) {
            RequirementState state = record.correlations.get(index);
            ProofPlan.Correlation correlation = (ProofPlan.Correlation) state.requirement;
            ProofSubjectRegistry.CorrelationSnapshot snapshot = snapshots.get(index);
            switch (snapshot.cardinality()) {
                case UNIQUE -> state.set(
                    ProofResolution.SATISFIED,
                    ProofResolutionReason.CORRELATION_UNIQUE,
                    Optional.of(correlation.connectionId()),
                    snapshot.interaction()
                        .map(ProofInteractionProvenance::correlation)
                        .stream()
                        .toList()
                );
                case AMBIGUOUS -> state.set(
                    ProofResolution.AMBIGUOUS,
                    ProofResolutionReason.CORRELATION_AMBIGUOUS,
                    Optional.of(correlation.connectionId()),
                    List.of()
                );
                case MISSING -> state.set(
                    ProofResolution.MISSING,
                    ProofResolutionReason.CORRELATION_MISSING,
                    Optional.of(correlation.connectionId()),
                    List.of()
                );
            }
        }
    }

    private ProofResult result(ExecutionRecord record) {
        synchronized (this) {
            requireRecord(record);
            if (record.outcome == null) {
                throw new IllegalStateException(
                    "Proof result is unavailable before execution completion"
                );
            }
        }
        finalizePending(record);
        return awaitResult(record);
    }

    private void complete(
        ExecutionRecord record,
        ProofOutcome outcome,
        ProofDiagnostic failure
    ) {
        synchronized (this) {
            if (record.outcome == null) {
                completeLocked(record, outcome, failure);
            } else if (failure != null && record.state != ProofExecutionState.COMPLETED) {
                addSecondary(record, failure);
            }
        }
        finalizePending(record);
    }

    private void completeAndCancelPrepared(
        ExecutionRecord record,
        ActivationControls activationControls,
        ProofOutcome outcome,
        ProofDiagnostic failure
    ) {
        complete(record, outcome, failure);
    }

    private void completeLocked(
        ExecutionRecord record,
        ProofOutcome outcome,
        ProofDiagnostic failure
    ) {
        if (record.factBatchActive) {
            if (record.pendingCompletion == null) {
                record.pendingCompletion = new PendingCompletion(outcome, failure);
            } else if (failure != null) {
                addSecondary(record, failure);
            }
            return;
        }
        if (record.outcome != null) {
            if (failure != null) {
                addSecondary(record, failure);
            }
            return;
        }
        record.state = ProofExecutionState.EVALUATING;
        record.outcome = Objects.requireNonNull(outcome, "outcome must not be null");
        record.primaryFailure = failure;
        if (outcome == ProofOutcome.VIOLATED || outcome == ProofOutcome.ERROR) {
            markNotEvaluatedAfterTerminal(record);
        } else if (outcome == ProofOutcome.INCONCLUSIVE) {
            markActivationNotReached(record);
        }
        record.primaryResolutions = record.states.stream()
            .map(RequirementState::snapshot)
            .toList();
        record.primaryStimulus = stimulusSnapshot(record, outcome);
        if (record.primaryEvaluation == null) {
            record.primaryEvaluation = evaluationSnapshot(record, outcome);
        }
    }

    private boolean completeFactBatchLocked(ExecutionRecord record) {
        PendingCompletion pending = record.pendingCompletion;
        record.pendingCompletion = null;
        record.factBatchActive = false;
        if (pending != null) {
            completeLocked(record, pending.outcome(), pending.failure());
            return true;
        }
        return false;
    }

    private static List<RequirementStateSnapshot> snapshotStates(ExecutionRecord record) {
        return record.states.stream()
            .map(RequirementState::stateSnapshot)
            .toList();
    }

    private static void restoreStates(
        ExecutionRecord record,
        List<RequirementStateSnapshot> snapshots
    ) {
        if (record.states.size() != snapshots.size()) {
            throw new IllegalStateException("Proof requirement state cardinality changed");
        }
        for (int index = 0; index < record.states.size(); index++) {
            record.states.get(index).restore(snapshots.get(index));
        }
    }

    private static boolean compatibleWith(
        ProofOutcome outcome,
        List<RequirementState> states
    ) {
        boolean violated = states.stream().anyMatch(
            state -> state.resolution == ProofResolution.VIOLATED
        );
        boolean failed = states.stream().anyMatch(
            state -> state.resolution == ProofResolution.FAILED
        );
        return switch (outcome) {
            case ERROR -> !violated;
            case VIOLATED -> !failed;
            case INCONCLUSIVE -> !violated && !failed;
            case PROVED -> states.stream().allMatch(
                state -> state.resolution == ProofResolution.SATISFIED
            );
        };
    }

    private void retainIncompatibleBatchFact(
        ExecutionRecord record,
        ScenarioEvent event
    ) {
        if (event instanceof FailureEvent failure && relevantFailure(record, failure)) {
            addSecondary(
                record,
                new ProofDiagnostic(failureStage(failure), failure.failure())
            );
        } else if (event instanceof SemanticHoldEvent
            || event instanceof SemanticPredecessorGuardEvent) {
            addSecondary(
                record,
                diagnostic(ProofFailureStage.CONTROL, new ControlFailure())
            );
        }
    }

    private static void markNotEvaluatedAfterTerminal(ExecutionRecord record) {
        for (RequirementState state : record.states) {
            if (state.resolution != ProofResolution.NOT_EVALUATED) {
                continue;
            }
            state.set(
                ProofResolution.NOT_EVALUATED,
                ProofResolutionReason.NOT_EVALUATED_AFTER_TERMINAL_OUTCOME,
                state.connectionId,
                List.of()
            );
        }
    }

    private static void markActivationNotReached(ExecutionRecord record) {
        for (RequirementState state : record.states) {
            if (state.resolution == ProofResolution.NOT_EVALUATED) {
                state.set(
                    ProofResolution.UNREACHED,
                    ProofResolutionReason.ACTIVATION_NOT_REACHED,
                    state.connectionId,
                    List.of()
                );
            }
        }
    }

    private static ProofStimulusResolution stimulusSnapshot(
        ExecutionRecord record,
        ProofOutcome outcome
    ) {
        if (record.stimulusTerminal != null) {
            return record.stimulusTerminal;
        }
        return switch (record.stimulusLifecycle) {
            case COMPLETED -> new ProofStimulusResolution(
                ProofStimulusState.COMPLETED,
                ProofResolution.SATISFIED,
                ProofResolutionReason.STIMULUS_COMPLETED
            );
            case FAILED -> new ProofStimulusResolution(
                ProofStimulusState.FAILED,
                ProofResolution.FAILED,
                ProofResolutionReason.STIMULUS_FAILED
            );
            case NOT_STARTED, RUNNING -> {
                ProofStimulusState state = record.stimulusLifecycle
                    == StimulusLifecycle.RUNNING
                        ? ProofStimulusState.RUNNING
                        : ProofStimulusState.NOT_STARTED;
                if (outcome == ProofOutcome.INCONCLUSIVE) {
                    yield new ProofStimulusResolution(
                        state,
                        ProofResolution.UNREACHED,
                        record.activationReached
                            ? ProofResolutionReason.STIMULUS_NOT_COMPLETED
                            : ProofResolutionReason.ACTIVATION_NOT_REACHED
                    );
                }
                yield new ProofStimulusResolution(
                    state,
                    ProofResolution.NOT_EVALUATED,
                    ProofResolutionReason.NOT_EVALUATED_AFTER_TERMINAL_OUTCOME
                );
            }
        };
    }

    private static ProofEvaluationResolution evaluationSnapshot(
        ExecutionRecord record,
        ProofOutcome outcome
    ) {
        return switch (record.evaluationState) {
            case COMPLETED -> new ProofEvaluationResolution(
                ProofEvaluationState.COMPLETED,
                ProofResolution.SATISFIED,
                ProofResolutionReason.EVALUATION_COMPLETED
            );
            case FAILED -> new ProofEvaluationResolution(
                ProofEvaluationState.FAILED,
                ProofResolution.FAILED,
                ProofResolutionReason.EVALUATION_FAILED
            );
            case NOT_STARTED, RUNNING -> outcome == ProofOutcome.INCONCLUSIVE
                ? new ProofEvaluationResolution(
                    record.evaluationState,
                    ProofResolution.UNREACHED,
                    ProofResolutionReason.EVALUATION_NOT_REACHED
                )
                : new ProofEvaluationResolution(
                    record.evaluationState,
                    ProofResolution.NOT_EVALUATED,
                    ProofResolutionReason.NOT_EVALUATED_AFTER_TERMINAL_OUTCOME
                );
        };
    }

    private void finalizePending(ExecutionRecord record) {
        boolean owner = false;
        synchronized (this) {
            if (record.outcome == null || record.finalizationComplete) {
                return;
            }
            if (record.deadlineInstalling) {
                return;
            }
            if (record.authoritativeOutcomeBoundaryPending) {
                return;
            }
            if (!record.finalizing) {
                record.finalizing = true;
                record.finalizationOwner = Thread.currentThread();
                owner = true;
            } else if (record.finalizationOwner == Thread.currentThread()) {
                return;
            }
        }
        if (!owner) {
            awaitFinalization(record);
            return;
        }

        DeadlineTask deadline;
        ActivationControls activationControls;
        synchronized (this) {
            deadline = record.deadlineTask;
            record.deadlineTask = null;
            activationControls = record.activationControls;
        }
        if (deadline != null) {
            cancelDeadlineTask(record, deadline);
        }
        SemanticControlCoordinator.PreparedControlCancellation controlNotifications = null;
        if (activationControls != null) {
            try {
                controlNotifications = controls.cancelPreparedControlsInternally(
                    activationControls.holds,
                    activationControls.guards
                );
                for (Throwable failure : controlNotifications.failures()) {
                    addSecondarySafely(
                        record,
                        diagnostic(ProofFailureStage.CLEANUP, failure)
                    );
                }
            } catch (RuntimeException | Error failure) {
                rethrowFatal(failure);
                addSecondarySafely(
                    record,
                    diagnostic(ProofFailureStage.CLEANUP, failure)
                );
            }
        }
        if (controlNotifications != null) {
            controls.runPreparedInternalActions(controlNotifications);
        }

        ProofResult frozen;
        SemanticControlCoordinator.CompletionGate publicationGate = controls.newCompletionGate();
        RuntimeException injectedConstructionFailure =
            boundaryObserver.resultConstructionFailure();
        synchronized (this) {
            try {
                if (injectedConstructionFailure != null) {
                    throw injectedConstructionFailure;
                }
                frozen = freezeResultLocked(record);
            } catch (RuntimeException constructionFailure) {
                frozen = recoverResultConstructionFailureLocked(
                    record,
                    constructionFailure
                );
            }
            record.result = frozen;
            record.state = ProofExecutionState.COMPLETED;
        }
        try {
            boundaryObserver.resultCreatedBeforeCompletionSubmission();
        } catch (RuntimeException | Error ignored) {
            rethrowFatal(ignored);
            // A package-private boundary observer cannot poison immutable result publication.
        }
        if (controlNotifications != null) {
            controls.submitPreparedPublicCompletions(
                controlNotifications,
                publicationGate
            );
        }
        record.resultReady.complete(frozen);
        publicationGate.open();
        synchronized (this) {
            record.finalizationComplete = true;
            record.finalizing = false;
            record.finalizationOwner = null;
        }
        record.finalizationReady.complete(null);
    }

    private static ProofResult freezeResultLocked(ExecutionRecord record) {
        return new ProofResult(
            record.plan.id(),
            record.plan.title(),
            record.outcome,
            record.plan.primarySubject(),
            record.primaryStimulus,
            record.primaryEvaluation,
            record.primaryResolutions,
            Optional.ofNullable(record.primaryFailure),
            List.copyOf(record.secondaryDiagnostics)
        );
    }

    private ProofResult recoverResultConstructionFailureLocked(
        ExecutionRecord record,
        Throwable constructionFailure
    ) {
        record.resultConstructionRecoveryCount++;
        if (record.primaryFailure != null) {
            addSecondary(record, record.primaryFailure);
        }
        record.outcome = ProofOutcome.ERROR;
        record.primaryFailure = new ProofDiagnostic(
            ProofFailureStage.EVALUATION,
            FailureDetails.from(constructionFailure)
        );
        for (RequirementState state : record.states) {
            state.set(
                ProofResolution.NOT_EVALUATED,
                ProofResolutionReason.NOT_EVALUATED_AFTER_TERMINAL_OUTCOME,
                state.connectionId,
                List.of()
            );
        }
        record.primaryResolutions = record.states.stream()
            .map(RequirementState::snapshot)
            .toList();
        record.primaryStimulus = stimulusSnapshot(record, ProofOutcome.ERROR);
        record.primaryEvaluation = evaluationSnapshot(record, ProofOutcome.ERROR);
        return freezeResultLocked(record);
    }

    private ProofResult awaitResult(ExecutionRecord record) {
        return record.resultReady.join();
    }

    private static void awaitFinalization(ExecutionRecord record) {
        record.finalizationReady.join();
    }

    private static void joinObservationRefresh(CompletionStage<Void> refresh) {
        try {
            Objects.requireNonNull(
                refresh,
                "Proof observation refresher must return a completion stage"
            ).toCompletableFuture().join();
        } catch (CompletionException failure) {
            Throwable cause = unwrapCompletionFailure(failure);
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new CompletionException(cause);
        }
    }

    private static Throwable unwrapCompletionFailure(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static void rethrowFatal(Throwable failure) {
        if (failure instanceof Error fatal && !(failure instanceof AssertionError)) {
            throw fatal;
        }
    }

    private RuntimeProofPrerequisite requirePrerequisite(ProofPrerequisite prerequisite) {
        Objects.requireNonNull(prerequisite, "prerequisite must not be null");
        if (!(prerequisite instanceof RuntimeProofPrerequisite runtime)
            || runtime.owner != prerequisiteOwner) {
            throw new ProofConfigurationException(
                "Proof prerequisite belongs to a different environment execution"
            );
        }
        return runtime;
    }

    private FailureDetails prerequisiteFailure(ProofPlan.Requirement requirement) {
        ProofPlan.Prerequisite prerequisite = (ProofPlan.Prerequisite) requirement;
        return requirePrerequisite(prerequisite.prerequisite()).failure.orElseThrow();
    }

    private static void requireObservation(
        Map<ConnectionId, ProofPlan.Observation> observations,
        ConnectionId connectionId,
        String obligationId
    ) {
        if (!observations.containsKey(connectionId)) {
            throw new ProofConfigurationException(
                "Proof obligation '" + obligationId
                    + "' has no required observation coverage declaration"
            );
        }
    }

    private void discardInvalidExecution(ExecutionRecord record) {
        synchronized (this) {
            if (execution == record && record.state == ProofExecutionState.ACTIVATING) {
                execution = null;
            }
        }
    }

    private synchronized boolean isComplete(ExecutionRecord record) {
        return record.outcome != null;
    }

    private static void setEvidence(
        RequirementState state,
        ProofInteractionProvenance provenance
    ) {
        if (state != null) {
            state.set(
                ProofResolution.SATISFIED,
                ProofResolutionReason.EVIDENCE_PRESENT,
                Optional.of(provenance.interaction().connectionId()),
                List.of(provenance)
            );
        }
    }

    private static List<ProofInteractionProvenance> guardProvenance(
        SemanticPredecessorGuardEvent event
    ) {
        List<ProofInteractionProvenance> provenance = new ArrayList<>(2);
        event.predecessor().map(ProofInteractionProvenance::predecessor)
            .ifPresent(provenance::add);
        event.successor().map(ProofInteractionProvenance::successor)
            .ifPresent(provenance::add);
        return List.copyOf(provenance);
    }

    private boolean isWithinEvidenceWindow(
        ExecutionRecord record,
        InteractionRef interaction
    ) {
        ProofEvidenceWindowTracker.EvidenceWindow evidenceWindow = record.evidenceWindow;
        if (evidenceWindow == null) {
            return false;
        }
        return connections.isWithinProofEvidenceWindow(evidenceWindow, interaction);
    }

    private static RequirementState first(ExecutionRecord record, ProofResolution resolution) {
        return record.states.stream()
            .filter(value -> value.resolution == resolution)
            .findFirst()
            .orElse(null);
    }

    private static RequirementState observationState(
        ExecutionRecord record,
        ConnectionId connectionId
    ) {
        RequirementState indexed = record.observations.get(connectionId);
        if (indexed != null) {
            return indexed;
        }
        return record.states.stream()
            .filter(state -> state.requirement instanceof ProofPlan.Observation observation
                && observation.connectionId().equals(connectionId))
            .findFirst()
            .orElse(null);
    }

    private static ProofDiagnostic diagnostic(
        ProofFailureStage stage,
        Throwable failure
    ) {
        return new ProofDiagnostic(stage, FailureDetails.from(failure));
    }

    private static ProofFailureStage failureStage(FailureEvent failure) {
        return switch (failure) {
            case FailureEvent.ConnectionCleanup ignored -> ProofFailureStage.CLEANUP;
            case FailureEvent.ComponentCleanup ignored -> ProofFailureStage.CLEANUP;
            case FailureEvent.DriverResourceCleanup ignored -> ProofFailureStage.CLEANUP;
            case FailureEvent.ConnectionMaterialization ignored -> ProofFailureStage.GATEWAY;
            case FailureEvent.EnvironmentStartup ignored -> ProofFailureStage.ACTIVATION;
            case FailureEvent.ComponentStartup ignored -> ProofFailureStage.ACTIVATION;
        };
    }

    private static boolean relevantFailure(
        ExecutionRecord record,
        FailureEvent failure
    ) {
        return switch (failure) {
            case FailureEvent.ConnectionCleanup value ->
                record.requiredObservationConnections.contains(value.connectionId());
            case FailureEvent.ConnectionMaterialization value ->
                record.requiredObservationConnections.contains(value.connectionId());
            case FailureEvent.ComponentCleanup ignored -> true;
            case FailureEvent.DriverResourceCleanup ignored -> true;
            case FailureEvent.EnvironmentStartup ignored -> true;
            case FailureEvent.ComponentStartup ignored -> true;
        };
    }

    private void retainSecondary(ExecutionRecord record, ScenarioEvent event) {
        if (event instanceof FailureEvent failure && relevantFailure(record, failure)) {
            addSecondary(
                record,
                new ProofDiagnostic(failureStage(failure), failure.failure())
            );
        } else if (event instanceof SemanticPredecessorGuardEvent guard
            && guard.kind() == SemanticPredecessorGuardEvent.Kind.SUPPRESSED_FAILURE) {
            addSecondary(
                record,
                diagnostic(ProofFailureStage.CLEANUP, new ControlFailure())
            );
        }
    }

    private synchronized void addSecondary(
        ExecutionRecord record,
        ProofDiagnostic diagnostic
    ) {
        if (record.state == ProofExecutionState.COMPLETED) {
            return;
        }
        record.secondaryDiagnostics.add(Objects.requireNonNull(
            diagnostic,
            "diagnostic must not be null"
        ));
        record.secondaryDiagnostics.sort(DIAGNOSTIC_ORDER);
        if (record.secondaryDiagnostics.size() > MAX_SECONDARY_DIAGNOSTICS) {
            record.secondaryDiagnostics.subList(
                MAX_SECONDARY_DIAGNOSTICS,
                record.secondaryDiagnostics.size()
            ).clear();
        }
    }

    private void addSecondarySafely(
        ExecutionRecord record,
        ProofDiagnostic diagnostic
    ) {
        addSecondary(record, diagnostic);
    }

    private void cancelDeadlineTask(ExecutionRecord record, DeadlineTask deadline) {
        try {
            deadline.cancel();
        } catch (RuntimeException | Error failure) {
            rethrowFatal(failure);
            addSecondarySafely(
                record,
                diagnostic(ProofFailureStage.CLEANUP, failure)
            );
        }
    }

    private synchronized void requireRecord(ExecutionRecord record) {
        if (record != execution) {
            throw new IllegalArgumentException(
                "Proof execution belongs to a different environment execution"
            );
        }
    }

    static PublicationInvariant publicationInvariant(ProofExecution execution) {
        Objects.requireNonNull(execution, "execution must not be null");
        if (!(execution instanceof ExecutionHandle handle)) {
            throw new IllegalArgumentException(
                "Proof execution is not owned by this environment coordinator"
            );
        }
        return handle.coordinator.publicationInvariant(handle.record);
    }

    private synchronized PublicationInvariant publicationInvariant(ExecutionRecord record) {
        requireRecord(record);
        int primaryResolutionCount = record.primaryResolutions == null
            ? 0
            : record.primaryResolutions.size();
        boolean strictPrimaryResolutions = record.primaryResolutions != null
            && record.primaryResolutions.stream().allMatch(
                ProofExecutionCoordinator::isStrictResolutionSnapshot
            );
        ProofResult ready = record.resultReady.getNow(null);
        return new PublicationInvariant(
            record.outcome != null,
            record.plan.requirements().size(),
            primaryResolutionCount,
            strictPrimaryResolutions,
            record.resultReady.isDone()
                && !record.resultReady.isCompletedExceptionally()
                && !record.resultReady.isCancelled(),
            ready != null && ready == record.result,
            record.finalizationReady.isDone()
                && !record.finalizationReady.isCompletedExceptionally()
                && !record.finalizationReady.isCancelled(),
            record.finalizationComplete,
            record.finalizing,
            record.finalizationOwner != null,
            record.authoritativeOutcomeBoundaryPending,
            record.resultConstructionRecoveryCount
        );
    }

    private static boolean isStrictResolutionSnapshot(ProofObligationResolution resolution) {
        try {
            new ProofObligationResolution(
                resolution.id(),
                resolution.kind(),
                resolution.descriptor(),
                resolution.resolution(),
                resolution.reason(),
                resolution.connectionId(),
                resolution.provenance()
            );
            return true;
        } catch (RuntimeException invalidSnapshot) {
            return false;
        }
    }

    private void requireBound() {
        if (!bound) {
            throw new IllegalStateException("Proof execution coordinator is not bound");
        }
    }

    interface DeadlineScheduler extends AutoCloseable {
        DeadlineTask schedule(Duration delay, Runnable action);

        @Override
        void close();
    }

    record PublicationInvariant(
        boolean outcomeSelected,
        int expectedPrimaryResolutionCount,
        int primaryResolutionCount,
        boolean strictPrimaryResolutions,
        boolean resultReadyCompletedNormally,
        boolean resultIdentityPublished,
        boolean finalizationReadyCompletedNormally,
        boolean finalizationComplete,
        boolean finalizing,
        boolean finalizationOwnerPresent,
        boolean authoritativeOutcomeBoundaryPending,
        int resultConstructionRecoveryCount
    ) {}

    @FunctionalInterface
    interface ObservationRefresher {
        CompletionStage<Void> refresh(Set<ConnectionId> connectionIds);
    }

    interface BoundaryObserver {
        BoundaryObserver NONE = new BoundaryObserver() {};

        default void deadlineBoundaryReached() {}

        default void evaluationBoundaryReached() {}

        default void evidenceWindowCaptured() {}

        default void resultCreatedBeforeCompletionSubmission() {}

        default void authoritativeOutcomeSelectedBeforeFinalization() {}

        default void beforeAuthoritativeOperationBoundary() {}

        default RuntimeException resultConstructionFailure() {
            return null;
        }
    }

    @FunctionalInterface
    interface DeadlineTask {
        void cancel();
    }

    private static final class SystemDeadlineScheduler implements DeadlineScheduler {
        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            runnable -> {
                Thread thread = new Thread(runnable, "system-proof-deadline");
                thread.setDaemon(true);
                return thread;
            }
        );

        @Override
        public DeadlineTask schedule(Duration delay, Runnable action) {
            ScheduledFuture<?> future = executor.schedule(
                Objects.requireNonNull(action, "action must not be null"),
                Objects.requireNonNull(delay, "delay must not be null").toNanos(),
                TimeUnit.NANOSECONDS
            );
            return () -> future.cancel(false);
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }

    private record ActivationControls(
        List<SemanticHoldRef> holds,
        List<ConnectionId> holdConnections,
        List<SemanticPredecessorGuardRef> guards
    ) {
        private ActivationControls {
            holds = List.copyOf(Objects.requireNonNull(holds, "holds must not be null"));
            holdConnections = List.copyOf(Objects.requireNonNull(
                holdConnections,
                "holdConnections must not be null"
            ));
            guards = List.copyOf(Objects.requireNonNull(guards, "guards must not be null"));
            if (holds.size() != holdConnections.size()) {
                throw new IllegalArgumentException(
                    "Every activated hold requires one owning connection"
                );
            }
        }

        private ConnectionId connectionFor(SemanticHoldRef holdRef) {
            for (int index = 0; index < holds.size(); index++) {
                if (holds.get(index) == holdRef) {
                    return holdConnections.get(index);
                }
            }
            throw new IllegalStateException("Activated hold connection is unavailable");
        }
    }

    private record ControlMetadata(
        Map<SemanticHoldRef, SemanticControlCoordinator.HoldDeclaration> holds,
        Map<SemanticPredecessorGuardRef, SemanticControlCoordinator.GuardDeclaration> guards
    ) {
        private ControlMetadata {
            holds = Map.copyOf(Objects.requireNonNull(holds, "holds must not be null"));
            guards = Map.copyOf(Objects.requireNonNull(guards, "guards must not be null"));
        }

        private SemanticControlCoordinator.HoldDeclaration hold(SemanticHoldRef ref) {
            SemanticControlCoordinator.HoldDeclaration declaration = holds.get(ref);
            if (declaration == null) {
                throw new ProofConfigurationException(
                    "Semantic hold metadata is unavailable for the proof plan"
                );
            }
            return declaration;
        }

        private SemanticControlCoordinator.GuardDeclaration guard(
            SemanticPredecessorGuardRef ref
        ) {
            SemanticControlCoordinator.GuardDeclaration declaration = guards.get(ref);
            if (declaration == null) {
                throw new ProofConfigurationException(
                    "Predecessor guard metadata is unavailable for the proof plan"
                );
            }
            return declaration;
        }
    }

    private record ObservationSeed(
        RequirementState state,
        RuntimeConnectionSnapshot snapshot
    ) {
        private ObservationSeed {
            state = Objects.requireNonNull(state, "state must not be null");
            snapshot = Objects.requireNonNull(snapshot, "snapshot must not be null");
        }
    }

    private ProofRequirementDescriptor descriptor(
        ProofPlan plan,
        ProofPlan.Requirement requirement,
        ControlMetadata controls
    ) {
        return switch (requirement) {
            case ProofPlan.Prerequisite value ->
                new ProofRequirementDescriptor.Prerequisite(
                    requirePrerequisite(value.prerequisite()).status()
                );
            case ProofPlan.Observation value ->
                new ProofRequirementDescriptor.Observation(
                    value.connectionId(),
                    value.profile()
                );
            case ProofPlan.Correlation value ->
                new ProofRequirementDescriptor.Correlation(
                    plan.primarySubject(),
                    value.key(),
                    value.connectionId(),
                    value.nativeReferenceSchema()
                );
            case ProofPlan.HoldControl value ->
                new ProofRequirementDescriptor.HoldControl(
                    value.holdRef(),
                    value.expectedState(),
                    controls.hold(value.holdRef()).connectionId()
                );
            case ProofPlan.GuardControl value ->
                new ProofRequirementDescriptor.GuardControl(
                    value.guardRef(),
                    value.expectedState(),
                    controls.guard(value.guardRef()).predecessorConnectionId(),
                    controls.guard(value.guardRef()).successorConnectionId()
                );
            case ProofPlan.HoldEvidence value ->
                new ProofRequirementDescriptor.HoldEvidence(
                    value.holdRef(),
                    value.evidenceKind(),
                    controls.hold(value.holdRef()).connectionId()
                );
            case ProofPlan.GuardEvidence value ->
                new ProofRequirementDescriptor.GuardEvidence(
                    value.guardRef(),
                    value.evidenceKind(),
                    value.evidenceKind() == ProofEvidenceKind.PREDECESSOR_INTERACTION
                        ? controls.guard(value.guardRef()).predecessorConnectionId()
                        : controls.guard(value.guardRef()).successorConnectionId()
                );
            case ProofPlan.CausalRelation value ->
                new ProofRequirementDescriptor.CausalRelation(
                    value.guardRef(),
                    controls.guard(value.guardRef()).predecessorConnectionId(),
                    controls.guard(value.guardRef()).successorConnectionId()
                );
        };
    }

    private enum StimulusLifecycle {
        NOT_STARTED,
        RUNNING,
        COMPLETED,
        FAILED
    }

    private static final class ExecutionRecord {
        private final ProofPlan plan;
        private final ExecutionHandle handle;
        private final ObservationRefresher refreshObservation;
        private final Set<ConnectionId> requiredObservationConnections;
        private final List<RequirementState> states;
        private final Map<ConnectionId, RequirementState> observations = new HashMap<>();
        private final List<RequirementState> correlations = new ArrayList<>();
        private final Map<RequirementState, InteractionRef> acceptedCorrelations =
            new HashMap<>();
        private final Map<SemanticHoldRef, RequirementState> holdControls = new HashMap<>();
        private final Map<SemanticPredecessorGuardRef, RequirementState> guardControls =
            new HashMap<>();
        private final Map<SemanticHoldRef, RequirementState> holdEvidence = new HashMap<>();
        private final Map<
            SemanticPredecessorGuardRef,
            Map<ProofEvidenceKind, RequirementState>
        > guardEvidence = new HashMap<>();
        private final Map<SemanticPredecessorGuardRef, RequirementState> relations =
            new HashMap<>();
        private final List<ProofDiagnostic> secondaryDiagnostics = new ArrayList<>();
        private final CompletableFuture<ProofResult> resultReady = new CompletableFuture<>();
        private final CompletableFuture<Void> finalizationReady = new CompletableFuture<>();
        private ProofExecutionState state = ProofExecutionState.DRAFT;
        private ProofOutcome outcome;
        private ProofDiagnostic primaryFailure;
        private ProofResult result;
        private List<ProofObligationResolution> primaryResolutions;
        private ProofStimulusResolution primaryStimulus;
        private ProofEvaluationResolution primaryEvaluation;
        private ProofStimulusResolution stimulusTerminal;
        private DeadlineTask deadlineTask;
        private boolean deadlineInstalling;
        private ActivationControls activationControls;
        private StimulusLifecycle stimulusLifecycle = StimulusLifecycle.NOT_STARTED;
        private ProofEvaluationState evaluationState = ProofEvaluationState.NOT_STARTED;
        private Thread finalizationOwner;
        private boolean finalizing;
        private boolean finalizationComplete;
        private boolean factBatchActive;
        private boolean authoritativeOutcomeBoundaryPending;
        private int resultConstructionRecoveryCount;
        private PendingCompletion pendingCompletion;
        private boolean activationReached;
        private ProofEvidenceWindowTracker.EvidenceWindow evidenceWindow;

        private ExecutionRecord(
            ProofPlan plan,
            ProofExecutionCoordinator coordinator,
            ObservationRefresher refreshObservation,
            ControlMetadata controlMetadata
        ) {
            this.plan = Objects.requireNonNull(plan, "plan must not be null");
            this.refreshObservation = Objects.requireNonNull(
                refreshObservation,
                "refreshObservation must not be null"
            );
            handle = new ExecutionHandle(coordinator, this);
            states = plan.requirements().stream()
                .map(requirement -> new RequirementState(
                    requirement,
                    coordinator.descriptor(plan, requirement, controlMetadata)
                ))
                .toList();
            LinkedHashSet<ConnectionId> requiredConnections = new LinkedHashSet<>();
            plan.requirements().stream()
                .filter(ProofPlan.Observation.class::isInstance)
                .map(ProofPlan.Observation.class::cast)
                .map(ProofPlan.Observation::connectionId)
                .forEach(requiredConnections::add);
            requiredObservationConnections = java.util.Collections.unmodifiableSet(
                requiredConnections
            );
        }
    }

    private record PendingCompletion(
        ProofOutcome outcome,
        ProofDiagnostic failure
    ) {
        private PendingCompletion {
            Objects.requireNonNull(outcome, "outcome must not be null");
        }
    }

    private static final class RequirementState {
        private final ProofPlan.Requirement requirement;
        private final ProofRequirementDescriptor descriptor;
        private ProofResolution resolution = ProofResolution.NOT_EVALUATED;
        private ProofResolutionReason reason =
            ProofResolutionReason.NOT_EVALUATED_AFTER_TERMINAL_OUTCOME;
        private Optional<ConnectionId> connectionId = Optional.empty();
        private List<ProofInteractionProvenance> provenance = List.of();

        private RequirementState(
            ProofPlan.Requirement requirement,
            ProofRequirementDescriptor descriptor
        ) {
            this.requirement = Objects.requireNonNull(
                requirement,
                "requirement must not be null"
            );
            this.descriptor = Objects.requireNonNull(
                descriptor,
                "descriptor must not be null"
            );
        }

        private void set(
            ProofResolution resolution,
            ProofResolutionReason reason,
            Optional<ConnectionId> connectionId,
            List<ProofInteractionProvenance> provenance
        ) {
            this.resolution = Objects.requireNonNull(resolution, "resolution must not be null");
            this.reason = Objects.requireNonNull(reason, "reason must not be null");
            this.connectionId = Objects.requireNonNull(
                connectionId,
                "connectionId must not be null"
            );
            this.provenance = List.copyOf(
                Objects.requireNonNull(provenance, "provenance must not be null")
            );
        }

        private ProofObligationResolution snapshot() {
            return new ProofObligationResolution(
                requirement.id(),
                requirement.kind(),
                descriptor,
                resolution,
                reason,
                connectionId,
                provenance
            );
        }

        private RequirementStateSnapshot stateSnapshot() {
            return new RequirementStateSnapshot(
                resolution,
                reason,
                connectionId,
                provenance
            );
        }

        private void restore(RequirementStateSnapshot snapshot) {
            set(
                snapshot.resolution(),
                snapshot.reason(),
                snapshot.connectionId(),
                snapshot.provenance()
            );
        }
    }

    private record RequirementStateSnapshot(
        ProofResolution resolution,
        ProofResolutionReason reason,
        Optional<ConnectionId> connectionId,
        List<ProofInteractionProvenance> provenance
    ) {
        private RequirementStateSnapshot {
            Objects.requireNonNull(resolution, "resolution must not be null");
            Objects.requireNonNull(reason, "reason must not be null");
            Objects.requireNonNull(connectionId, "connectionId must not be null");
            provenance = List.copyOf(Objects.requireNonNull(
                provenance,
                "provenance must not be null"
            ));
        }
    }

    private static final class ExecutionHandle implements ProofExecution {
        private final ProofExecutionCoordinator coordinator;
        private final ExecutionRecord record;

        private ExecutionHandle(
            ProofExecutionCoordinator coordinator,
            ExecutionRecord record
        ) {
            this.coordinator = Objects.requireNonNull(
                coordinator,
                "coordinator must not be null"
            );
            this.record = Objects.requireNonNull(record, "record must not be null");
        }

        @Override
        public ProofExecutionState state() {
            coordinator.finalizePending(record);
            synchronized (coordinator) {
                return record.state;
            }
        }

        @Override
        public void runStimulus(Runnable stimulus) {
            coordinator.runStimulus(record, stimulus);
        }

        @Override
        public ProofResult evaluate() {
            return coordinator.evaluate(record);
        }

        @Override
        public ProofResult result() {
            return coordinator.result(record);
        }
    }

    private static final class RuntimeProofPrerequisite implements ProofPrerequisite {
        private final Object owner;
        private final ProofPrerequisiteStatus status;
        private final Optional<FailureDetails> failure;

        private RuntimeProofPrerequisite(
            Object owner,
            ProofPrerequisiteStatus status,
            Optional<FailureDetails> failure
        ) {
            this.owner = Objects.requireNonNull(owner, "owner must not be null");
            this.status = Objects.requireNonNull(status, "status must not be null");
            this.failure = Objects.requireNonNull(failure, "failure must not be null");
        }

        @Override
        public ProofPrerequisiteStatus status() {
            return status;
        }

        @Override
        public Optional<FailureDetails> failure() {
            return failure;
        }

        @Override
        public String toString() {
            return "ProofPrerequisite[status=" + status + "]";
        }
    }

    private static final class ObservationFailure extends RuntimeException {
        private ObservationFailure() {}
    }

    private static final class ControlFailure extends RuntimeException {
        private ControlFailure() {}
    }

    private static final class EvaluationFailure extends RuntimeException {
        private EvaluationFailure() {}
    }

    private static final class UnfinishedProofExecution extends RuntimeException {
        private UnfinishedProofExecution() {}
    }
}
