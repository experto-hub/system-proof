package io.github.jacekkardys.systemproof.environment;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import io.github.jacekkardys.systemproof.journal.ScenarioJournalSnapshot;
import io.github.jacekkardys.systemproof.component.AbstractComponent;
import io.github.jacekkardys.systemproof.component.Component;
import io.github.jacekkardys.systemproof.component.ComponentState;
import io.github.jacekkardys.systemproof.configuration.RuntimeConfig;
import io.github.jacekkardys.systemproof.environment.state.EnvironmentState;
import io.github.jacekkardys.systemproof.environment.state.RuntimeConnectionSnapshot;
import io.github.jacekkardys.systemproof.topology.ConnectionId;
import io.github.jacekkardys.systemproof.proof.ProofSubjects;
import io.github.jacekkardys.systemproof.control.SemanticControls;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticInteractionSelector;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardSpec;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofPrerequisite;
import io.github.jacekkardys.systemproof.proof.ProofPrerequisiteStatus;
import io.github.jacekkardys.systemproof.proof.Proofs;

/** Thread-safe internal facade over one environment execution. */
final class EnvironmentRuntime {
    private final EnvironmentExecution execution;
    private final ComponentRuntimeSupervisor components;
    private final EnvironmentInspector inspector;
    private final SemanticControlCoordinator controlCoordinator;
    private final ProofExecutionCoordinator proofCoordinator;
    private final SemanticControls controls;
    private final Proofs proofs;
    private final ExecutorService proofObservationExecutor =
        Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "system-proof-observation-refresh");
            thread.setDaemon(true);
            return thread;
        });
    private boolean observationRefreshInProgress;
    private CompletableFuture<Void> proofObservationRefresh;
    private Set<ConnectionId> proofObservationConnections = Set.of();

    private EnvironmentRuntime(EnvironmentRuntimeFactory.Assembly assembly) {
        this.execution = assembly.execution();
        this.components = assembly.components();
        this.inspector = assembly.inspector();
        controlCoordinator = assembly.controls();
        proofCoordinator = assembly.proofs();
        controls = new RuntimeAwareSemanticControls();
        proofs = new RuntimeAwareProofs();
    }

    static EnvironmentRuntime of(
        EnvironmentTopology topology,
        EnvironmentLogging logging
    ) {
        return new EnvironmentRuntime(EnvironmentRuntimeFactory.assemble(topology, logging));
    }

    static EnvironmentRuntime of(
        EnvironmentTopology topology,
        EnvironmentLogging logging,
        ConnectionRouting routing
    ) {
        return new EnvironmentRuntime(
            EnvironmentRuntimeFactory.assemble(topology, logging, routing)
        );
    }

    void start() {
        EnvironmentExecution.StartupFailure initialFailure;
        synchronized (this) {
            initialFailure = execution.beginStart();
        }
        if (initialFailure != null) {
            throwStartFailure(initialFailure);
        }

        while (true) {
            EnvironmentExecution.StartStep step;
            synchronized (this) {
                step = execution.nextStartStep();
            }
            if (step.failure() != null) {
                throwStartFailure(step.failure());
            }
            if (step.complete()) {
                return;
            }

            RuntimeConnectionRegistry.ObservationResults observationResults = null;
            Throwable observationFailure = null;
            try {
                observationResults = step.observationBatch().evaluate();
            } catch (RuntimeException | Error failure) {
                observationFailure = failure;
            }

            EnvironmentExecution.StartupFailure startupFailure;
            synchronized (this) {
                startupFailure = execution.completeStartStep(
                    observationResults,
                    observationFailure
                );
            }
            if (startupFailure != null) {
                throwStartFailure(startupFailure);
            }
        }
    }

    synchronized EnvironmentState state() {
        return execution.state();
    }

    synchronized ComponentState componentState(Component component) {
        return components.state(component);
    }

    synchronized <C extends RuntimeConfig, O> O operations(
        AbstractComponent<C, O> component
    ) {
        return components.operations(component);
    }

    EnvironmentDiagnostics diagnostics() {
        refreshObservationStatuses();
        RuntimeDiagnostics.Snapshot snapshot;
        synchronized (this) {
            snapshot = inspector.diagnosticsSnapshot();
        }
        return inspector.renderDiagnostics(snapshot);
    }

    synchronized ScenarioJournalSnapshot journalSnapshot() {
        return inspector.journalSnapshot();
    }

    synchronized ProofSubjects proofSubjects() {
        return inspector.proofSubjects();
    }

    Proofs proofs() {
        return proofs;
    }

    SemanticControls controls() {
        return controls;
    }

    List<RuntimeConnectionSnapshot> connectionSnapshots() {
        refreshObservationStatuses();
        synchronized (this) {
            return inspector.connectionSnapshots();
        }
    }

    RuntimeConnectionSnapshot connectionSnapshot(ConnectionId id) {
        refreshObservationStatuses();
        synchronized (this) {
            return inspector.connectionSnapshot(id);
        }
    }

    void close() {
        try {
            synchronized (this) {
                execution.close();
            }
        } finally {
            proofObservationExecutor.shutdownNow();
        }
    }

    private void refreshObservationStatuses() {
        RuntimeConnectionRegistry.ObservationBatch batch;
        synchronized (this) {
            if (execution.state() != EnvironmentState.RUNNING
                || observationRefreshInProgress) {
                return;
            }
            batch = execution.observationRefreshBatch();
            observationRefreshInProgress = true;
        }
        try {
            RuntimeConnectionRegistry.ObservationResults results = batch.evaluate();
            synchronized (this) {
                if (execution.state() == EnvironmentState.RUNNING) {
                    execution.applyObservationRefresh(results);
                }
            }
        } finally {
            synchronized (this) {
                observationRefreshInProgress = false;
            }
        }
    }

    private <T> T registerSemanticControl(Supplier<T> registration) {
        RuntimeConnectionRegistry.ObservationBatch batch;
        synchronized (this) {
            if (execution.state() != EnvironmentState.RUNNING) {
                return registration.get();
            }
            if (observationRefreshInProgress) {
                throw new IllegalStateException(
                    "Fresh observation status is unavailable while a refresh is in progress"
                );
            }
            batch = execution.observationRefreshBatch();
            observationRefreshInProgress = true;
        }
        try {
            RuntimeConnectionRegistry.ObservationResults results = batch.evaluate();
            synchronized (this) {
                if (execution.state() != EnvironmentState.RUNNING) {
                    throw new IllegalStateException(
                        "Fresh observation status is unavailable because the environment "
                            + "is not running"
                    );
                }
                execution.applyObservationRefresh(results);
                return registration.get();
            }
        } finally {
            synchronized (this) {
                observationRefreshInProgress = false;
            }
        }
    }

    private void throwStartFailure(EnvironmentExecution.StartupFailure failure) {
        throw new EnvironmentStartException(
            failure.cause(),
            inspector.renderDiagnostics(failure.diagnostics())
        );
    }

    private final class RuntimeAwareSemanticControls implements SemanticControls {
        @Override
        public <T> SemanticHold declareHold(
            SemanticInteractionSelector<T> selector,
            Duration maximumHoldDuration
        ) {
            return controlCoordinator.declareHold(selector, maximumHoldDuration);
        }

        @Override
        public SemanticPredecessorGuard declareGuard(
            SemanticPredecessorGuardSpec specification
        ) {
            return controlCoordinator.declareGuard(specification);
        }

        @Override
        public <T> SemanticHold arm(
            SemanticInteractionSelector<T> selector,
            Duration maximumHoldDuration
        ) {
            return registerSemanticControl(() ->
                controlCoordinator.arm(selector, maximumHoldDuration)
            );
        }

        @Override
        public SemanticPredecessorGuard guard(
            SemanticPredecessorGuardSpec specification
        ) {
            return registerSemanticControl(() -> controlCoordinator.guard(specification));
        }
    }

    private final class RuntimeAwareProofs implements Proofs {
        @Override
        public ProofPrerequisite satisfiedPrerequisite() {
            return proofCoordinator.prerequisite(ProofPrerequisiteStatus.SATISFIED, null);
        }

        @Override
        public ProofPrerequisite unsupportedPrerequisite() {
            return proofCoordinator.prerequisite(ProofPrerequisiteStatus.UNSUPPORTED, null);
        }

        @Override
        public ProofPrerequisite failedPrerequisite(Throwable failure) {
            return proofCoordinator.prerequisite(
                ProofPrerequisiteStatus.FAILED,
                java.util.Objects.requireNonNull(failure, "failure must not be null")
            );
        }

        @Override
        public ProofExecution activate(ProofPlan plan) {
            return proofCoordinator.activate(plan, EnvironmentRuntime.this::refreshForProof);
        }
    }

    private CompletionStage<Void> refreshForProof(
        Set<ConnectionId> connectionIds
    ) {
        RuntimeConnectionRegistry.ObservationBatch batch;
        CompletableFuture<Void> refresh;
        synchronized (this) {
            if (execution.state() != EnvironmentState.RUNNING) {
                throw new IllegalStateException(
                    "Proof activation requires a RUNNING environment"
                );
            }
            if (observationRefreshInProgress) {
                if (proofObservationRefresh != null
                    && !proofObservationRefresh.isDone()
                    && proofObservationConnections.equals(connectionIds)) {
                    return proofObservationRefresh;
                }
                throw new IllegalStateException(
                    "Fresh observation status is unavailable while a refresh is in progress"
                );
            }
            batch = execution.observationRefreshBatch(connectionIds);
            observationRefreshInProgress = true;
            proofObservationConnections = Set.copyOf(connectionIds);
            refresh = new CompletableFuture<>();
            proofObservationRefresh = refresh;
        }
        try {
            proofObservationExecutor.execute(() -> completeProofObservationRefresh(
                batch,
                connectionIds,
                refresh
            ));
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                observationRefreshInProgress = false;
                proofObservationRefresh = null;
                proofObservationConnections = Set.of();
            }
            refresh.completeExceptionally(failure);
        }
        return refresh;
    }

    private void completeProofObservationRefresh(
        RuntimeConnectionRegistry.ObservationBatch batch,
        Set<ConnectionId> connectionIds,
        CompletableFuture<Void> refresh
    ) {
        Throwable failure = null;
        RuntimeConnectionRegistry.ObservationResults results = null;
        try {
            results = batch.evaluate();
        } catch (RuntimeException | Error providerFailure) {
            failure = providerFailure;
        }
        synchronized (this) {
            try {
                if (failure == null && execution.state() == EnvironmentState.RUNNING) {
                    execution.applyObservationRefresh(results, connectionIds);
                }
            } catch (RuntimeException | Error commitFailure) {
                failure = commitFailure;
            } finally {
                observationRefreshInProgress = false;
                proofObservationRefresh = null;
                proofObservationConnections = Set.of();
            }
        }
        if (failure == null) {
            refresh.complete(null);
        } else {
            refresh.completeExceptionally(failure);
        }
    }
}
