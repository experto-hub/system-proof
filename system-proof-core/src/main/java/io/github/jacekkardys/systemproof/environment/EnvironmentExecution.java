package io.github.jacekkardys.systemproof.environment;

import java.util.Objects;
import java.util.Set;
import io.github.jacekkardys.systemproof.environment.state.EnvironmentState;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/** Coordinates the environment lifecycle and cleanup of its execution subsystems. */
final class EnvironmentExecution {
    private final EnvironmentLifecycle lifecycle;
    private final ComponentRuntimeSupervisor components;
    private final RuntimeConnectionRegistry connections;
    private final SemanticControlCoordinator controls;
    private final ProofExecutionCoordinator proofs;
    private final ProofSubjectRegistry proofSubjects;
    private final EnvironmentEventPublisher events;
    private final EnvironmentInspector inspector;

    EnvironmentExecution(
        EnvironmentLifecycle lifecycle,
        ComponentRuntimeSupervisor components,
        RuntimeConnectionRegistry connections,
        SemanticControlCoordinator controls,
        ProofExecutionCoordinator proofs,
        ProofSubjectRegistry proofSubjects,
        EnvironmentEventPublisher events,
        EnvironmentInspector inspector
    ) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle must not be null");
        this.components = Objects.requireNonNull(components, "components must not be null");
        this.connections = Objects.requireNonNull(connections, "connections must not be null");
        this.controls = Objects.requireNonNull(controls, "controls must not be null");
        this.proofs = Objects.requireNonNull(proofs, "proofs must not be null");
        this.proofSubjects = Objects.requireNonNull(
            proofSubjects,
            "proofSubjects must not be null"
        );
        this.events = Objects.requireNonNull(events, "events must not be null");
        this.inspector = Objects.requireNonNull(inspector, "inspector must not be null");
    }

    StartupFailure beginStart() {
        lifecycle.beginStart();
        try {
            connections.beginStartup();
            return null;
        } catch (RuntimeException | Error failure) {
            return handleStartupFailure(failure);
        }
    }

    StartStep nextStartStep() {
        try {
            if (components.startNext()) {
                return StartStep.pending(connections.startupObservationBatch());
            }
            connections.validateStartupComplete();
            lifecycle.markReady();
            return StartStep.completed();
        } catch (RuntimeException | Error failure) {
            return StartStep.failed(handleStartupFailure(failure));
        }
    }

    StartupFailure completeStartStep(
        RuntimeConnectionRegistry.ObservationResults observationResults,
        Throwable observationFailure
    ) {
        if (observationFailure != null) {
            failObservationMaterialization(observationFailure);
            return handleStartupFailure(observationFailure);
        }
        try {
            connections.applyStartupObservationResults(observationResults);
            return null;
        } catch (RuntimeException | Error failure) {
            failObservationMaterialization(failure);
            return handleStartupFailure(failure);
        }
    }

    EnvironmentState state() {
        return lifecycle.state();
    }

    RuntimeConnectionRegistry.ObservationBatch observationRefreshBatch() {
        return connections.observationRefreshBatch();
    }

    RuntimeConnectionRegistry.ObservationBatch observationRefreshBatch(
        Set<ConnectionId> connectionIds
    ) {
        return connections.observationRefreshBatch(connectionIds);
    }

    void applyObservationRefresh(
        RuntimeConnectionRegistry.ObservationResults observationResults
    ) {
        connections.applyObservationRefresh(observationResults);
    }

    void applyObservationRefresh(
        RuntimeConnectionRegistry.ObservationResults observationResults,
        Set<ConnectionId> connectionIds
    ) {
        connections.applyObservationRefresh(observationResults, connectionIds);
    }

    void close() {
        switch (lifecycle.beginClose()) {
            case ALREADY_STOPPED -> {
                return;
            }
            case STOP_DECLARED -> closeDeclaredExecution();
            case CLEAN_UP_RUNNING -> closeRunningExecution();
        }
    }

    private StartupFailure handleStartupFailure(Throwable failure) {
        lifecycle.markStartFailed();
        events.environmentStartupFailure(failure);
        Throwable cleanupFailure = cleanup();
        EnvironmentRuntimeFailures.accumulate(failure, cleanupFailure);
        lifecycle.markStopped();
        return new StartupFailure(failure, inspector.diagnosticsSnapshot());
    }

    private void failObservationMaterialization(Throwable primaryFailure) {
        try {
            connections.failObservationMaterialization(primaryFailure);
        } catch (RuntimeException | Error cleanupFailure) {
            EnvironmentRuntimeFailures.accumulate(primaryFailure, cleanupFailure);
        }
    }

    private void closeDeclaredExecution() {
        Throwable failure = completeProofEvaluation();
        failure = EnvironmentRuntimeFailures.accumulate(
            failure,
            completeControls()
        );
        failure = EnvironmentRuntimeFailures.accumulate(
            failure,
            attempt(connections::stopRemaining)
        );
        failure = EnvironmentRuntimeFailures.accumulate(
            failure,
            completeProofSubjects()
        );
        lifecycle.markStopped();
        if (failure != null) {
            EnvironmentRuntimeFailures.rethrowCleanupFailure(failure);
        }
    }

    private void closeRunningExecution() {
        Throwable failure = cleanup();
        if (failure != null) {
            lifecycle.markCleanupFailed();
        }
        lifecycle.markStopped();
        if (failure != null) {
            EnvironmentRuntimeFailures.rethrowCleanupFailure(failure);
        }
    }

    private Throwable cleanup() {
        Throwable firstFailure = completeProofEvaluation();
        firstFailure = EnvironmentRuntimeFailures.accumulate(
            firstFailure,
            completeControls()
        );
        firstFailure = EnvironmentRuntimeFailures.accumulate(
            firstFailure,
            attempt(components::stopStartedComponents)
        );
        firstFailure = EnvironmentRuntimeFailures.accumulate(
            firstFailure,
            attempt(connections::stopRemaining)
        );
        firstFailure = EnvironmentRuntimeFailures.accumulate(
            firstFailure,
            attempt(components::closeSharedResources)
        );
        return EnvironmentRuntimeFailures.accumulate(firstFailure, completeProofSubjects());
    }

    private Throwable completeProofEvaluation() {
        return proofs.completeExecution();
    }

    private Throwable completeProofSubjects() {
        try {
            proofSubjects.completeExecution();
            return null;
        } catch (RuntimeException | Error completionFailure) {
            return completionFailure;
        }
    }

    private Throwable completeControls() {
        try {
            controls.completeExecution();
            return null;
        } catch (RuntimeException | Error failure) {
            return failure;
        }
    }

    private static Throwable attempt(CleanupAction action) {
        try {
            return action.run();
        } catch (RuntimeException | Error failure) {
            return failure;
        }
    }

    @FunctionalInterface
    private interface CleanupAction {
        Throwable run();
    }

    record StartupFailure(
        Throwable cause,
        RuntimeDiagnostics.Snapshot diagnostics
    ) {
        StartupFailure {
            Objects.requireNonNull(cause, "cause must not be null");
            Objects.requireNonNull(diagnostics, "diagnostics must not be null");
        }
    }

    record StartStep(
        RuntimeConnectionRegistry.ObservationBatch observationBatch,
        StartupFailure failure,
        boolean complete
    ) {
        StartStep {
            int outcomes = (observationBatch == null ? 0 : 1)
                + (failure == null ? 0 : 1)
                + (complete ? 1 : 0);
            if (outcomes != 1) {
                throw new IllegalArgumentException(
                    "Start step must contain exactly one observation batch, failure, or completion"
                );
            }
        }

        static StartStep pending(
            RuntimeConnectionRegistry.ObservationBatch observationBatch
        ) {
            return new StartStep(
                Objects.requireNonNull(observationBatch, "observationBatch must not be null"),
                null,
                false
            );
        }

        static StartStep failed(StartupFailure failure) {
            return new StartStep(
                null,
                Objects.requireNonNull(failure, "failure must not be null"),
                false
            );
        }

        static StartStep completed() {
            return new StartStep(null, null, true);
        }
    }
}
