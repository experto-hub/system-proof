package io.github.jacekkardys.systemproof.environment;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import io.github.jacekkardys.systemproof.control.SemanticInteractionSelector;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardSpec;
import io.github.jacekkardys.systemproof.diagnostics.JournalRenderer;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.CorrelationKeySchema;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofPrerequisiteStatus;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/** Test-owned assembly retaining internal proof collaborators without production test seams. */
final class ProofRuntimeHarness implements AutoCloseable {
    final ManualDeadlineScheduler deadlines;
    final ProofExecutionCoordinator proofs;
    final ProofSubjectRegistry proofSubjects;
    final SemanticControlCoordinator controls;
    final EnvironmentEventPublisher events;
    final ScenarioJournal journal;
    final RuntimeConnectionRegistry connections;
    final EnvironmentExecution execution;
    final ProofTestFixture.RouteProvider route;
    final ConnectionId connectionId;
    final ProofSubjectRef subject;
    final CorrelationKey key;
    final CorrelationKey successorKey;

    private ProofRuntimeHarness(
        ProofOutcomeEvaluator evaluator,
        SemanticControlCoordinator.TimeoutScheduler controlTimeouts
    ) {
        this(new ManualDeadlineScheduler(), evaluator, controlTimeouts);
    }

    private ProofRuntimeHarness(
        ManualDeadlineScheduler deadlineScheduler,
        ProofOutcomeEvaluator evaluator,
        SemanticControlCoordinator.TimeoutScheduler controlTimeouts
    ) {
        this(deadlineScheduler, evaluator, controlTimeouts, BoundaryHooks.NONE);
    }

    private ProofRuntimeHarness(
        ManualDeadlineScheduler deadlineScheduler,
        ProofOutcomeEvaluator evaluator,
        SemanticControlCoordinator.TimeoutScheduler controlTimeouts,
        BoundaryHooks boundaryHooks
    ) {
        this(deadlineScheduler, evaluator, controlTimeouts, boundaryHooks, null);
    }

    private ProofRuntimeHarness(
        ManualDeadlineScheduler deadlineScheduler,
        ProofOutcomeEvaluator evaluator,
        SemanticControlCoordinator.TimeoutScheduler controlTimeouts,
        BoundaryHooks boundaryHooks,
        SemanticControlCoordinator.CompletionDispatcher completionDispatcher
    ) {
        deadlines = java.util.Objects.requireNonNull(
            deadlineScheduler,
            "deadlineScheduler must not be null"
        );
        boundaryHooks = java.util.Objects.requireNonNull(
            boundaryHooks,
            "boundaryHooks must not be null"
        );
        proofs = new ProofExecutionCoordinator(deadlines, evaluator, boundaryHooks);
        route = new ProofTestFixture.RouteProvider();
        ProofTestFixture.Client client = new ProofTestFixture.Client();
        ProofTestFixture.Server server = new ProofTestFixture.Server();
        EnvironmentTopology topology = EnvironmentTopology.of(
            List.of(client, server),
            List.of(ConnectionFactory.create(client.api, server.api))
        );
        EnvironmentLogging logging = EnvironmentLogging.defaults();
        journal = ScenarioJournal.withoutDiagnosticTime();
        JournalRenderer renderer = new JournalRenderer();
        BoundaryHooks hooks = boundaryHooks;
        ProofFactObserver observedFacts = new ProofFactObserver() {
            @Override
            public void fact(io.github.jacekkardys.systemproof.journal.ScenarioEvent event) {
                hooks.beforeProofFact(event);
                proofs.fact(event);
            }

            @Override
            public <T> T factBatch(java.util.function.Supplier<T> action) {
                return proofs.factBatch(action);
            }

            @Override
            public void journalFailure(Throwable failure) {
                proofs.journalFailure(failure);
            }
        };
        ProofObservationListener observedProofState = new ProofObservationListener() {
            @Override
            public void observationChanged(
                io.github.jacekkardys.systemproof.environment.state.RuntimeConnectionSnapshot snapshot
            ) {
                proofs.observationChanged(snapshot);
            }

            @Override
            public void requiredObservationFailed(ConnectionId connectionId) {
                hooks.beforeRequiredObservationFailure(connectionId);
                proofs.requiredObservationFailed(connectionId);
            }

            @Override
            public void finalizePending() {
                proofs.finalizePending();
            }
        };
        events = new EnvironmentEventPublisher(
            journal,
            new JournalSlf4jEmitter(logging, renderer),
            observedFacts
        );
        proofSubjects = new ProofSubjectRegistry(events);
        SemanticControlCapabilityRegistry capabilities =
            new SemanticControlCapabilityRegistry();
        controls = completionDispatcher == null
            ? new SemanticControlCoordinator(
                events,
                proofSubjects,
                capabilities,
                controlTimeouts,
                observedProofState
            )
            : new SemanticControlCoordinator(
                events,
                proofSubjects,
                capabilities,
                controlTimeouts,
                observedProofState,
                completionDispatcher
            );
        connections = new RuntimeConnectionRegistry(
            topology.connections(),
            events,
            ConnectionRouting.routed(
                ProofTestFixture.API,
                ProofTestFixture.PROFILE,
                route
            ),
            controls,
            proofSubjects,
            capabilities,
            observedProofState,
            new ProofEvidenceWindowTracker(
                hooks::beforeEvidenceWindow,
                hooks::beforeInteractionWatermarkRecord
            )
        );
        proofs.bind(proofSubjects, controls, connections);
        ComponentExecutionPlan plan = ComponentExecutionPlan.create(
            topology.runtimeComponents(),
            topology::connectionFrom
        );
        RuntimeBindings bindings = new RuntimeBindings(connections);
        RuntimeDiagnostics diagnostics = new RuntimeDiagnostics(journal, renderer);
        EnvironmentLifecycle lifecycle = new EnvironmentLifecycle(events);
        ComponentRuntimeSupervisor components = new ComponentRuntimeSupervisor(
            plan,
            bindings,
            diagnostics,
            events
        );
        EnvironmentInspector inspector = new EnvironmentInspector(
            lifecycle,
            components,
            connections,
            diagnostics,
            journal,
            proofSubjects
        );
        execution = new EnvironmentExecution(
            lifecycle,
            components,
            connections,
            controls,
            proofs,
            proofSubjects,
            events,
            inspector
        );
        startExecution();
        connectionId = topology.connections().getFirst().id();
        subject = proofSubjects.create();
        key = key(1);
        successorKey = key(2);
        proofSubjects.arm(subject, key);
        proofSubjects.arm(subject, successorKey);
    }

    static ProofRuntimeHarness start() {
        return new ProofRuntimeHarness(
            ProofOutcomeEvaluator.failClosed(),
            new PassiveControlTimeoutScheduler()
        );
    }

    static ProofRuntimeHarness start(ProofOutcomeEvaluator evaluator) {
        return new ProofRuntimeHarness(
            evaluator,
            new PassiveControlTimeoutScheduler()
        );
    }

    static ProofRuntimeHarness startWithFailingControlScheduler() {
        return new ProofRuntimeHarness(
            ProofOutcomeEvaluator.failClosed(),
            new FailingControlTimeoutScheduler()
        );
    }

    static ProofRuntimeHarness startWithImmediateControlTimeout() {
        return new ProofRuntimeHarness(
            ProofOutcomeEvaluator.failClosed(),
            new ImmediateControlTimeoutScheduler()
        );
    }

    static ProofRuntimeHarness startWithManualControlTimeout(
        ManualControlTimeoutScheduler scheduler
    ) {
        return startWithControlTimeoutScheduler(scheduler);
    }

    static ProofRuntimeHarness startWithControlTimeoutScheduler(
        SemanticControlCoordinator.TimeoutScheduler scheduler
    ) {
        return new ProofRuntimeHarness(
            ProofOutcomeEvaluator.failClosed(),
            scheduler
        );
    }

    static ProofRuntimeHarness startWithFailingControlCancellation() {
        return new ProofRuntimeHarness(
            ProofOutcomeEvaluator.failClosed(),
            new FailingCancellationControlTimeoutScheduler()
        );
    }

    static ProofRuntimeHarness startWithDeadlineScheduler(
        ManualDeadlineScheduler scheduler
    ) {
        return new ProofRuntimeHarness(
            scheduler,
            ProofOutcomeEvaluator.failClosed(),
            new PassiveControlTimeoutScheduler()
        );
    }

    static ProofRuntimeHarness startWithBoundaryHooks(BoundaryHooks hooks) {
        return new ProofRuntimeHarness(
            new ManualDeadlineScheduler(),
            ProofOutcomeEvaluator.failClosed(),
            new PassiveControlTimeoutScheduler(),
            hooks
        );
    }

    static ProofRuntimeHarness startWithCompletionDispatcher(
        SemanticControlCoordinator.CompletionDispatcher completionDispatcher
    ) {
        return new ProofRuntimeHarness(
            new ManualDeadlineScheduler(),
            ProofOutcomeEvaluator.failClosed(),
            new PassiveControlTimeoutScheduler(),
            BoundaryHooks.NONE,
            completionDispatcher
        );
    }

    SemanticPredecessorGuard declareGuard() {
        SemanticInteractionSelector<String> predecessor = selector("predecessor");
        return controls.declareGuard(SemanticPredecessorGuardSpec.requiring(
            subject,
            io.github.jacekkardys.systemproof.control.SemanticPredecessorRequirement
                .confirmed(predecessor),
            selector("successor"),
            Duration.ofSeconds(30)
        ));
    }

    SemanticPredecessorGuard declareForwardedGuard() {
        SemanticInteractionSelector<String> predecessor = selector("predecessor");
        return controls.declareGuard(SemanticPredecessorGuardSpec.requiring(
            subject,
            io.github.jacekkardys.systemproof.control.SemanticPredecessorRequirement
                .forwarded(predecessor),
            selector("successor"),
            Duration.ofSeconds(30)
        ));
    }

    SemanticPredecessorGuard declareGuard(
        io.github.jacekkardys.systemproof.control.SemanticPredecessorBoundary boundary,
        Predicate<String> predecessor,
        Predicate<String> successor
    ) {
        io.github.jacekkardys.systemproof.control.SemanticPredecessorRequirement requirement =
            switch (boundary) {
                case CONFIRMED ->
                    io.github.jacekkardys.systemproof.control.SemanticPredecessorRequirement
                        .confirmed(selector(predecessor));
                case FORWARDED ->
                    io.github.jacekkardys.systemproof.control.SemanticPredecessorRequirement
                        .forwarded(selector(predecessor));
            };
        return controls.declareGuard(SemanticPredecessorGuardSpec.requiring(
            subject,
            requirement,
            selector(successor),
            Duration.ofSeconds(30)
        ));
    }

    SemanticPredecessorGuard declareNativeFlowGuard(
        CorrelationKey flowKey,
        Predicate<String> predecessor,
        Predicate<String> successor,
        String nativeReference
    ) {
        SemanticInteractionSelector<String> predecessorSelector = selector(predecessor)
            .through(
                flowKey,
                ProofTestFixture.NativeCodec.INSTANCE,
                ignored -> nativeReference
            );
        SemanticInteractionSelector<String> successorSelector = selector(successor)
            .through(
                flowKey,
                ProofTestFixture.NativeCodec.INSTANCE,
                ignored -> nativeReference
            );
        return controls.declareGuard(SemanticPredecessorGuardSpec.requiring(
            subject,
            io.github.jacekkardys.systemproof.control.SemanticPredecessorRequirement
                .forwarded(predecessorSelector),
            successorSelector,
            Duration.ofSeconds(30)
        ));
    }

    SemanticHold declareHold(String expected) {
        return controls.declareHold(selector(expected), Duration.ofSeconds(30));
    }

    SemanticHold declareHold(Predicate<String> predicate) {
        return controls.declareHold(selector(predicate), Duration.ofSeconds(30));
    }

    SemanticHold declareNativeFlowHold(
        CorrelationKey flowKey,
        Predicate<String> predicate,
        String nativeReference
    ) {
        return controls.declareHold(
            selector(predicate).through(
                flowKey,
                ProofTestFixture.NativeCodec.INSTANCE,
                ignored -> nativeReference
            ),
            Duration.ofSeconds(30)
        );
    }

    ProofExecution activate(ProofPlan plan) {
        return proofs.activate(plan, this::refreshObservation);
    }

    io.github.jacekkardys.systemproof.proof.ProofPrerequisite prerequisite() {
        return proofs.prerequisite(ProofPrerequisiteStatus.SATISFIED, null);
    }

    void publish(String value) {
        Recorded recorded = record(value);
        correlate(recorded, value);
        forward(recorded);
    }

    Recorded record(String value) {
        InteractionSession session = route.observations().openSession();
        io.github.jacekkardys.systemproof.observation.RecordedInteraction interaction =
            session.record(
                FlowDirection.CONSUMER_TO_PROVIDER,
                ProofTestFixture.TextCodec.INSTANCE,
                value
            );
        return new Recorded(session, interaction);
    }

    void correlate(Recorded recorded, String value) {
        correlate(
            recorded,
            "successor".equals(value) ? successorKey : key,
            value
        );
    }

    void correlate(Recorded recorded, CorrelationKey correlationKey, String value) {
        recorded.session().correlate(
            recorded.interaction().interactionRef(),
            CorrelationContribution.capture(
                correlationKey,
                ProofTestFixture.NativeCodec.INSTANCE,
                value
            )
        );
    }

    void forward(Recorded recorded) {
        try {
            io.github.jacekkardys.systemproof.observation.ForwardingPermit permit =
                route.coordinator().permit(recorded.interaction());
            if (permit.awaitDecision()
                == io.github.jacekkardys.systemproof.observation.ForwardingDecision.FORWARD) {
                permit.forwarded();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while publishing a proof fact", interrupted);
        }
    }

    record Recorded(
        InteractionSession session,
        io.github.jacekkardys.systemproof.observation.RecordedInteraction interaction
    ) {}

    interface BoundaryHooks extends ProofExecutionCoordinator.BoundaryObserver {
        BoundaryHooks NONE = new BoundaryHooks() {};

        default void beforeEvidenceWindow() {}

        default void beforeInteractionWatermarkRecord() {}

        default void beforeProofFact(
            io.github.jacekkardys.systemproof.journal.ScenarioEvent event
        ) {}

        default void beforeRequiredObservationFailure(ConnectionId connectionId) {}
    }

    void cleanupFailure() {
        proofs.fact(new io.github.jacekkardys.systemproof.journal.FailureEvent.ConnectionCleanup(
            connectionId,
            io.github.jacekkardys.systemproof.journal.FailureDetails.from(
                new CleanupFailure()
            )
        ));
    }

    void frameworkFailure() {
        proofs.journalFailure(new FrameworkFailure());
    }

    void gatewayFailure() {
        proofs.fact(new io.github.jacekkardys.systemproof.journal.FailureEvent
            .ConnectionMaterialization(
                connectionId,
                io.github.jacekkardys.systemproof.journal.FailureDetails.from(
                    new GatewayFailure()
                )
            ));
    }

    void adapterFailure() {
        route.failSampling(new AdapterFailure());
    }

    @Override
    public void close() {
        try {
            execution.close();
        } catch (IllegalStateException unfinished) {
            if (!unfinished.getMessage().contains("unfinished active proof execution")) {
                throw unfinished;
            }
        }
    }

    private SemanticInteractionSelector<String> selector(String expected) {
        return selector(expected::equals);
    }

    private SemanticInteractionSelector<String> selector(Predicate<String> predicate) {
        return SemanticInteractionSelector.matching(
            connectionId,
            FlowDirection.CONSUMER_TO_PROVIDER,
            ProofTestFixture.TextCodec.INSTANCE,
            predicate
        ).forSubject(subject);
    }

    private void startExecution() {
        EnvironmentExecution.StartupFailure failure = execution.beginStart();
        if (failure != null) {
            throw new AssertionError("Test proof runtime failed to begin startup", failure.cause());
        }
        while (true) {
            EnvironmentExecution.StartStep step = execution.nextStartStep();
            if (step.failure() != null) {
                throw new AssertionError(
                    "Test proof runtime failed during startup",
                    step.failure().cause()
                );
            }
            if (step.complete()) {
                return;
            }
            RuntimeConnectionRegistry.ObservationResults results =
                step.observationBatch().evaluate();
            failure = execution.completeStartStep(results, null);
            if (failure != null) {
                throw new AssertionError(
                    "Test proof runtime failed to complete startup",
                    failure.cause()
                );
            }
        }
    }

    private java.util.concurrent.CompletionStage<Void> refreshObservation(
        java.util.Set<ConnectionId> connectionIds
    ) {
        RuntimeConnectionRegistry.ObservationBatch batch =
            execution.observationRefreshBatch(connectionIds);
        execution.applyObservationRefresh(batch.evaluate(), connectionIds);
        return java.util.concurrent.CompletableFuture.completedFuture(null);
    }

    private static CorrelationKey key(int seed) {
        byte[] digest = new byte[16];
        java.util.Arrays.fill(digest, (byte) seed);
        return CorrelationKey.ofDigest(
            new CorrelationKeySchema("system-proof-test", "proof-race", 1),
            digest
        );
    }

    static final class ManualDeadlineScheduler
        implements ProofExecutionCoordinator.DeadlineScheduler {
        private final AtomicReference<ScheduledDeadline> scheduled = new AtomicReference<>();
        private final CountDownLatch cancellationEntered;
        private final CountDownLatch cancellationRelease;
        private final RuntimeException cancellationFailure;

        ManualDeadlineScheduler() {
            this(null, null, null);
        }

        ManualDeadlineScheduler(
            CountDownLatch cancellationEntered,
            CountDownLatch cancellationRelease,
            RuntimeException cancellationFailure
        ) {
            this.cancellationEntered = cancellationEntered;
            this.cancellationRelease = cancellationRelease;
            this.cancellationFailure = cancellationFailure;
        }

        @Override
        public ProofExecutionCoordinator.DeadlineTask schedule(
            Duration delay,
            Runnable action
        ) {
            ScheduledDeadline deadline = new ScheduledDeadline(action, this::cancel);
            if (!scheduled.compareAndSet(null, deadline)) {
                throw new IllegalStateException("Only one proof deadline is expected");
            }
            return deadline::cancel;
        }

        void fireRacingCallback() {
            java.util.Objects.requireNonNull(
                scheduled.get(),
                "proof deadline was not scheduled"
            ).fire();
        }

        @Override
        public void close() {}

        private void cancel() {
            if (cancellationEntered != null) {
                cancellationEntered.countDown();
            }
            if (cancellationRelease != null) {
                try {
                    if (!cancellationRelease.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError(
                            "Deadline cancellation release was not signalled"
                        );
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(
                        "Interrupted while awaiting deadline cancellation release",
                        interrupted
                    );
                }
            }
            if (cancellationFailure != null) {
                throw cancellationFailure;
            }
        }
    }

    private static final class ScheduledDeadline {
        private final Runnable action;
        private final Runnable cancellation;

        private ScheduledDeadline(Runnable action, Runnable cancellation) {
            this.action = java.util.Objects.requireNonNull(action, "action must not be null");
            this.cancellation = java.util.Objects.requireNonNull(
                cancellation,
                "cancellation must not be null"
            );
        }

        private void cancel() {
            cancellation.run();
        }

        private void fire() {
            action.run();
        }
    }

    private static final class FrameworkFailure extends RuntimeException {}

    private static final class CleanupFailure extends RuntimeException {}

    private static final class GatewayFailure extends RuntimeException {}

    private static final class AdapterFailure extends RuntimeException {}

    private static final class FailingControlTimeoutScheduler
        implements SemanticControlCoordinator.TimeoutScheduler {
        @Override
        public SemanticControlCoordinator.TimeoutTask schedule(
            Duration delay,
            Runnable action
        ) {
            throw new ControlSchedulingFailure();
        }

        @Override
        public void close() {}
    }

    private static final class PassiveControlTimeoutScheduler
        implements SemanticControlCoordinator.TimeoutScheduler {
        @Override
        public SemanticControlCoordinator.TimeoutTask schedule(
            Duration delay,
            Runnable action
        ) {
            return () -> {};
        }

        @Override
        public void close() {}
    }

    static final class ManualControlTimeoutScheduler
        implements SemanticControlCoordinator.TimeoutScheduler {
        private final AtomicReference<Runnable> scheduled = new AtomicReference<>();

        @Override
        public SemanticControlCoordinator.TimeoutTask schedule(
            Duration delay,
            Runnable action
        ) {
            if (!scheduled.compareAndSet(null, action)) {
                throw new IllegalStateException("Only one control timeout is expected");
            }
            return () -> scheduled.compareAndSet(action, null);
        }

        void fire() {
            java.util.Objects.requireNonNull(
                scheduled.getAndSet(null),
                "control timeout was not scheduled"
            ).run();
        }

        @Override
        public void close() {
            scheduled.set(null);
        }
    }

    private static final class ImmediateControlTimeoutScheduler
        implements SemanticControlCoordinator.TimeoutScheduler {
        @Override
        public SemanticControlCoordinator.TimeoutTask schedule(
            Duration delay,
            Runnable action
        ) {
            action.run();
            return () -> {};
        }

        @Override
        public void close() {}
    }

    private static final class FailingCancellationControlTimeoutScheduler
        implements SemanticControlCoordinator.TimeoutScheduler {
        @Override
        public SemanticControlCoordinator.TimeoutTask schedule(
            Duration delay,
            Runnable action
        ) {
            return () -> {
                throw new ControlCancellationFailure();
            };
        }

        @Override
        public void close() {}
    }

    private static final class ControlSchedulingFailure extends RuntimeException {}

    static final class DeadlineCancellationFailure extends RuntimeException {}

    static final class ControlCancellationFailure extends RuntimeException {}
}
