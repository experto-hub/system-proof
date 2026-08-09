package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;
import io.github.jacekkardys.systemproof.journal.SemanticPredecessorGuardEvent;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofExecutionState;
import io.github.jacekkardys.systemproof.proof.ProofFailureStage;
import io.github.jacekkardys.systemproof.proof.ProofObligationResolution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.proof.ProofResult;

class ProofDeadlineFinalizationReadinessTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);

    @RepeatedTest(20)
    void shouldPublishViolationOnlyAfterBlockedDeadlineInstallationCompletes()
        throws Exception {
        assertViolationRace(new BlockingDeadlineScheduler(), false);
    }

    @RepeatedTest(20)
    void shouldPublishObservationErrorOnlyAfterBlockedDeadlineInstallationCompletes()
        throws Exception {
        BlockingDeadlineScheduler scheduler = new BlockingDeadlineScheduler();
        RaceHooks hooks = new RaceHooks();
        try (ProofRuntimeHarness harness =
                ProofRuntimeHarness.startWithDeadlineSchedulerAndBoundaryHooks(
                    scheduler,
                    hooks
                );
             ExecutorService executor = Executors.newFixedThreadPool(4)) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            AtomicReference<ProofResult> callbackResult = new AtomicReference<>();
            CompletableFuture<Void> callback = guard.completion().thenRun(() ->
                callbackResult.set(hooks.execution().result())
            ).toCompletableFuture();
            Future<ProofExecution> activation = executor.submit(() ->
                harness.activate(guardPlan(harness, guard, "deadline-observation-error"))
            );
            scheduler.awaitEntered();
            ProofExecution execution = hooks.execution();

            Future<?> failure = executor.submit(() ->
                harness.controls.observationFailed(harness.connectionId)
            );
            hooks.awaitOutcomeSelected();
            Future<ProofResult> resultAccess = executor.submit(execution::result);
            Future<ProofResult> evaluation = executor.submit(execution::evaluate);

            assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.FAILED);
            assertThat(callback.isDone()).isFalse();
            assertThat(resultAccess.isDone()).isFalse();
            assertThat(evaluation.isDone()).isFalse();

            scheduler.release();
            assertThat(activation.get(5, TimeUnit.SECONDS)).isSameAs(execution);
            failure.get(5, TimeUnit.SECONDS);
            ProofResult result = resultAccess.get(5, TimeUnit.SECONDS);
            assertThat(evaluation.get(5, TimeUnit.SECONDS)).isSameAs(result);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            callback.get(5, TimeUnit.SECONDS);
            assertThat(callbackResult.get()).isSameAs(result);
            assertThat(scheduler.installations()).isOne();
            assertThat(scheduler.cancellations()).isOne();
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        } finally {
            scheduler.release();
        }
    }

    @RepeatedTest(20)
    void shouldRetainDeadlineInstallationFailureBehindAnEarlierViolation()
        throws Exception {
        assertViolationRace(
            new BlockingDeadlineScheduler(new InjectedDeadlineInstallationFailure()),
            true
        );
    }

    @RepeatedTest(20)
    void shouldFinalizeNormallyWhenDeadlineInstallationWinsTheRace()
        throws Exception {
        BlockingDeadlineScheduler scheduler = new BlockingDeadlineScheduler();
        scheduler.release();
        RaceHooks hooks = new RaceHooks();
        try (ProofRuntimeHarness harness =
                ProofRuntimeHarness.startWithDeadlineSchedulerAndBoundaryHooks(
                    scheduler,
                    hooks
                )) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            AtomicReference<ProofResult> callbackResult = new AtomicReference<>();
            CompletableFuture<Void> callback = guard.completion().thenRun(() ->
                callbackResult.set(hooks.execution().result())
            ).toCompletableFuture();
            ProofExecution execution = harness.activate(
                guardPlan(harness, guard, "deadline-installed-first")
            );
            ProofRuntimeHarness.Recorded successor = harness.record("successor");
            harness.correlate(successor, "successor");

            ForwardingPermit permit = harness.route.coordinator().permit(
                successor.interaction()
            );

            assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            ProofResult result = execution.result();
            assertViolation(result, guard, harness);
            callback.get(5, TimeUnit.SECONDS);
            assertThat(callbackResult.get()).isSameAs(result);
            assertThat(scheduler.installations()).isOne();
            assertThat(scheduler.cancellations()).isOne();
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    @Test
    void shouldSelectErrorForNonFatalDeadlineInstallationFailureWithoutAPrimary() {
        BlockingDeadlineScheduler scheduler = new BlockingDeadlineScheduler(
            new InjectedDeadlineInstallationFailure()
        );
        scheduler.release();
        RaceHooks hooks = new RaceHooks();
        try (ProofRuntimeHarness harness =
                ProofRuntimeHarness.startWithDeadlineSchedulerAndBoundaryHooks(
                    scheduler,
                    hooks
                )) {
            ProofExecution execution = harness.activate(
                basePlan(harness, "deadline-installation-primary-error").build()
            );

            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).hasValueSatisfying(diagnostic ->
                assertThat(diagnostic.stage()).isEqualTo(ProofFailureStage.ACTIVATION)
            );
            assertThat(scheduler.installations()).isZero();
            assertThat(scheduler.cancellations()).isZero();
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    @Test
    void shouldPropagateFatalDeadlineInstallationFailureWithoutProofMutation()
        throws Exception {
        InjectedDeadlineLinkageError fatal = new InjectedDeadlineLinkageError();
        BlockingDeadlineScheduler scheduler = new BlockingDeadlineScheduler(fatal);
        scheduler.release();
        RaceHooks hooks = new RaceHooks();
        try (ProofRuntimeHarness harness =
                ProofRuntimeHarness.startWithDeadlineSchedulerAndBoundaryHooks(
                    scheduler,
                    hooks
                );
             ExecutorService executor = Executors.newSingleThreadExecutor()) {
            ProofPlan plan = basePlan(harness, "fatal-deadline-installation").build();
            Future<ProofExecution> activation = executor.submit(() -> harness.activate(plan));

            assertThatThrownFuture(activation, fatal);
            ProofExecution execution = hooks.execution();
            assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);
            assertUnpublished(execution);
            assertThat(scheduler.installations()).isZero();
            assertThat(scheduler.cancellations()).isZero();
        }
    }

    private static void assertViolationRace(
        BlockingDeadlineScheduler scheduler,
        boolean installationFails
    ) throws Exception {
        RaceHooks hooks = new RaceHooks();
        try (ProofRuntimeHarness harness =
                ProofRuntimeHarness.startWithDeadlineSchedulerAndBoundaryHooks(
                    scheduler,
                    hooks
                );
             ExecutorService executor = Executors.newFixedThreadPool(4)) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            AtomicReference<ProofResult> callbackResult = new AtomicReference<>();
            CompletableFuture<Void> callback = guard.completion().thenRun(() ->
                callbackResult.set(hooks.execution().result())
            ).toCompletableFuture();
            Future<ProofExecution> activation = executor.submit(() -> harness.activate(
                guardPlan(
                    harness,
                    guard,
                    installationFails
                        ? "deadline-installation-failure-after-violation"
                        : "deadline-installation-after-violation"
                )
            ));
            scheduler.awaitEntered();
            ProofExecution execution = hooks.execution();
            ProofRuntimeHarness.Recorded successor = harness.record("successor");
            harness.correlate(successor, "successor");

            Future<ForwardingPermit> permitAccess = executor.submit(() ->
                harness.route.coordinator().permit(successor.interaction())
            );
            hooks.awaitOutcomeSelected();
            Future<ProofResult> resultAccess = executor.submit(execution::result);
            Future<ProofResult> evaluation = executor.submit(execution::evaluate);

            assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
            assertThat(callback.isDone()).isFalse();
            assertThat(resultAccess.isDone()).isFalse();
            assertThat(evaluation.isDone()).isFalse();
            assertThat(events(harness, SemanticPredecessorGuardEvent.class))
                .filteredOn(event -> event.guardRef().equals(guard.ref()))
                .filteredOn(event -> event.state()
                    == SemanticPredecessorGuardState.VIOLATED)
                .filteredOn(event -> event.decision()
                    .equals(Optional.of(ForwardingDecision.CLOSE_SESSION)))
                .singleElement();

            scheduler.release();
            assertThat(activation.get(5, TimeUnit.SECONDS)).isSameAs(execution);
            ForwardingPermit permit = permitAccess.get(5, TimeUnit.SECONDS);
            assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            ProofResult result = resultAccess.get(5, TimeUnit.SECONDS);
            assertThat(evaluation.get(5, TimeUnit.SECONDS)).isSameAs(result);
            assertViolation(result, guard, harness);
            callback.get(5, TimeUnit.SECONDS);
            assertThat(callbackResult.get()).isSameAs(result);
            if (installationFails) {
                assertThat(scheduler.installations()).isZero();
                assertThat(scheduler.cancellations()).isZero();
                assertThat(result.secondaryDiagnostics())
                    .anyMatch(diagnostic -> diagnostic.stage()
                        == ProofFailureStage.ACTIVATION);
            } else {
                assertThat(scheduler.installations()).isOne();
                assertThat(scheduler.cancellations()).isOne();
            }
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        } finally {
            scheduler.release();
        }
    }

    private static void assertViolation(
        ProofResult result,
        SemanticPredecessorGuard guard,
        ProofRuntimeHarness harness
    ) {
        assertThat(result.outcome()).isEqualTo(ProofOutcome.VIOLATED);
        assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
        ProofObligationResolution resolution = resolution(result, "guard");
        assertThat(resolution.resolution()).isEqualTo(ProofResolution.VIOLATED);
        assertThat(resolution.reason())
            .isEqualTo(ProofResolutionReason.CAUSAL_RELATION_VIOLATED);
        assertThat(events(harness, SemanticPredecessorGuardEvent.class))
            .filteredOn(event -> event.guardRef().equals(guard.ref()))
            .filteredOn(event -> event.state()
                == SemanticPredecessorGuardState.VIOLATED)
            .singleElement();
    }

    private static ProofPlan guardPlan(
        ProofRuntimeHarness harness,
        SemanticPredecessorGuard guard,
        String id
    ) {
        return basePlan(harness, id)
            .control("guard", guard, SemanticPredecessorGuardState.SATISFIED)
            .build();
    }

    private static ProofPlan.Builder basePlan(
        ProofRuntimeHarness harness,
        String id
    ) {
        return ProofPlan.builder(id, "Deadline finalization readiness", harness.subject, DEADLINE)
            .prerequisite("prerequisite", harness.prerequisite())
            .observation("observation", harness.connectionId, ProofTestFixture.PROFILE);
    }

    private static ProofObligationResolution resolution(ProofResult result, String id) {
        return result.resolutions().stream()
            .filter(value -> value.id().value().equals(id))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Missing proof resolution " + id));
    }

    private static <T extends ScenarioEvent> List<T> events(
        ProofRuntimeHarness harness,
        Class<T> eventType
    ) {
        return harness.journal.snapshot().entries().stream()
            .map(entry -> entry.event())
            .filter(eventType::isInstance)
            .map(eventType::cast)
            .toList();
    }

    private static void assertUnpublished(ProofExecution execution) {
        ProofExecutionCoordinator.PublicationInvariant invariant =
            ProofExecutionCoordinator.publicationInvariant(execution);
        assertThat(invariant.outcomeSelected()).isFalse();
        assertThat(invariant.resultReadyCompletedNormally()).isFalse();
        assertThat(invariant.finalizationReadyCompletedNormally()).isFalse();
        assertThat(invariant.finalizationComplete()).isFalse();
        assertThat(invariant.finalizing()).isFalse();
        assertThat(invariant.finalizationOwnerPresent()).isFalse();
        assertThat(invariant.authoritativeOutcomeBoundaryPending()).isFalse();
        assertThat(invariant.deadlineInstallationReadinessPresent()).isFalse();
        assertThat(invariant.authoritativeOperationOwnerPresent()).isFalse();
        assertThat(invariant.factBatchActive()).isFalse();
        assertThat(invariant.pendingCompletionPresent()).isFalse();
        assertThat(invariant.resultConstructionRecoveryCount()).isZero();
    }

    private static void assertThatThrownFuture(Future<?> future, Throwable expected)
        throws Exception {
        try {
            future.get(5, TimeUnit.SECONDS);
            throw new AssertionError("Expected fatal deadline scheduler failure");
        } catch (ExecutionException failure) {
            assertThat(failure.getCause()).isSameAs(expected);
        }
    }

    private static final class RaceHooks implements ProofRuntimeHarness.BoundaryHooks {
        private final AtomicReference<ProofExecution> execution = new AtomicReference<>();
        private final CountDownLatch deadlineStarted = new CountDownLatch(1);
        private final CountDownLatch outcomeSelected = new CountDownLatch(1);

        @Override
        public void deadlineInstallationStarted(ProofExecution value) {
            execution.set(value);
            deadlineStarted.countDown();
        }

        @Override
        public void authoritativeOutcomeSelectedBeforeFinalization() {
            outcomeSelected.countDown();
        }

        private ProofExecution execution() {
            await(deadlineStarted, "deadline installation start");
            return java.util.Objects.requireNonNull(
                execution.get(),
                "proof execution was not exposed"
            );
        }

        private void awaitOutcomeSelected() {
            await(outcomeSelected, "authoritative outcome selection");
        }
    }

    private static final class BlockingDeadlineScheduler
        extends ProofRuntimeHarness.ManualDeadlineScheduler {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final Throwable failure;
        private final AtomicInteger installations = new AtomicInteger();
        private final AtomicInteger cancellations = new AtomicInteger();

        private BlockingDeadlineScheduler() {
            this(null);
        }

        private BlockingDeadlineScheduler(Throwable failure) {
            this.failure = failure;
        }

        @Override
        public ProofExecutionCoordinator.DeadlineTask schedule(
            Duration delay,
            Runnable action
        ) {
            entered.countDown();
            await(release, "deadline scheduler release");
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            ProofExecutionCoordinator.DeadlineTask installed = super.schedule(delay, action);
            installations.incrementAndGet();
            return () -> {
                cancellations.incrementAndGet();
                installed.cancel();
            };
        }

        private void awaitEntered() {
            await(entered, "deadline scheduler entry");
        }

        private void release() {
            release.countDown();
        }

        private int installations() {
            return installations.get();
        }

        private int cancellations() {
            return cancellations.get();
        }
    }

    private static void await(CountDownLatch latch, String description) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out awaiting " + description);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while awaiting " + description, interrupted);
        }
    }

    private static final class InjectedDeadlineInstallationFailure
        extends RuntimeException {}

    private static final class InjectedDeadlineLinkageError extends LinkageError {}
}
