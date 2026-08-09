package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;
import io.github.jacekkardys.systemproof.journal.SemanticPredecessorGuardEvent;
import io.github.jacekkardys.systemproof.proof.ProofEvaluationState;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.CorrelationKeySchema;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofObligationResolution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofRequirementKind;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.topology.ConnectionId;
import org.junit.jupiter.api.RepeatedTest;

class ProofExecutionLinearizationBoundaryTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);

    @RepeatedTest(20)
    void shouldExcludeAnInteractionRecordedBeforeTheWindowWhenCorrelationArrivesBeforeActive()
        throws Exception {
        assertPreWindowCorrelationExcluded(false);
    }

    @RepeatedTest(20)
    void shouldExcludeAnInteractionRecordedBeforeTheWindowWhenCorrelationArrivesAfterActive()
        throws Exception {
        assertPreWindowCorrelationExcluded(true);
    }

    @RepeatedTest(20)
    void shouldAdmitAnInteractionThatStartsRecordingAfterTheWatermarkIsCaptured()
        throws Exception {
        CountDownLatch windowCaptured = new CountDownLatch(1);
        CountDownLatch activationRelease = new CountDownLatch(1);
        CountDownLatch interactionRecordAttempted = new CountDownLatch(1);
        AtomicBoolean observeRecordAttempt = new AtomicBoolean(false);
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void evidenceWindowCaptured() {
                windowCaptured.countDown();
                await(activationRelease);
            }

            @Override
            public void beforeInteractionWatermarkRecord() {
                if (observeRecordAttempt.get()) {
                    interactionRecordAttempted.countDown();
                }
            }
        };
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks);
             ExecutorService executor = Executors.newFixedThreadPool(2)) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            CorrelationKey key = preWindowKey();
            harness.proofSubjects.arm(harness.subject, key);
            Future<ProofExecution> activating = executor.submit(() ->
                harness.activate(correlationAndGuardPlan(harness, guard, key))
            );
            await(windowCaptured);

            observeRecordAttempt.set(true);
            Future<?> observation = executor.submit(() -> {
                ProofRuntimeHarness.Recorded recorded = harness.record("post-watermark");
                harness.correlate(recorded, key, "post-watermark");
            });
            await(interactionRecordAttempted);
            assertThat(observation).isNotDone();

            activationRelease.countDown();
            ProofExecution execution = get(activating);
            get(observation);
            execution.runStimulus(() -> {
                harness.publish("predecessor");
                harness.publish("successor");
            });

            ProofResult result = execution.evaluate();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.PROVED);
            assertThat(result.resolutions()).allMatch(
                value -> value.resolution() == ProofResolution.SATISFIED
            );
        }
    }

    @RepeatedTest(20)
    void shouldLetRequiredObservationFailureLinearizeBeforeDeadline() throws Exception {
        CountDownLatch markerEntered = new CountDownLatch(1);
        CountDownLatch markerRelease = new CountDownLatch(1);
        AtomicBoolean blockOnce = new AtomicBoolean(true);
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void beforeRequiredObservationFailure(ConnectionId connectionId) {
                if (blockOnce.compareAndSet(true, false)) {
                    markerEntered.countDown();
                    await(markerRelease);
                }
            }
        };
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks);
             ExecutorService executor = Executors.newFixedThreadPool(2)) {
            ProofExecution execution = activatedPrerequisite(harness, "observation-before-deadline");
            execution.runStimulus(() -> {});

            Future<?> failure = executor.submit(() ->
                harness.controls.observationFailed(harness.connectionId)
            );
            await(markerEntered);
            Future<?> deadline = executor.submit(harness.deadlines::fireRacingCallback);
            markerRelease.countDown();

            get(failure);
            get(deadline);
            assertThat(execution.result().outcome()).isEqualTo(ProofOutcome.ERROR);
        }
    }

    @RepeatedTest(20)
    void shouldLetDeadlineLinearizeBeforeRequiredObservationFailure() throws Exception {
        CountDownLatch deadlineEntered = new CountDownLatch(1);
        CountDownLatch deadlineRelease = new CountDownLatch(1);
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void deadlineBoundaryReached() {
                deadlineEntered.countDown();
                await(deadlineRelease);
            }
        };
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks);
             ExecutorService executor = Executors.newFixedThreadPool(2)) {
            ProofExecution execution = activatedPrerequisite(harness, "deadline-before-observation");
            execution.runStimulus(() -> {});

            Future<?> deadline = executor.submit(harness.deadlines::fireRacingCallback);
            await(deadlineEntered);
            Future<?> failure = executor.submit(() ->
                harness.controls.observationFailed(harness.connectionId)
            );
            deadlineRelease.countDown();

            get(deadline);
            get(failure);
            assertDeadlineGap(execution.result());
        }
    }

    @RepeatedTest(20)
    void shouldLetGuardViolationLinearizeBeforeDeadline() throws Exception {
        CountDownLatch factEntered = new CountDownLatch(1);
        CountDownLatch factRelease = new CountDownLatch(1);
        AtomicBoolean blockOnce = new AtomicBoolean(true);
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void beforeProofFact(ScenarioEvent event) {
                if (isViolation(event) && blockOnce.compareAndSet(true, false)) {
                    factEntered.countDown();
                    await(factRelease);
                }
            }
        };
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks);
             ExecutorService executor = Executors.newFixedThreadPool(2)) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));

            Future<?> stimulus = executor.submit(() ->
                execution.runStimulus(() -> harness.publish("successor"))
            );
            await(factEntered);
            Future<?> deadline = executor.submit(harness.deadlines::fireRacingCallback);
            factRelease.countDown();

            get(stimulus);
            get(deadline);
            assertThat(execution.result().outcome()).isEqualTo(ProofOutcome.VIOLATED);
        }
    }

    @RepeatedTest(20)
    void shouldLetDeadlineLinearizeBeforeGuardViolation() throws Exception {
        CountDownLatch deadlineEntered = new CountDownLatch(1);
        CountDownLatch deadlineRelease = new CountDownLatch(1);
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void deadlineBoundaryReached() {
                deadlineEntered.countDown();
                await(deadlineRelease);
            }
        };
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks);
             ExecutorService executor = Executors.newFixedThreadPool(2)) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));

            Future<?> deadline = executor.submit(harness.deadlines::fireRacingCallback);
            await(deadlineEntered);
            Future<?> stimulus = executor.submit(() ->
                execution.runStimulus(() -> harness.publish("successor"))
            );
            deadlineRelease.countDown();

            get(deadline);
            get(stimulus);
            assertDeadlineGap(execution.result());
        }
    }

    @RepeatedTest(20)
    void shouldExposeEvaluationDeadlineGapWhenEveryObligationIsSatisfied() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = activatedPrerequisite(harness, "all-satisfied-deadline");
            execution.runStimulus(() -> {});

            harness.deadlines.fireRacingCallback();
            ProofResult result = execution.result();

            assertDeadlineGap(result);
            assertThat(result.resolutions())
                .allMatch(value -> value.resolution() == ProofResolution.SATISFIED);
            assertThat(result.report().content())
                .contains("decisive=EVALUATION/DEADLINE_EXPIRED");
        }
    }

    @RepeatedTest(20)
    void shouldLetExplicitEvaluationLinearizeBeforeDeadline() throws Exception {
        CountDownLatch evaluationEntered = new CountDownLatch(1);
        CountDownLatch evaluationRelease = new CountDownLatch(1);
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void evaluationBoundaryReached() {
                evaluationEntered.countDown();
                await(evaluationRelease);
            }
        };
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks);
             ExecutorService executor = Executors.newFixedThreadPool(2)) {
            ProofExecution execution = activatedPrerequisite(harness, "evaluation-before-deadline");
            execution.runStimulus(() -> {});

            Future<ProofResult> evaluation = executor.submit(execution::evaluate);
            await(evaluationEntered);
            Future<?> deadline = executor.submit(harness.deadlines::fireRacingCallback);
            evaluationRelease.countDown();

            ProofResult result = get(evaluation);
            get(deadline);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.PROVED);
            assertThat(execution.result()).isSameAs(result);
        }
    }

    @RepeatedTest(20)
    void shouldLetDeadlineLinearizeBeforeExplicitEvaluation() throws Exception {
        CountDownLatch deadlineEntered = new CountDownLatch(1);
        CountDownLatch deadlineRelease = new CountDownLatch(1);
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void deadlineBoundaryReached() {
                deadlineEntered.countDown();
                await(deadlineRelease);
            }
        };
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks);
             ExecutorService executor = Executors.newFixedThreadPool(2)) {
            ProofExecution execution = activatedPrerequisite(harness, "deadline-before-evaluation");
            execution.runStimulus(() -> {});

            Future<?> deadline = executor.submit(harness.deadlines::fireRacingCallback);
            await(deadlineEntered);
            Future<ProofResult> evaluation = executor.submit(execution::evaluate);
            deadlineRelease.countDown();

            get(deadline);
            ProofResult result = get(evaluation);
            assertDeadlineGap(result);
            assertThat(execution.result()).isSameAs(result);
        }
    }

    @RepeatedTest(20)
    void shouldFreezeResultBeforeDeliveringUnreachedControlCompletions() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticHold hold = harness.declareHold("held");
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(controlsPlan(harness, hold, guard));
            CountDownLatch callbacks = new CountDownLatch(2);
            AtomicReference<ProofResult> holdResult = new AtomicReference<>();
            AtomicReference<ProofResult> guardResult = new AtomicReference<>();
            hold.completion().thenAccept(ignored -> {
                holdResult.set(execution.result());
                assertThat(execution.evaluate()).isSameAs(holdResult.get());
                callbacks.countDown();
            });
            guard.completion().thenAccept(ignored -> {
                guardResult.set(execution.result());
                assertThat(execution.evaluate()).isSameAs(guardResult.get());
                callbacks.countDown();
            });

            harness.frameworkFailure();
            ProofResult result = execution.result();
            await(callbacks);

            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(holdResult.get()).isSameAs(result);
            assertThat(guardResult.get()).isSameAs(result);
        }
    }

    @RepeatedTest(20)
    void shouldPreserveSatisfiedRequirementsDeclaredAfterTheViolation() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofPlan plan = ProofPlan.builder(
                "satisfied-after-decisive",
                "Satisfied after decisive",
                harness.subject,
                DEADLINE
            ).observation(
                "observation",
                harness.connectionId,
                ProofTestFixture.PROFILE
            ).control(
                "decisive-guard",
                guard,
                SemanticPredecessorGuardState.SATISFIED
            ).prerequisite(
                "later-satisfied-prerequisite",
                harness.prerequisite()
            ).build();
            ProofExecution execution = harness.activate(plan);

            execution.runStimulus(() -> harness.publish("successor"));
            ProofResult result = execution.result();

            assertThat(result.outcome()).isEqualTo(ProofOutcome.VIOLATED);
            assertThat(result.resolutions().stream()
                .filter(value -> value.id().toString().equals("later-satisfied-prerequisite"))
                .map(ProofObligationResolution::resolution))
                .containsExactly(ProofResolution.SATISFIED);
        }
    }

    private static void assertPreWindowCorrelationExcluded(boolean delayedCorrelation)
        throws Exception {
        CountDownLatch windowEntered = new CountDownLatch(1);
        CountDownLatch windowRelease = new CountDownLatch(1);
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void beforeEvidenceWindow() {
                windowEntered.countDown();
                await(windowRelease);
            }
        };
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks);
             ExecutorService executor = Executors.newSingleThreadExecutor()) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            CorrelationKey preWindowKey = preWindowKey();
            harness.proofSubjects.arm(harness.subject, preWindowKey);
            Future<ProofExecution> activating = executor.submit(() ->
                harness.activate(correlationAndGuardPlan(harness, guard, preWindowKey))
            );
            await(windowEntered);
            ProofRuntimeHarness.Recorded preWindow = harness.record("pre-window");
            if (!delayedCorrelation) {
                harness.correlate(preWindow, preWindowKey, "pre-window");
            }
            windowRelease.countDown();
            ProofExecution execution = get(activating);
            if (delayedCorrelation) {
                harness.correlate(preWindow, preWindowKey, "pre-window");
            }

            execution.runStimulus(() -> {
                harness.publish("predecessor");
                harness.publish("successor");
            });
            ProofResult result = execution.evaluate();

            assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            assertThat(result.resolutions().stream()
                .filter(value -> value.kind() == ProofRequirementKind.CORRELATION))
                .singleElement()
                .satisfies(value -> {
                    assertThat(value.resolution()).isEqualTo(ProofResolution.MISSING);
                    assertThat(value.interactions()).isEmpty();
                });
            assertThat(result.resolutions().stream()
                .filter(value -> value.kind() != ProofRequirementKind.CORRELATION))
                .allMatch(value -> value.resolution() == ProofResolution.SATISFIED);
        }
    }

    private static ProofExecution activatedPrerequisite(
        ProofRuntimeHarness harness,
        String id
    ) {
        return harness.activate(ProofPlan.builder(
            id,
            "Boundary prerequisite",
            harness.subject,
            DEADLINE
        ).prerequisite(
            "prerequisite",
            harness.prerequisite()
        ).observation(
            "observation",
            harness.connectionId,
            ProofTestFixture.PROFILE
        ).build());
    }

    private static ProofPlan guardPlan(
        ProofRuntimeHarness harness,
        SemanticPredecessorGuard guard
    ) {
        return ProofPlan.builder(
            "guard-deadline-race",
            "Guard deadline race",
            harness.subject,
            DEADLINE
        ).observation(
            "observation",
            harness.connectionId,
            ProofTestFixture.PROFILE
        ).control(
            "guard",
            guard,
            SemanticPredecessorGuardState.SATISFIED
        ).build();
    }

    private static ProofPlan correlationAndGuardPlan(
        ProofRuntimeHarness harness,
        SemanticPredecessorGuard guard,
        CorrelationKey preWindowKey
    ) {
        return ProofPlan.builder(
            "evidence-window-correlation",
            "Evidence window correlation",
            harness.subject,
            DEADLINE
        ).prerequisite(
            "prerequisite",
            harness.prerequisite()
        ).observation(
            "observation",
            harness.connectionId,
            ProofTestFixture.PROFILE
        ).correlation(
            "correlation",
            harness.connectionId,
            preWindowKey,
            ProofTestFixture.NATIVE_SCHEMA
        ).control(
            "guard",
            guard,
            SemanticPredecessorGuardState.SATISFIED
        ).causalRelation(
            "relation",
            guard
        ).build();
    }

    private static ProofPlan controlsPlan(
        ProofRuntimeHarness harness,
        SemanticHold hold,
        SemanticPredecessorGuard guard
    ) {
        return ProofPlan.builder(
            "reentrant-control-completions",
            "Reentrant control completions",
            harness.subject,
            DEADLINE
        ).observation(
            "observation",
            harness.connectionId,
            ProofTestFixture.PROFILE
        ).control(
            "hold",
            hold,
            SemanticHoldState.FORWARDED
        ).control(
            "guard",
            guard,
            SemanticPredecessorGuardState.SATISFIED
        ).build();
    }

    private static boolean isViolation(ScenarioEvent event) {
        return event instanceof SemanticPredecessorGuardEvent guard
            && guard.kind() == SemanticPredecessorGuardEvent.Kind.TERMINAL
            && guard.state() == SemanticPredecessorGuardState.VIOLATED;
    }

    private static void assertDeadlineGap(ProofResult result) {
        assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
        assertThat(result.evaluation().state()).isIn(
            ProofEvaluationState.NOT_STARTED,
            ProofEvaluationState.RUNNING
        );
        assertThat(result.evaluation().resolution()).isEqualTo(ProofResolution.TIMED_OUT);
        assertThat(result.evaluation().reason())
            .isEqualTo(ProofResolutionReason.DEADLINE_EXPIRED);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Test boundary was not reached");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for test boundary", interrupted);
        }
    }

    private static <T> T get(Future<T> future) throws Exception {
        return future.get(5, TimeUnit.SECONDS);
    }

    private static CorrelationKey preWindowKey() {
        byte[] digest = new byte[16];
        java.util.Arrays.fill(digest, (byte) 91);
        return CorrelationKey.ofDigest(
            new CorrelationKeySchema("system-proof-test", "pre-window", 1),
            digest
        );
    }

}
