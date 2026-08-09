package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.proof.ProofEvidenceKind;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofFailureStage;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResult;

class ProofExecutionRaceTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);

    @RepeatedTest(20)
    void shouldKeepViolationWhenItLinearizesBeforeFrameworkFailure() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = guardExecution(harness);

            runInOrder(
                () -> harness.publish("successor"),
                harness::frameworkFailure
            );

            ProofResult result = execution.result();
            assertStable(result, execution, ProofOutcome.VIOLATED);
            assertThat(result.secondaryDiagnostics()).isEmpty();
        }
    }

    @RepeatedTest(20)
    void shouldKeepErrorWhenItLinearizesBeforeViolationFact() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = guardExecution(harness);

            runInOrder(
                harness::frameworkFailure,
                () -> harness.publish("successor")
            );

            ProofResult result = execution.result();
            assertStable(result, execution, ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).hasValueSatisfying(
                diagnostic -> assertThat(diagnostic.stage()).isEqualTo(
                    ProofFailureStage.JOURNAL
                )
            );
        }
    }

    @RepeatedTest(20)
    void shouldKeepViolationWhenItLinearizesBeforeDeadline() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = guardExecution(harness);

            runInOrder(
                () -> harness.publish("successor"),
                harness.deadlines::fireRacingCallback
            );

            assertStable(execution.result(), execution, ProofOutcome.VIOLATED);
        }
    }

    @RepeatedTest(20)
    void shouldKeepTimeoutWhenItLinearizesBeforeViolationFact() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = guardExecution(harness);

            runInOrder(
                harness.deadlines::fireRacingCallback,
                () -> harness.publish("successor")
            );

            assertStable(execution.result(), execution, ProofOutcome.INCONCLUSIVE);
        }
    }

    @RepeatedTest(20)
    void shouldKeepEvaluationWhenItLinearizesBeforeDeadline() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = prerequisiteExecution(harness, "evaluation-first");
            execution.runStimulus(() -> {});

            runInOrder(execution::evaluate, harness.deadlines::fireRacingCallback);

            assertStable(execution.result(), execution, ProofOutcome.PROVED);
        }
    }

    @RepeatedTest(20)
    void shouldKeepTimeoutWhenItLinearizesBeforeEvaluation() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = prerequisiteExecution(harness, "timeout-first");

            runInOrder(harness.deadlines::fireRacingCallback, execution::evaluate);

            assertStable(execution.result(), execution, ProofOutcome.INCONCLUSIVE);
        }
    }

    @RepeatedTest(20)
    void shouldRetainCleanupAsSecondaryAfterTerminalViolation() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = guardExecution(harness);

            runInOrder(() -> harness.publish("successor"), harness::cleanupFailure);

            ProofResult result = execution.result();
            assertStable(result, execution, ProofOutcome.VIOLATED);
            assertThat(result.secondaryDiagnostics()).isEmpty();
        }
    }

    @RepeatedTest(20)
    void shouldKeepCleanupErrorWhenItLinearizesBeforeViolationFact() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = guardExecution(harness);

            runInOrder(harness::cleanupFailure, () -> harness.publish("successor"));

            ProofResult result = execution.result();
            assertStable(result, execution, ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).hasValueSatisfying(
                diagnostic -> assertThat(diagnostic.stage()).isEqualTo(
                    ProofFailureStage.CLEANUP
                )
            );
        }
    }

    @Test
    void shouldConvertProtocolNeutralEvaluatorFailureToError() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start(resolutions -> {
            throw new EvaluatorFailure();
        })) {
            ProofExecution execution = prerequisiteExecution(harness, "evaluator-failure");
            execution.runStimulus(() -> {});

            ProofResult result = execution.evaluate();

            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).hasValueSatisfying(
                diagnostic -> assertThat(diagnostic.stage()).isEqualTo(
                    ProofFailureStage.EVALUATION
                )
            );
        }
    }

    @Test
    void shouldConvertGatewayAdapterAndJournalFailuresToTypedErrors() {
        try (ProofRuntimeHarness gateway = ProofRuntimeHarness.start()) {
            ProofExecution execution = observedPrerequisiteExecution(
                gateway,
                "gateway-failure"
            );
            gateway.gatewayFailure();
            assertFailureStage(execution.result(), ProofFailureStage.GATEWAY);
        }
        try (ProofRuntimeHarness adapter = ProofRuntimeHarness.start()) {
            ProofExecution execution = observedPrerequisiteExecution(
                adapter,
                "adapter-failure"
            );
            execution.runStimulus(() -> {});
            adapter.adapterFailure();
            assertFailureStage(execution.evaluate(), ProofFailureStage.OBSERVATION);
        }
        try (ProofRuntimeHarness journal = ProofRuntimeHarness.start()) {
            ProofExecution execution = prerequisiteExecution(journal, "journal-failure");
            journal.frameworkFailure();
            assertFailureStage(execution.result(), ProofFailureStage.JOURNAL);
        }
    }

    @Test
    void shouldRollBackEveryPreparedControlWhenActivationCannotArmAllOfThem() {
        try (ProofRuntimeHarness harness =
                 ProofRuntimeHarness.startWithFailingControlScheduler()) {
            SemanticHold hold = harness.declareHold("held");
            SemanticPredecessorGuard guard = harness.declareGuard();
            java.util.concurrent.atomic.AtomicInteger stimulusCalls =
                new java.util.concurrent.atomic.AtomicInteger();
            ProofPlan plan = ProofPlan.builder(
                "activation-rollback",
                "Activation rollback",
                harness.subject,
                DEADLINE
            ).observation(
                "observation",
                harness.connectionId,
                ProofTestFixture.PROFILE
            ).control("hold-control", hold, SemanticHoldState.FORWARDED)
                .control(
                    "guard-control",
                    guard,
                    SemanticPredecessorGuardState.SATISFIED
                ).build();

            ProofExecution execution = harness.activate(plan);
            execution.runStimulus(stimulusCalls::incrementAndGet);

            assertFailureStage(execution.result(), ProofFailureStage.ACTIVATION);
            assertThat(hold.state()).isEqualTo(SemanticHoldState.CANCELLED);
            assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.FAILED);
            assertThat(stimulusCalls).hasValue(0);
        }
    }

    private static ProofExecution guardExecution(ProofRuntimeHarness harness) {
        SemanticPredecessorGuard guard = harness.declareGuard();
        ProofPlan plan = ProofPlan.builder(
            "race-guard-proof",
            "Race guard proof",
            harness.subject,
            DEADLINE
        ).observation(
            "observation",
            harness.connectionId,
            ProofTestFixture.PROFILE
        ).correlation(
            "correlation",
            harness.connectionId,
            harness.key,
            ProofTestFixture.NATIVE_SCHEMA
        ).control(
            "guard-control",
            guard,
            SemanticPredecessorGuardState.SATISFIED
        ).evidence(
            "predecessor-evidence",
            guard,
            ProofEvidenceKind.PREDECESSOR_INTERACTION
        ).evidence(
            "successor-evidence",
            guard,
            ProofEvidenceKind.SUCCESSOR_INTERACTION
        ).causalRelation("causal-relation", guard).build();
        return harness.activate(plan);
    }

    private static ProofExecution prerequisiteExecution(
        ProofRuntimeHarness harness,
        String id
    ) {
        return harness.activate(ProofPlan.builder(
            id,
            "Race prerequisite proof",
            harness.subject,
            DEADLINE
        ).prerequisite("prerequisite", harness.prerequisite()).build());
    }

    private static ProofExecution observedPrerequisiteExecution(
        ProofRuntimeHarness harness,
        String id
    ) {
        return harness.activate(ProofPlan.builder(
            id,
            "Observed prerequisite proof",
            harness.subject,
            DEADLINE
        ).prerequisite("prerequisite", harness.prerequisite())
            .observation(
                "observation",
                harness.connectionId,
                ProofTestFixture.PROFILE
            ).build());
    }

    private static void assertStable(
        ProofResult result,
        ProofExecution execution,
        ProofOutcome expected
    ) {
        String explanation = result.report().content();
        assertThat(result.outcome()).isEqualTo(expected);
        assertThat(execution.evaluate()).isSameAs(result);
        assertThat(execution.result()).isSameAs(result);
        assertThat(execution.result().report().content()).isEqualTo(explanation);
    }

    private static void assertFailureStage(ProofResult result, ProofFailureStage stage) {
        assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
        assertThat(result.primaryFailure()).hasValueSatisfying(
            diagnostic -> assertThat(diagnostic.stage()).isEqualTo(stage)
        );
    }

    private static void runInOrder(ThrowingAction first, ThrowingAction second)
        throws Exception {
        CyclicBarrier start = new CyclicBarrier(3);
        CountDownLatch firstCompleted = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> firstFuture = executor.submit(() -> {
                await(start);
                try {
                    first.run();
                } finally {
                    firstCompleted.countDown();
                }
                return null;
            });
            Future<?> secondFuture = executor.submit(() -> {
                await(start);
                if (!firstCompleted.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("First racing action did not complete");
                }
                second.run();
                return null;
            });
            start.await(5, TimeUnit.SECONDS);
            firstFuture.get(5, TimeUnit.SECONDS);
            secondFuture.get(5, TimeUnit.SECONDS);
        }
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await(5, TimeUnit.SECONDS);
        } catch (Exception failure) {
            throw new AssertionError("Proof race barrier failed", failure);
        }
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }

    private static final class EvaluatorFailure extends RuntimeException {}
}
