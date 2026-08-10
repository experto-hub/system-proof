package io.github.jacekkardys.systemproof.examples.sms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.TestSms;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1Environment;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1ProofPoint;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1ProofPoint.CommitHoldExperiment;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1ProofPoint.CounterexampleState;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1ProofPoint.EarlyHttpNegative;
import io.github.jacekkardys.systemproof.junit.annotation.SystemProof;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlDurabilityResult.RelationStatus;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlDurabilityResult.Setting;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofExecutionState;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofResult;

/** Direct AML T1 falsification and the independent early-HTTP negative control. */
@Tag("docker")
final class AmlT1ProofIT {

    @SystemProof(
        value = AmlT1Environment.class,
        title = "AML T1 held-commit counterexample",
        description = "Captures positive SMPP evidence while the exact RAW and Outbox commit is held"
    )
    void capturesPositiveSmppWhileExactCommitIsHeld(AmlT1Environment environment)
        throws Exception {
        TestSms message = proofMessage();
        CommitHoldExperiment experiment = AmlT1ProofPoint.prepareCommitHoldExperiment(
            environment,
            message
        );
        ProofExecution execution = environment.proofs().activate(experiment.plan());
        assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);

        ExecutorService stimulusExecutor = boundedStimulusExecutor();
        try {
            var stimulus = stimulusExecutor.submit(() ->
                execution.runStimulus(() -> environment.smsc().send(message))
            );
            CounterexampleState counterexample = experiment.awaitCounterexampleState();
            experiment.releaseAndAwaitDurability(counterexample);
            stimulus.get(AmlT1ProofPoint.TIMEOUT.toSeconds(), TimeUnit.SECONDS);

            ProofResult coverage = execution.evaluate();
            experiment.assertCoverage(execution, coverage);
        } finally {
            shutdown(stimulusExecutor);
        }
    }

    @SystemProof(
        value = AmlT1Environment.class,
        title = "Direct AML T1 proof",
        description = "Requires durable PostgreSQL commit before the correlated positive SMPP response"
    )
    void provesDirectCommitBeforePositiveSmppResponse(AmlT1Environment environment)
        throws Exception {
        TestSms message = proofMessage();
        AmlT1ProofPoint proof = AmlT1ProofPoint.prepare(environment, message);
        assertThat(proof.subject()).isNotNull();
        assertThat(proof.correlationKey()).isNotNull();
        assertThat(proof.commitHold().state()).isEqualTo(SemanticHoldState.DECLARED);
        assertThat(proof.directGuard().state())
            .isEqualTo(SemanticPredecessorGuardState.DECLARED);
        assertDurability(proof);

        ProofExecution execution = environment.proofs().activate(proof.plan());
        assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);
        ExecutorService stimulusExecutor = boundedStimulusExecutor();
        ProofResult result;
        try {
            stimulusExecutor.submit(() ->
                execution.runStimulus(() -> environment.smsc().send(message))
            );
            result = proof.awaitDirectViolation(execution);
            proof.assertDirectViolation(execution, result);
        } finally {
            shutdownAfterTerminal(stimulusExecutor);
        }

        // This is the canonical invariant assertion. It intentionally remains red for a real
        // counterexample; the exact VIOLATED result above must never be converted to success.
        result.require(ProofOutcome.PROVED);
    }

    @Test
    void rejectsTheRealEarlyAcknowledgingApplicationForCommitBeforeHttp()
        throws Exception {
        try (AmlT1Environment environment = AmlT1Environment.earlyAcknowledging()) {
            environment.start();
            TestSms message = proofMessage();
            EarlyHttpNegative negative = AmlT1ProofPoint.prepareEarlyHttpNegative(
                environment,
                message
            );
            ProofExecution execution = environment.proofs().activate(negative.plan());
            assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);

            ExecutorService stimulusExecutor = boundedStimulusExecutor();
            try {
                var stimulus = stimulusExecutor.submit(() ->
                    execution.runStimulus(() -> environment.smsc().send(message))
                );
                ProofResult violated = negative.awaitViolation(execution);
                negative.assertCommitBeforeHttpViolation(execution, violated);
                stimulus.get(AmlT1ProofPoint.TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                negative.awaitEventuallyCommitted();
                assertThat(execution.result()).isSameAs(violated);
            } finally {
                shutdown(stimulusExecutor);
            }
        }
    }

    private static TestSms proofMessage() {
        return TestSms.forProof(UUID.randomUUID().toString());
    }

    private static void assertDurability(AmlT1ProofPoint proof) {
        assertThat(proof.durability().synchronousCommit()).isEqualTo(Setting.ON);
        assertThat(proof.durability().fsync()).isEqualTo(Setting.ON);
        assertThat(proof.durability().relations().values())
            .isNotEmpty()
            .allMatch(status -> status == RelationStatus.PERMANENT_TABLE);
    }

    private static ExecutorService boundedStimulusExecutor() {
        return new ThreadPoolExecutor(
            1,
            1,
            0,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }

    private static void shutdown(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    private static void shutdownAfterTerminal(ExecutorService executor)
        throws InterruptedException {
        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }
}
