package io.github.jacekkardys.systemproof.examples.sms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.TestSms;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1Environment;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1ProofPoint;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1ProofPoint.NativeAttribution;
import io.github.jacekkardys.systemproof.junit.annotation.SystemProof;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlDurabilityResult.RelationStatus;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlDurabilityResult.Setting;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofExecutionState;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;

/** Canonical positive and negative real-system proof of the AML T1 invariant. */
@Tag("docker")
final class AmlT1ProofIT {

    @SystemProof(
        value = AmlT1Environment.class,
        title = "Deterministic AML T1 proof",
        description = "Proves durable RAW and Outbox commit before positive HTTP and SMPP acknowledgement"
    )
    void provesCommitBeforePositiveAcknowledgement(AmlT1Environment environment)
        throws Exception {
        TestSms message = proofMessage();
        AmlT1ProofPoint proof = AmlT1ProofPoint.prepare(environment, message);
        ProofSubjectRef subject = proof.subject();
        CorrelationKey correlationKey = proof.correlationKey();
        SemanticHold commitHold = proof.commitHold();
        SemanticHold httpResponseHold = proof.httpResponseHold();
        SemanticPredecessorGuard commitToHttp = proof.commitToHttpGuard();
        SemanticPredecessorGuard httpToSmpp = proof.httpToSmppGuard();
        ProofPlan plan = proof.plan();

        assertThat(subject).isNotNull();
        assertThat(correlationKey).isNotNull();
        assertThat(commitHold.state()).isEqualTo(SemanticHoldState.DECLARED);
        assertThat(httpResponseHold.state()).isEqualTo(SemanticHoldState.DECLARED);
        assertThat(commitToHttp.state()).isEqualTo(SemanticPredecessorGuardState.DECLARED);
        assertThat(httpToSmpp.state()).isEqualTo(SemanticPredecessorGuardState.DECLARED);
        assertDurability(proof);

        ProofExecution execution = environment.proofs().activate(plan);
        assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);
        ExecutorService stimulusExecutor = boundedStimulusExecutor();
        try {
            var stimulus = stimulusExecutor.submit(() ->
                execution.runStimulus(() -> environment.smsc().send(message))
            );
            NativeAttribution attribution = proof.awaitCommitHeldAndAssertInvisible();
            proof.releaseCommitAndAwaitDurability(attribution);
            proof.awaitHttpHeldAndAssertNoEarlySmpp(attribution);
            proof.releaseHttpAndAwaitRelations();
            stimulus.get(AmlT1ProofPoint.TIMEOUT.toSeconds(), TimeUnit.SECONDS);

            ProofResult result = execution.evaluate();
            proof.assertProved(execution, result);
        } finally {
            shutdown(stimulusExecutor);
        }
    }

    @Test
    void rejectsTheRealEarlyAcknowledgingApplicationAsViolated() throws Exception {
        try (AmlT1Environment environment = AmlT1Environment.earlyAcknowledging()) {
            environment.start();
            TestSms message = proofMessage();
            AmlT1ProofPoint proof = AmlT1ProofPoint.prepare(environment, message);
            ProofPlan plan = proof.plan();
            ProofExecution execution = environment.proofs().activate(plan);
            assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);

            ExecutorService stimulusExecutor = boundedStimulusExecutor();
            try {
                var stimulus = stimulusExecutor.submit(() ->
                    execution.runStimulus(() -> environment.smsc().send(message))
                );
                NativeAttribution attribution = proof.awaitCounterexampleCommitHeld();
                proof.awaitEarlyHttpViolation();

                ProofResult violated = execution.result();
                proof.assertViolated(execution, violated);

                proof.releaseCommitAndAwaitDurability(attribution);
                stimulus.get(AmlT1ProofPoint.TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                assertThat(execution.result()).isSameAs(violated);
                proof.assertViolated(execution, violated);
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
}
