package io.github.jacekkardys.systemproof.examples.sms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.TestSms;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1Environment;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1ProofPoint;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1ProofPoint.ArchitecturalHypothesis;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1ProofPoint.HttpHoldCharacterization;
import io.github.jacekkardys.systemproof.junit.annotation.SystemProof;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofExecutionState;
import io.github.jacekkardys.systemproof.proof.ProofResult;

/** Real pinned-Jasmin characterization; it is deliberately separate from AML T1. */
@Tag("docker")
final class JasminHttpSmppCharacterizationIT {

    @SystemProof(
        value = AmlT1Environment.class,
        title = "Jasmin HTTP-held SMPP characterization",
        description = "Observes positive SMPP while the exact positive HTTP response is held"
    )
    void observesPositiveSmppWhileHttpResponseIsHeld(AmlT1Environment environment)
        throws Exception {
        TestSms message = proofMessage();
        HttpHoldCharacterization characterization =
            AmlT1ProofPoint.prepareHttpHoldCharacterization(environment, message);
        ProofExecution execution = environment.proofs().activate(characterization.plan());
        assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);

        ExecutorService stimulusExecutor = boundedStimulusExecutor();
        try {
            var stimulus = stimulusExecutor.submit(() ->
                execution.runStimulus(() -> environment.smsc().send(message))
            );
            characterization.awaitHeldHttpWithPositiveSmpp();
            characterization.release();
            environment.database().await().rawAndOutboxVisible(message);
            stimulus.get(AmlT1ProofPoint.TIMEOUT.toSeconds(), TimeUnit.SECONDS);

            ProofResult coverage = execution.evaluate();
            characterization.assertCoverage(execution, coverage);
        } finally {
            shutdown(stimulusExecutor, true);
        }
    }

    @SystemProof(
        value = AmlT1Environment.class,
        title = "Jasmin HTTP before SMPP hypothesis",
        description = "Evaluates HTTP response forwarding as a predecessor of positive SMPP"
    )
    void falsifiesForwardedHttpBeforePositiveSmpp(AmlT1Environment environment)
        throws Exception {
        TestSms message = proofMessage();
        ArchitecturalHypothesis hypothesis = AmlT1ProofPoint.prepareHttpToSmppHypothesis(
            environment,
            message
        );
        ProofExecution execution = environment.proofs().activate(hypothesis.plan());
        assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);

        ExecutorService stimulusExecutor = boundedStimulusExecutor();
        try {
            stimulusExecutor.submit(() ->
                execution.runStimulus(() -> environment.smsc().send(message))
            );
            ProofResult result = hypothesis.awaitViolation(execution);
            hypothesis.assertFalsified(execution, result);
        } finally {
            shutdown(stimulusExecutor, false);
        }
    }

    private static TestSms proofMessage() {
        return TestSms.forProof(UUID.randomUUID().toString());
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

    private static void shutdown(ExecutorService executor, boolean assertTerminated)
        throws InterruptedException {
        executor.shutdownNow();
        boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
        if (assertTerminated) {
            assertThat(terminated).isTrue();
        }
    }
}
