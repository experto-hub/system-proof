package io.github.jacekkardys.systemproof.examples.sms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.relay.ReferenceRelayOperations.Delivery;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.SmsPersistence;
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

/** Direct AML T1 classification for the reference relay and pinned stock Jasmin. */
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

    @Test
    void provesDirectCommitBeforePositiveSmppResponseWithReferenceRelay()
        throws Exception {
        try (AmlT1Environment environment = AmlT1Environment.referenceRelay()) {
            environment.start();
            TestSms message = proofMessage();
            AmlT1ProofPoint proof = AmlT1ProofPoint.prepare(environment, message);
            assertDirectProofDeclared(proof);

            ProofExecution execution = environment.proofs().activate(proof.plan());
            assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);
            ExecutorService stimulusExecutor = boundedStimulusExecutor();
            AtomicReference<Delivery> delivered = new AtomicReference<>();
            try {
                var stimulus = stimulusExecutor.submit(() -> execution.runStimulus(() -> {
                    environment.smsc().send(message);
                    delivered.set(environment.referenceRelayOperations().awaitDelivery());
                }));
                stimulus.get(AmlT1ProofPoint.TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                ProofResult result = proof.awaitDirectProof(execution);
                SmsPersistence persistence = proof.assertDirectProof(execution, result);

                assertThat(result.outcome()).isEqualTo(ProofOutcome.PROVED);
                assertThat(delivered.get()).satisfies(value -> {
                    assertThat(value.sequence()).isPositive();
                    assertThat(value.sourceAddress()).isEqualTo(message.sourceAddress());
                    assertThat(value.destinationAddress()).isEqualTo(message.destinationAddress());
                    assertThat(value.dataCoding()).isEqualTo(8);
                    assertThat(value.content()).isEqualTo(message.content());
                    assertThat(value.callbackId()).isNotBlank().isNotEqualTo(message.id());
                    assertThat(persistence.externalMessageId()).isEqualTo(value.callbackId());
                });
            } finally {
                shutdown(stimulusExecutor);
            }
        }
    }

    @Test
    void classifiesStockJasminAsDirectT1Violation() throws Exception {
        ProofExecution execution;
        ProofResult result;
        try (AmlT1Environment environment = AmlT1Environment.stockJasmin()) {
            environment.start();
            TestSms message = proofMessage();
            AmlT1ProofPoint proof = AmlT1ProofPoint.prepare(environment, message);
            assertDirectProofDeclared(proof);

            execution = environment.proofs().activate(proof.plan());
            assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);
            ExecutorService stimulusExecutor = boundedStimulusExecutor();
            try {
                var stimulus = stimulusExecutor.submit(() ->
                    execution.runStimulus(() -> environment.smsc().send(message))
                );
                stimulus.get(AmlT1ProofPoint.TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                result = proof.awaitDirectViolation(execution);
                proof.assertDirectViolation(execution, result);
                assertThat(result.outcome()).isEqualTo(ProofOutcome.VIOLATED);
            } finally {
                shutdown(stimulusExecutor);
            }
        }

        assertThat(execution.result()).isSameAs(result);
        assertThat(execution.evaluate()).isSameAs(result);
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

    private static void assertDirectProofDeclared(AmlT1ProofPoint proof) {
        assertThat(proof.subject()).isNotNull();
        assertThat(proof.correlationKey()).isNotNull();
        assertThat(proof.plan().id().value())
            .isEqualTo("aml-t1-direct-commit-before-positive-smpp");
        assertThat(proof.plan().requirements())
            .extracting(value -> value.id().value())
            .containsExactly(
                "postgresql-durability",
                "postgresql-observation",
                "smpp-observation",
                "postgresql-transaction-correlation",
                "smpp-exchange-correlation",
                "t1-direct-commit-before-smpp-guard",
                "t1-direct-commit-evidence",
                "t1-direct-smpp-evidence",
                "t1-direct-commit-before-smpp-relation"
            );
        assertThat(proof.directGuard().state())
            .isEqualTo(SemanticPredecessorGuardState.DECLARED);
        assertDurability(proof);
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
