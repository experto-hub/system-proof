package io.github.jacekkardys.systemproof.examples.sms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticInteractionSelector;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.environment.Environment;
import io.github.jacekkardys.systemproof.environment.state.ConnectionState;
import io.github.jacekkardys.systemproof.environment.state.RoutingMode;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.postgres.SmsDatabaseOperations;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.SmsMessageFingerprint;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.SmsPersistence;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.TestSms;
import io.github.jacekkardys.systemproof.examples.sms.environment.proof.AmlT1Environment;
import io.github.jacekkardys.systemproof.http.HttpEvidence;
import io.github.jacekkardys.systemproof.http.HttpEvidence.RequestCompleted;
import io.github.jacekkardys.systemproof.http.HttpEvidence.ResponseCompleted;
import io.github.jacekkardys.systemproof.http.HttpExchangeRef;
import io.github.jacekkardys.systemproof.http.HttpProtocolAdapter;
import io.github.jacekkardys.systemproof.journal.InteractionObservationEvent;
import io.github.jacekkardys.systemproof.observation.EffectiveObservationStatus;
import io.github.jacekkardys.systemproof.observation.EvidenceCodec;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.ObservationRequirement;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlDurabilityRequirements;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlDurabilityRequirements.Table;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlEvidence;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlEvidence.CommitAttempt;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlEvidence.CommitSucceeded;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlEvidence.Rollback;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlProtocolAdapter;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlWriteCorrelation;
import io.github.jacekkardys.systemproof.postgresql.TransactionRef;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.CorrelationResult;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.smpp.SmppEvidence;
import io.github.jacekkardys.systemproof.smpp.SmppEvidence.DeliverSmCompleted;
import io.github.jacekkardys.systemproof.smpp.SmppEvidence.DeliverSmResponseCompleted;
import io.github.jacekkardys.systemproof.smpp.SmppExchangeRef;
import io.github.jacekkardys.systemproof.smpp.SmppProtocolAdapter;
import io.github.jacekkardys.systemproof.testcontainers.gateway.ProtocolLimits;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/** Attributes one reference AML proof subject across three observed protocol-native flows. */
@Tag("docker")
final class PostgresqlCorrelatedCommitIT {
    private static final int REPETITIONS = 5;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final PostgresqlDurabilityRequirements DURABILITY_REQUIREMENTS =
        new PostgresqlDurabilityRequirements(Set.of(
            new Table("public", "raw_sms_event"),
            new Table("public", "outbox_event")
        ));

    @Test
    void shouldAttributeTheRealAmlTransactionWithSubjectSafeIsolationAndReconnect()
        throws Exception {
        PostgresqlProtocolAdapter postgresqlAdapter = new PostgresqlProtocolAdapter(
            SmsMessageFingerprint.rawWriteCorrelation()
        );
        ExecutorService submissions = Executors.newFixedThreadPool(4);
        ProofMessage reconnectSource;
        TransactionRef transactionBeforeReconnect;
        try {
            AmlT1Environment environment = AmlT1Environment.observed(
                postgresqlAdapter
            );
            try {
                environment.start();
                assertRequiredObservedRoutes(environment);
                assertThat(environment.database()
                    .durabilityPreflight(DURABILITY_REQUIREMENTS)
                    .requireSatisfied()).isNotNull();
                assertThat(commitSuccesses(environment))
                    .as("Flyway must complete an unrelated observed transaction")
                    .isNotEmpty();

                List<ProofMessage> secrets = new ArrayList<>();
                TransactionRef previous = null;
                for (int repetition = 0; repetition < REPETITIONS; repetition++) {
                    TransactionPair pair = verifyTargetAfterUnrelatedCommit(
                        environment,
                        submissions
                    );
                    secrets.add(pair.unrelatedProof());
                    secrets.add(pair.targetProof());
                    if (previous != null) {
                        assertNextTransaction(previous, pair.unrelated().transaction());
                    }
                    assertNextTransaction(
                        pair.unrelated().transaction(),
                        pair.target().transaction()
                    );
                    previous = pair.target().transaction();
                }

                ConcurrentAttributions concurrent = verifyConcurrentSubjects(
                    environment,
                    submissions
                );
                secrets.add(concurrent.firstProof());
                secrets.add(concurrent.secondProof());

                ProofMessage rollbackProof = verifyRollbackAndAmbiguousRetry(
                    environment,
                    submissions
                );
                secrets.add(rollbackProof);
                assertSecretSafe(environment, secrets, List.of(), null);

                reconnectSource = concurrent.secondProof();
                transactionBeforeReconnect = concurrent.second().transaction();
            } finally {
                environment.close();
            }

            verifyReconnectStartsANewPhysicalAttribution(
                postgresqlAdapter,
                submissions,
                reconnectSource,
                transactionBeforeReconnect
            );
        } finally {
            submissions.shutdownNow();
            assertThat(submissions.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void shouldFailRequiredObservationWithoutLeakingAPostgresqlPolicyException() {
        String policySecret = "postgresql-policy-secret-" + UUID.randomUUID();
        PostgresqlWriteCorrelation rawWrite =
            SmsMessageFingerprint.rawWriteCorrelation();
        PostgresqlWriteCorrelation failingPolicy = interaction -> {
            if (rawWrite.correlate(interaction).isPresent()) {
                throw new IllegalStateException("policy failed with " + policySecret);
            }
            return Optional.empty();
        };
        AmlT1Environment environment = AmlT1Environment.observed(
            new PostgresqlProtocolAdapter(failingPolicy)
        );
        SemanticHold hold = null;
        try {
            environment.start();
            assertRequiredObservedRoutes(environment);
            ProofMessage proof = ProofMessage.create(environment);
            hold = commitHold(environment, proof);

            Throwable submissionFailure = catchThrowable(() ->
                environment.smsc().send(proof.message())
            );
            Awaitility.await("PostgreSQL observation failed closed")
                .atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(
                    environment.runtimeConnection(environment.databaseConnectionId())
                        .effectiveObservationStatus()
                ).isEqualTo(EffectiveObservationStatus.FAILED));

            assertThat(environment.proofSubjects().correlation(
                proof.subject(),
                proof.key(),
                TransactionRef.codec()
            )).isInstanceOf(CorrelationResult.Missing.class);
            assertThat(hold.state()).isEqualTo(SemanticHoldState.ARMED);
            assertThat(environment.database().snapshot(proof.message()))
                .satisfies(persistence -> {
                    assertThat(persistence.rawCount()).isZero();
                    assertThat(persistence.outboxCount()).isZero();
                });
            assertSecretSafe(
                environment,
                List.of(proof),
                List.of(policySecret),
                submissionFailure
            );
        } finally {
            if (hold != null && hold.state() == SemanticHoldState.ARMED) {
                assertThat(hold.cancel()).isTrue();
            }
            environment.close();
        }
    }

    private static TransactionPair verifyTargetAfterUnrelatedCommit(
        AmlT1Environment environment,
        ExecutorService submissions
    ) throws Exception {
        ProofMessage target = ProofMessage.create(environment);
        SemanticHold targetHold = commitHold(environment, target);
        ProofMessage unrelated = ProofMessage.create(environment);
        assertThat(unrelated.key()).isNotEqualTo(target.key());

        environment.smsc().send(unrelated.message());
        NativeAttribution unrelatedAttribution = awaitUniqueAttribution(
            environment,
            unrelated
        );
        awaitCommitSucceeded(environment, unrelatedAttribution.transaction());
        assertPersistedAtomically(environment, unrelated.message());
        assertThat(targetHold.state()).isEqualTo(SemanticHoldState.ARMED);
        assertThat(environment.proofSubjects().correlation(
            target.subject(),
            target.key(),
            TransactionRef.codec()
        )).isInstanceOf(CorrelationResult.Missing.class);

        Future<?> submission = submissions.submit(() ->
            environment.smsc().send(target.message())
        );
        targetHold.reached().toCompletableFuture().get(
            TIMEOUT.toSeconds(),
            TimeUnit.SECONDS
        );
        NativeAttribution targetAttribution = uniqueAttribution(environment, target);
        assertAttributionEvidence(environment, targetAttribution);
        assertThat(commitAttempts(environment))
            .filteredOn(attempt -> attempt.transaction().equals(
                targetAttribution.transaction()
            ))
            .hasSize(1);
        assertThat(commitSuccesses(environment))
            .noneMatch(success -> success.transaction().equals(
                targetAttribution.transaction()
            ));
        assertThat(environment.database().snapshot(target.message()))
            .satisfies(persistence -> {
                assertThat(persistence.rawCount()).isZero();
                assertThat(persistence.outboxCount()).isZero();
            });

        targetHold.release().toCompletableFuture().get(
            TIMEOUT.toSeconds(),
            TimeUnit.SECONDS
        );
        submission.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        awaitCommitSucceeded(environment, targetAttribution.transaction());
        assertPersistedAtomically(environment, target.message());

        assertThat(targetAttribution.smpp()).isNotEqualTo(unrelatedAttribution.smpp());
        assertThat(targetAttribution.http()).isNotEqualTo(unrelatedAttribution.http());
        assertThat(targetAttribution.transaction())
            .isNotEqualTo(unrelatedAttribution.transaction());
        return new TransactionPair(
            unrelated,
            unrelatedAttribution,
            target,
            targetAttribution
        );
    }

    private static ConcurrentAttributions verifyConcurrentSubjects(
        AmlT1Environment environment,
        ExecutorService submissions
    ) throws Exception {
        ProofMessage first = ProofMessage.create(environment);
        ProofMessage second = ProofMessage.create(environment);
        assertThat(first.key()).isNotEqualTo(second.key());
        SemanticHold firstHold = commitHold(environment, first);
        SemanticHold secondHold = commitHold(environment, second);
        CyclicBarrier ready = new CyclicBarrier(3);

        Future<?> secondSubmission = submissions.submit(() -> {
            awaitBarrier(ready);
            environment.smsc().send(second.message());
        });
        Future<?> firstSubmission = submissions.submit(() -> {
            awaitBarrier(ready);
            environment.smsc().send(first.message());
        });
        awaitBarrier(ready);

        CompletableFuture.allOf(
            firstHold.reached().toCompletableFuture(),
            secondHold.reached().toCompletableFuture()
        ).get(
            TIMEOUT.toSeconds(),
            TimeUnit.SECONDS
        );
        NativeAttribution firstAttribution = uniqueAttribution(environment, first);
        NativeAttribution secondAttribution = uniqueAttribution(environment, second);
        assertThat(firstHold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertThat(secondHold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertAttributionEvidence(environment, firstAttribution);
        assertAttributionEvidence(environment, secondAttribution);
        assertThat(firstAttribution.smpp()).isNotEqualTo(secondAttribution.smpp());
        assertThat(firstAttribution.http()).isNotEqualTo(secondAttribution.http());
        assertThat(firstAttribution.transaction())
            .isNotEqualTo(secondAttribution.transaction());
        assertThat(firstAttribution.transaction().sessionOrdinal())
            .isNotEqualTo(secondAttribution.transaction().sessionOrdinal());
        assertThat(commitSuccesses(environment))
            .noneMatch(success -> success.transaction().equals(
                firstAttribution.transaction()
            ) || success.transaction().equals(secondAttribution.transaction()));
        assertNotPersisted(environment, first.message());
        assertNotPersisted(environment, second.message());

        CompletableFuture.allOf(
            firstHold.release().toCompletableFuture(),
            secondHold.release().toCompletableFuture()
        ).get(
            TIMEOUT.toSeconds(),
            TimeUnit.SECONDS
        );
        firstSubmission.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        secondSubmission.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        awaitCommitSucceeded(environment, firstAttribution.transaction());
        awaitCommitSucceeded(environment, secondAttribution.transaction());
        assertPersistedAtomically(environment, first.message());
        assertPersistedAtomically(environment, second.message());
        return new ConcurrentAttributions(
            first,
            firstAttribution,
            second,
            secondAttribution
        );
    }

    private static ProofMessage verifyRollbackAndAmbiguousRetry(
        AmlT1Environment environment,
        ExecutorService submissions
    ) throws Exception {
        ProofMessage proof = ProofMessage.create(environment);
        SemanticHold commitHold = commitHold(environment, proof);
        SemanticHold rollbackHold = rollbackHold(environment, proof);
        NativeAttribution rollbackAttribution;

        try (SmsDatabaseOperations.OutboxRejection ignored =
                 environment.database().rejectOutboxInserts()) {
            Future<?> submission = submissions.submit(() ->
                environment.smsc().send(proof.message())
            );
            rollbackHold.reached().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            );
            rollbackAttribution = uniqueAttribution(environment, proof);
            assertThat(commitHold.state()).isEqualTo(SemanticHoldState.ARMED);
            assertThat(commitSuccesses(environment))
                .noneMatch(success -> success.transaction().equals(
                    rollbackAttribution.transaction()
                ));
            assertThat(environment.database().snapshot(proof.message()))
                .satisfies(persistence -> {
                    assertThat(persistence.rawCount()).isZero();
                    assertThat(persistence.outboxCount()).isZero();
                });

            rollbackHold.release().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            );
            submission.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        }

        assertThat(rollbacks(environment))
            .filteredOn(rollback -> rollback.transaction().equals(
                rollbackAttribution.transaction()
            ))
            .hasSize(1);
        int successesBeforeRetry = commitSuccesses(environment).size();
        environment.smsc().send(proof.message());
        assertPersistedAtomically(environment, proof.message());
        Awaitility.await("ambiguous retry attribution")
            .atMost(TIMEOUT)
            .untilAsserted(() -> {
                assertThat(environment.proofSubjects().correlation(
                    proof.subject(),
                    proof.key(),
                    SmppExchangeRef.codec()
                )).isInstanceOf(CorrelationResult.Ambiguous.class);
                assertThat(environment.proofSubjects().correlation(
                    proof.subject(),
                    proof.key(),
                    HttpExchangeRef.codec()
                )).isInstanceOf(CorrelationResult.Ambiguous.class);
                assertThat(environment.proofSubjects().correlation(
                    proof.subject(),
                    proof.key(),
                    TransactionRef.codec()
                )).isInstanceOf(CorrelationResult.Ambiguous.class);
            });
        Awaitility.await("retry commit confirmation")
            .atMost(TIMEOUT)
            .untilAsserted(() -> assertThat(commitSuccesses(environment))
                .hasSize(successesBeforeRetry + 1));
        assertThat(commitHold.state()).isEqualTo(SemanticHoldState.ARMED);
        assertThat(commitHold.cancel()).isTrue();
        assertThat(commitSuccesses(environment))
            .noneMatch(success -> success.transaction().equals(
                rollbackAttribution.transaction()
            ));
        return proof;
    }

    private static void verifyReconnectStartsANewPhysicalAttribution(
        PostgresqlProtocolAdapter postgresqlAdapter,
        ExecutorService submissions,
        ProofMessage previousProof,
        TransactionRef previousTransaction
    ) throws Exception {
        AmlT1Environment environment = AmlT1Environment.observed(postgresqlAdapter);
        try {
            environment.start();
            assertRequiredObservedRoutes(environment);
            ProofMessage reconnected = ProofMessage.rearm(
                environment,
                previousProof.discriminator(),
                previousProof.message()
            );
            SemanticHold hold = commitHold(environment, reconnected);
            Future<?> submission = submissions.submit(() ->
                environment.smsc().send(reconnected.message())
            );

            hold.reached().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            );
            NativeAttribution attribution = uniqueAttribution(environment, reconnected);
            assertAttributionEvidence(environment, attribution);
            assertThat(attribution.transaction()).isNotEqualTo(previousTransaction);
            assertThat(attribution.transaction().sessionOrdinal())
                .isGreaterThan(previousTransaction.sessionOrdinal());
            assertThat(postgresqlEvidence(environment))
                .noneMatch(evidence -> evidence instanceof CommitAttempt attempt
                    && attempt.transaction().equals(previousTransaction));

            hold.release().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            );
            submission.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            awaitCommitSucceeded(environment, attribution.transaction());
            assertPersistedAtomically(environment, reconnected.message());
            assertSecretSafe(environment, List.of(reconnected), List.of(), null);
        } finally {
            environment.close();
        }
    }

    private static SemanticHold commitHold(
        AmlT1Environment environment,
        ProofMessage proof
    ) {
        return environment.controls().arm(
            SemanticInteractionSelector.matching(
                environment.databaseConnectionId(),
                FlowDirection.CONSUMER_TO_PROVIDER,
                environment.postgresqlAdapter().evidenceCodec(),
                CommitAttempt.class::isInstance
            ).forSubject(proof.subject()).through(
                proof.key(),
                TransactionRef.codec(),
                evidence -> ((CommitAttempt) evidence).transaction()
            ),
            TIMEOUT
        );
    }

    private static SemanticHold rollbackHold(
        AmlT1Environment environment,
        ProofMessage proof
    ) {
        return environment.controls().arm(
            SemanticInteractionSelector.matching(
                environment.databaseConnectionId(),
                FlowDirection.CONSUMER_TO_PROVIDER,
                environment.postgresqlAdapter().evidenceCodec(),
                Rollback.class::isInstance
            ).forSubject(proof.subject()).through(
                proof.key(),
                TransactionRef.codec(),
                evidence -> ((Rollback) evidence).transaction()
            ),
            TIMEOUT
        );
    }

    private static NativeAttribution awaitUniqueAttribution(
        AmlT1Environment environment,
        ProofMessage proof
    ) {
        Awaitility.await("three-protocol subject attribution")
            .atMost(TIMEOUT)
            .untilAsserted(() -> {
                assertThat(environment.proofSubjects().correlation(
                    proof.subject(),
                    proof.key(),
                    SmppExchangeRef.codec()
                )).isInstanceOf(CorrelationResult.Unique.class);
                assertThat(environment.proofSubjects().correlation(
                    proof.subject(),
                    proof.key(),
                    HttpExchangeRef.codec()
                )).isInstanceOf(CorrelationResult.Unique.class);
                assertThat(environment.proofSubjects().correlation(
                    proof.subject(),
                    proof.key(),
                    TransactionRef.codec()
                )).isInstanceOf(CorrelationResult.Unique.class);
            });
        return uniqueAttribution(environment, proof);
    }

    private static NativeAttribution uniqueAttribution(
        AmlT1Environment environment,
        ProofMessage proof
    ) {
        return new NativeAttribution(
            uniqueCorrelation(
                environment,
                proof,
                SmppExchangeRef.codec()
            ),
            uniqueCorrelation(
                environment,
                proof,
                HttpExchangeRef.codec()
            ),
            uniqueCorrelation(
                environment,
                proof,
                TransactionRef.codec()
            )
        );
    }

    private static <T> T uniqueCorrelation(
        AmlT1Environment environment,
        ProofMessage proof,
        EvidenceCodec<T> codec
    ) {
        CorrelationResult<T> result = environment.proofSubjects().correlation(
            proof.subject(),
            proof.key(),
            codec
        );
        assertThat(result).isInstanceOf(CorrelationResult.Unique.class);
        return ((CorrelationResult.Unique<T>) result).nativeReference();
    }

    private static void assertAttributionEvidence(
        AmlT1Environment environment,
        NativeAttribution attribution
    ) {
        assertThat(smppEvidence(environment))
            .filteredOn(evidence -> evidence instanceof DeliverSmCompleted deliver
                && deliver.exchange().equals(attribution.smpp()))
            .hasSize(1);
        assertThat(smppEvidence(environment))
            .filteredOn(evidence ->
                evidence instanceof DeliverSmResponseCompleted response
                    && response.exchange().equals(attribution.smpp()))
            .hasSize(1);
        assertThat(httpEvidence(environment))
            .filteredOn(evidence -> evidence instanceof RequestCompleted request
                && request.exchange().equals(attribution.http()))
            .hasSize(1);
        assertThat(httpEvidence(environment))
            .filteredOn(evidence -> evidence instanceof ResponseCompleted response
                && response.exchange().equals(attribution.http()))
            .hasSizeLessThanOrEqualTo(1);
        assertThat(postgresqlEvidence(environment))
            .filteredOn(evidence -> evidence instanceof CommitAttempt attempt
                && attempt.transaction().equals(attribution.transaction()))
            .hasSize(1);
    }

    private static void assertPersistedAtomically(
        AmlT1Environment environment,
        TestSms message
    ) {
        SmsPersistence persisted = environment.database().await().rawAndOutboxVisible(message);
        assertThat(persisted.rawCount()).isEqualTo(1);
        assertThat(persisted.outboxCount()).isEqualTo(1);
        assertThat(persisted.rawId()).isEqualTo(persisted.outboxAggregateId());
        assertThat(persisted.sourceAddress()).isEqualTo(message.sourceAddress());
        assertThat(persisted.destinationAddress()).isEqualTo(message.destinationAddress());
        assertThat(persisted.content()).isEqualTo(message.content());
    }

    private static void assertNotPersisted(
        AmlT1Environment environment,
        TestSms message
    ) {
        assertThat(environment.database().snapshot(message))
            .satisfies(persistence -> {
                assertThat(persistence.rawCount()).isZero();
                assertThat(persistence.outboxCount()).isZero();
            });
    }

    private static void awaitCommitSucceeded(
        AmlT1Environment environment,
        TransactionRef transaction
    ) {
        Awaitility.await("matching PostgreSQL commit confirmation")
            .atMost(TIMEOUT)
            .untilAsserted(() -> assertThat(commitSuccesses(environment))
                .filteredOn(success -> success.transaction().equals(transaction))
                .hasSize(1));
    }

    private static void assertNextTransaction(
        TransactionRef previous,
        TransactionRef next
    ) {
        assertThat(next.sessionOrdinal()).isEqualTo(previous.sessionOrdinal());
        assertThat(next.transactionOrdinal())
            .isEqualTo(previous.transactionOrdinal() + 1);
    }

    private static void assertSecretSafe(
        AmlT1Environment environment,
        List<ProofMessage> proofs,
        List<String> additionalSecrets,
        Throwable failure
    ) {
        String durable = environment.journalSnapshot().entries().toString()
            + System.lineSeparator() + environment.diagnostics().content();
        String publicRendering = smppEvidence(environment).toString()
            + httpEvidence(environment)
            + postgresqlEvidence(environment)
            + proofs;
        List<String> secrets = new ArrayList<>(additionalSecrets);
        for (ProofMessage proof : proofs) {
            secrets.add(proof.discriminator());
            secrets.add(proof.message().id());
            secrets.add(proof.message().sourceAddress());
            secrets.add(proof.message().destinationAddress());
            secrets.add(proof.message().content());
        }
        secrets.addAll(environment.credentials());
        for (String secret : secrets) {
            assertThat(durable).doesNotContain(secret);
            assertThat(publicRendering).doesNotContain(secret);
            assertThat(exceptionRendering(failure)).doesNotContain(secret);
        }
    }

    private static String exceptionRendering(Throwable failure) {
        StringBuilder rendered = new StringBuilder();
        Throwable current = failure;
        while (current != null) {
            rendered.append(current.getClass().getName())
                .append(':')
                .append(current.getMessage())
                .append(System.lineSeparator());
            for (Throwable suppressed : current.getSuppressed()) {
                rendered.append(suppressed.getClass().getName())
                    .append(':')
                    .append(suppressed.getMessage())
                    .append(System.lineSeparator());
            }
            current = current.getCause();
        }
        return rendered.toString();
    }

    private static void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted at the submission barrier", failure);
        } catch (Exception failure) {
            throw new IllegalStateException("Submission barrier failed", failure);
        }
    }

    private static List<CommitAttempt> commitAttempts(AmlT1Environment environment) {
        return postgresqlEvidence(environment).stream()
            .filter(CommitAttempt.class::isInstance)
            .map(CommitAttempt.class::cast)
            .toList();
    }

    private static List<CommitSucceeded> commitSuccesses(
        AmlT1Environment environment
    ) {
        return postgresqlEvidence(environment).stream()
            .filter(CommitSucceeded.class::isInstance)
            .map(CommitSucceeded.class::cast)
            .toList();
    }

    private static List<Rollback> rollbacks(AmlT1Environment environment) {
        return postgresqlEvidence(environment).stream()
            .filter(Rollback.class::isInstance)
            .map(Rollback.class::cast)
            .toList();
    }

    private static List<PostgresqlEvidence> postgresqlEvidence(
        AmlT1Environment environment
    ) {
        return evidence(environment, environment.postgresqlAdapter().evidenceCodec());
    }

    private static List<HttpEvidence> httpEvidence(AmlT1Environment environment) {
        return evidence(environment, environment.httpAdapter().evidenceCodec());
    }

    private static List<SmppEvidence> smppEvidence(AmlT1Environment environment) {
        return evidence(environment, environment.smppAdapter().evidenceCodec());
    }

    private static <T> List<T> evidence(Environment environment, EvidenceCodec<T> codec) {
        return environment.journalSnapshot().entries().stream()
            .map(entry -> entry.event())
            .filter(InteractionObservationEvent.class::isInstance)
            .map(InteractionObservationEvent.class::cast)
            .filter(event -> event.evidence().schemaId().equals(codec.schemaId()))
            .map(event -> event.evidence().decode(codec))
            .toList();
    }

    private static void assertRequiredObservedRoutes(AmlT1Environment environment) {
        assertRequiredObservedRoute(environment, environment.smppConnectionId());
        assertRequiredObservedRoute(environment, environment.httpConnectionId());
        assertRequiredObservedRoute(environment, environment.databaseConnectionId());
    }

    private static void assertRequiredObservedRoute(
        AmlT1Environment environment,
        ConnectionId connectionId
    ) {
        assertThat(environment.runtimeConnection(connectionId))
            .satisfies(connection -> {
                assertThat(connection.state()).isEqualTo(ConnectionState.RUNNING);
                assertThat(connection.routingMode()).isEqualTo(RoutingMode.ROUTED);
                assertThat(connection.observationRequirement())
                    .isEqualTo(ObservationRequirement.REQUIRED);
                assertThat(connection.effectiveObservationStatus())
                    .isEqualTo(EffectiveObservationStatus.ACTIVE);
            });
    }

    private static final class ProofMessage {
        private final String discriminator;
        private final TestSms message;
        private final CorrelationKey key;
        private final ProofSubjectRef subject;

        private ProofMessage(
            String discriminator,
            TestSms message,
            CorrelationKey key,
            ProofSubjectRef subject
        ) {
            this.discriminator = discriminator;
            this.message = message;
            this.key = key;
            this.subject = subject;
        }

        private static ProofMessage create(AmlT1Environment environment) {
            String discriminator = UUID.randomUUID().toString();
            return rearm(
                environment,
                discriminator,
                TestSms.forProof(discriminator)
            );
        }

        private static ProofMessage rearm(
            AmlT1Environment environment,
            String discriminator,
            TestSms message
        ) {
            CorrelationKey key = SmsMessageFingerprint.of(message);
            ProofSubjectRef subject = environment.proofSubjects().create();
            environment.proofSubjects().arm(subject, key);
            return new ProofMessage(discriminator, message, key, subject);
        }

        private String discriminator() {
            return discriminator;
        }

        private TestSms message() {
            return message;
        }

        private CorrelationKey key() {
            return key;
        }

        private ProofSubjectRef subject() {
            return subject;
        }

        @Override
        public String toString() {
            return "ProofMessage[subject=" + subject + ", key=" + key + "]";
        }
    }

    private record NativeAttribution(
        SmppExchangeRef smpp,
        HttpExchangeRef http,
        TransactionRef transaction
    ) {}

    private record TransactionPair(
        ProofMessage unrelatedProof,
        NativeAttribution unrelated,
        ProofMessage targetProof,
        NativeAttribution target
    ) {}

    private record ConcurrentAttributions(
        ProofMessage firstProof,
        NativeAttribution first,
        ProofMessage secondProof,
        NativeAttribution second
    ) {}

}
