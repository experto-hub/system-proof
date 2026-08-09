package io.github.jacekkardys.systemproof.examples.sms;

import static io.github.jacekkardys.systemproof.endpoint.EndpointBinding.binding;
import static io.github.jacekkardys.systemproof.environment.ComponentPortFactory.provides;
import static io.github.jacekkardys.systemproof.environment.ComponentPortFactory.requiresAtStartup;
import static io.github.jacekkardys.systemproof.testcontainers.gateway.TcpEndpointAdapter.endpoint;
import static io.github.jacekkardys.systemproof.topology.Contract.contract;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.communication.Communication;
import io.github.jacekkardys.systemproof.component.AbstractComponent;
import io.github.jacekkardys.systemproof.component.ComponentId;
import io.github.jacekkardys.systemproof.component.ComponentType;
import io.github.jacekkardys.systemproof.configuration.RuntimeConfig;
import io.github.jacekkardys.systemproof.configuration.Secret;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticInteractionSelector;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardSpec;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorRequirement;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorViolation;
import io.github.jacekkardys.systemproof.diagnostics.JournalRenderer;
import io.github.jacekkardys.systemproof.driver.ComponentDriver;
import io.github.jacekkardys.systemproof.driver.ComponentRuntime;
import io.github.jacekkardys.systemproof.driver.DriverContext;
import io.github.jacekkardys.systemproof.endpoint.JdbcEndpoint;
import io.github.jacekkardys.systemproof.endpoint.SmppEndpoint;
import io.github.jacekkardys.systemproof.environment.ConnectionRouting;
import io.github.jacekkardys.systemproof.environment.Environment;
import io.github.jacekkardys.systemproof.environment.EnvironmentBuilder;
import io.github.jacekkardys.systemproof.environment.EnvironmentLogging;
import io.github.jacekkardys.systemproof.environment.EnvironmentTopology;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.postgres.PostgresComponent;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.postgres.SmsDatabaseOperations;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.SmsMessageFingerprint;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.TestSms;
import io.github.jacekkardys.systemproof.http.HttpEvidence;
import io.github.jacekkardys.systemproof.http.HttpEvidence.RequestCompleted;
import io.github.jacekkardys.systemproof.http.HttpEvidence.ResponseCompleted;
import io.github.jacekkardys.systemproof.http.HttpExchangeRef;
import io.github.jacekkardys.systemproof.http.HttpProtocolAdapter;
import io.github.jacekkardys.systemproof.journal.InteractionObservationEvent;
import io.github.jacekkardys.systemproof.journal.LogLevel;
import io.github.jacekkardys.systemproof.journal.SemanticPredecessorGuardEvent;
import io.github.jacekkardys.systemproof.observation.EvidenceCodec;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ObservationRequirement;
import io.github.jacekkardys.systemproof.observation.RequiredObservationProfile;
import io.github.jacekkardys.systemproof.observation.RequiredObservationProfile.Capability;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlEvidence;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlEvidence.CommitAttempt;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlEvidence.CommitSucceeded;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlProtocolAdapter;
import io.github.jacekkardys.systemproof.postgresql.TransactionRef;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.CorrelationResult;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.smpp.SmppEvidence;
import io.github.jacekkardys.systemproof.smpp.SmppEvidence.DeliverSmCompleted;
import io.github.jacekkardys.systemproof.smpp.SmppEvidence.DeliverSmResponseCompleted;
import io.github.jacekkardys.systemproof.smpp.SmppExchangeRef;
import io.github.jacekkardys.systemproof.smpp.SmppProtocolAdapter;
import io.github.jacekkardys.systemproof.testcontainers.gateway.InteractionGateway;
import io.github.jacekkardys.systemproof.testcontainers.gateway.ProtocolLimits;
import io.github.jacekkardys.systemproof.topology.Contract;
import io.github.jacekkardys.systemproof.topology.InteractionSpec;
import io.github.jacekkardys.systemproof.topology.ProtocolSpec;
import io.github.jacekkardys.systemproof.topology.ProvidedPort;
import io.github.jacekkardys.systemproof.topology.RequiredPort;

/** Exercises AML predecessor guards with real protocol bytes and deterministic test peers. */
@Tag("docker")
final class SemanticPredecessorGuardAmlIT {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final ProtocolLimits POSTGRESQL_LIMITS = new ProtocolLimits(
        1024 * 1024,
        2 * 1024 * 1024
    );
    private static final ProtocolLimits HTTP_LIMITS = new ProtocolLimits(
        1024 * 1024,
        2 * 1024 * 1024
    );
    private static final ProtocolLimits SMPP_LIMITS = new ProtocolLimits(
        64 * 1024,
        128 * 1024
    );
    private static final byte[] HTTP_ACK = "ACK/Jasmin".getBytes(StandardCharsets.US_ASCII);
    private static final String SMPP_SYSTEM_ID = "causal-peer";
    private static final String SMPP_PASSWORD = "causepwd";
    private static final Contract<SmppEndpoint> SMPP = contract("smpp", SmppEndpoint.class);
    private static final Contract<URI> HTTP = contract("sms", URI.class);
    private static final Contract<JdbcEndpoint> JDBC = contract("jdbc", JdbcEndpoint.class);

    @Test
    void shouldSatisfyBothAmlRelationsAndForwardEachSuccessorExactlyOnce()
        throws Exception {
        try (ControlledAmlEnvironment environment = ControlledAmlEnvironment.define(
            IngestionOrder.COMMIT_BEFORE_ACK,
            ClientOrder.HTTP_BEFORE_SMPP
        )) {
            environment.start();
            ProofMessage proof = ProofMessage.create(environment);
            GuardPair guards = armGuards(environment, proof);
            environment.client().prepare(proof.message());

            CompletableFuture<byte[]> smppResponse = environment.smsc()
                .send(proof.message());
            environment.client().completion().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);

            assertThat(guards.postgresqlToHttp().completion().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            )).isEqualTo(SemanticPredecessorGuardState.SATISFIED);
            assertThat(guards.httpToSmpp().completion().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            )).isEqualTo(SemanticPredecessorGuardState.SATISFIED);
            assertThat(smppResponse.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                .containsExactly(smppDeliverResponse(1));
            assertThat(environment.client().httpResponseBytes()).endsWith(HTTP_ACK);
            assertThat(countOccurrences(
                environment.client().httpResponseBytes(),
                HTTP_ACK
            )).isOne();
            assertThat(environment.ingestion().acknowledgements()).isEqualTo(1);

            NativeAttribution attribution = uniqueAttribution(environment, proof);
            assertMatchingEvidenceExactlyOnce(environment, attribution);
            assertThat(relationEvents(environment, guards)).hasSize(2)
                .allSatisfy(event -> {
                    assertThat(event.predecessor()).isPresent();
                    assertThat(event.successor()).isPresent();
                    assertThat(event.proofSubject()).isEqualTo(proof.subject());
                });
            assertPersistedAtomically(environment, proof.message());
            assertSecretSafe(environment, proof);
        }
    }

    @Test
    void shouldCloseEarlyHttpBeforeAnyAckByteAndRemainViolatedAfterCommitRelease()
        throws Exception {
        try (ControlledAmlEnvironment environment = ControlledAmlEnvironment.define(
            IngestionOrder.ACK_WHILE_COMMIT_HELD,
            ClientOrder.HTTP_BEFORE_SMPP
        )) {
            environment.start();
            ProofMessage proof = ProofMessage.create(environment);
            SemanticHold commit = commitHold(environment, proof);
            SemanticPredecessorGuard guard = armPostgresqlToHttp(environment, proof);
            environment.client().prepare(proof.message());

            CompletableFuture<byte[]> smppResponse = environment.smsc()
                .send(proof.message());
            commit.reached().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            );
            TransactionRef transaction = uniqueCorrelation(
                environment,
                proof,
                TransactionRef.codec()
            );
            assertThat(commitAttempts(environment))
                .filteredOn(attempt -> attempt.transaction().equals(transaction))
                .hasSize(1);
            assertThat(commitSuccesses(environment))
                .noneMatch(success -> success.transaction().equals(transaction));

            environment.ingestion().allowAcknowledgement();
            assertThat(guard.completion().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            )).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
            environment.client().completion().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertThat(environment.client().httpResponseBytes()).isEmpty();
            assertViolation(environment, guard, proof.subject());

            commit.release().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            );
            awaitCommitSucceeded(environment, transaction);
            assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
            assertThat(smppResponse.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isEmpty();
            assertThat(violationEvents(environment, guard)).hasSize(1);
        }
    }

    @Test
    void shouldCloseEarlySmppWhileHttpIsAuthorizedButNotForwarded()
        throws Exception {
        try (ControlledAmlEnvironment environment = ControlledAmlEnvironment.define(
            IngestionOrder.COMMIT_BEFORE_ACK,
            ClientOrder.HTTP_AUTHORIZED_BEFORE_EARLY_SMPP
        )) {
            environment.start();
            ProofMessage proof = ProofMessage.create(environment);
            GuardPair guards = armGuards(environment, proof);
            SemanticHold httpResponse = positiveHttpResponseHold(environment, proof);
            environment.client().prepare(proof.message());

            CompletableFuture<byte[]> smppResponse = environment.smsc()
                .send(proof.message());
            httpResponse.reached().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            );
            assertThat(guards.postgresqlToHttp().state())
                .isEqualTo(SemanticPredecessorGuardState.SUCCESSOR_AUTHORIZED);
            assertThat(guards.httpToSmpp().state())
                .isEqualTo(SemanticPredecessorGuardState.PREDECESSOR_OBSERVED);

            environment.client().allowEarlySmpp();
            assertThat(guards.httpToSmpp().completion().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            )).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
            assertThat(smppResponse.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS)).isEmpty();
            assertViolation(environment, guards.httpToSmpp(), proof.subject());

            httpResponse.release().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            );
            environment.client().completion().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertThat(guards.postgresqlToHttp().state())
                .isEqualTo(SemanticPredecessorGuardState.SATISFIED);
            assertThat(guards.httpToSmpp().state())
                .isEqualTo(SemanticPredecessorGuardState.VIOLATED);
            assertThat(environment.client().httpResponseBytes()).contains(HTTP_ACK);
            assertThat(violationEvents(environment, guards.httpToSmpp())).hasSize(1);
        }
    }

    private static GuardPair armGuards(
        ControlledAmlEnvironment environment,
        ProofMessage proof
    ) {
        SemanticInteractionSelector<HttpEvidence> response = positiveHttpResponse(
            environment,
            proof
        );
        return new GuardPair(
            armPostgresqlToHttp(environment, proof, response),
            environment.controls().guard(SemanticPredecessorGuardSpec.requiring(
                proof.subject(),
                SemanticPredecessorRequirement.forwarded(response),
                SemanticInteractionSelector.matching(
                    environment.smppConnectionId(),
                    FlowDirection.CONSUMER_TO_PROVIDER,
                    environment.smppAdapter().evidenceCodec(),
                    evidence -> evidence instanceof DeliverSmResponseCompleted completed
                        && completed.acknowledgement()
                            == SmppEvidence.Acknowledgement.POSITIVE
                ).forSubject(proof.subject()).through(
                    proof.key(),
                    SmppExchangeRef.codec(),
                    evidence -> ((DeliverSmResponseCompleted) evidence).exchange()
                ),
                TIMEOUT
            ))
        );
    }

    private static SemanticPredecessorGuard armPostgresqlToHttp(
        ControlledAmlEnvironment environment,
        ProofMessage proof
    ) {
        return armPostgresqlToHttp(
            environment,
            proof,
            positiveHttpResponse(environment, proof)
        );
    }

    private static SemanticPredecessorGuard armPostgresqlToHttp(
        ControlledAmlEnvironment environment,
        ProofMessage proof,
        SemanticInteractionSelector<HttpEvidence> response
    ) {
        return environment.controls().guard(SemanticPredecessorGuardSpec.requiring(
            proof.subject(),
            SemanticPredecessorRequirement.confirmed(
                SemanticInteractionSelector.matching(
                    environment.databaseConnectionId(),
                    FlowDirection.PROVIDER_TO_CONSUMER,
                    environment.postgresqlAdapter().evidenceCodec(),
                    CommitSucceeded.class::isInstance
                ).forSubject(proof.subject()).through(
                    proof.key(),
                    TransactionRef.codec(),
                    evidence -> ((CommitSucceeded) evidence).transaction()
                )
            ),
            response,
            TIMEOUT
        ));
    }

    private static SemanticInteractionSelector<HttpEvidence> positiveHttpResponse(
        ControlledAmlEnvironment environment,
        ProofMessage proof
    ) {
        return SemanticInteractionSelector.matching(
            environment.httpConnectionId(),
            FlowDirection.PROVIDER_TO_CONSUMER,
            environment.httpAdapter().evidenceCodec(),
            evidence -> evidence instanceof ResponseCompleted completed
                && completed.acknowledgement() == HttpEvidence.Acknowledgement.POSITIVE
        ).forSubject(proof.subject()).through(
            proof.key(),
            HttpExchangeRef.codec(),
            evidence -> ((ResponseCompleted) evidence).exchange()
        );
    }

    private static SemanticHold positiveHttpResponseHold(
        ControlledAmlEnvironment environment,
        ProofMessage proof
    ) {
        return environment.controls().arm(
            positiveHttpResponse(environment, proof),
            TIMEOUT
        );
    }

    private static SemanticHold commitHold(
        ControlledAmlEnvironment environment,
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

    private static void assertViolation(
        ControlledAmlEnvironment environment,
        SemanticPredecessorGuard guard,
        ProofSubjectRef subject
    ) {
        assertThat(violationEvents(environment, guard)).singleElement().satisfies(event -> {
            assertThat(event.proofSubject()).isEqualTo(subject);
            assertThat(event.violation())
                .contains(SemanticPredecessorViolation.PREDECESSOR_NOT_ESTABLISHED);
            assertThat(event.decision()).contains(ForwardingDecision.CLOSE_SESSION);
            assertThat(event.successor()).isPresent();
        });
    }

    private static List<SemanticPredecessorGuardEvent> relationEvents(
        ControlledAmlEnvironment environment,
        GuardPair guards
    ) {
        return guardEvents(environment).stream()
            .filter(event -> event.kind() == SemanticPredecessorGuardEvent.Kind.TERMINAL
                && event.state() == SemanticPredecessorGuardState.SATISFIED)
            .filter(event -> event.guardRef().equals(guards.postgresqlToHttp().ref())
                || event.guardRef().equals(guards.httpToSmpp().ref()))
            .toList();
    }

    private static List<SemanticPredecessorGuardEvent> violationEvents(
        ControlledAmlEnvironment environment,
        SemanticPredecessorGuard guard
    ) {
        return guardEvents(environment).stream()
            .filter(event -> event.kind() == SemanticPredecessorGuardEvent.Kind.TERMINAL
                && event.state() == SemanticPredecessorGuardState.VIOLATED)
            .filter(event -> event.guardRef().equals(guard.ref()))
            .toList();
    }

    private static List<SemanticPredecessorGuardEvent> guardEvents(
        ControlledAmlEnvironment environment
    ) {
        return environment.journalSnapshot().entries().stream()
            .map(entry -> entry.event())
            .filter(SemanticPredecessorGuardEvent.class::isInstance)
            .map(SemanticPredecessorGuardEvent.class::cast)
            .toList();
    }

    private static NativeAttribution uniqueAttribution(
        ControlledAmlEnvironment environment,
        ProofMessage proof
    ) {
        return new NativeAttribution(
            uniqueCorrelation(environment, proof, SmppExchangeRef.codec()),
            uniqueCorrelation(environment, proof, HttpExchangeRef.codec()),
            uniqueCorrelation(environment, proof, TransactionRef.codec())
        );
    }

    @SuppressWarnings("unchecked")
    private static <T> T uniqueCorrelation(
        ControlledAmlEnvironment environment,
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

    private static void assertMatchingEvidenceExactlyOnce(
        ControlledAmlEnvironment environment,
        NativeAttribution attribution
    ) {
        assertThat(smppEvidence(environment))
            .filteredOn(evidence -> evidence instanceof DeliverSmCompleted completed
                && completed.exchange().equals(attribution.smpp()))
            .hasSize(1);
        assertThat(smppEvidence(environment))
            .filteredOn(evidence -> evidence instanceof DeliverSmResponseCompleted completed
                && completed.exchange().equals(attribution.smpp()))
            .hasSize(1);
        assertThat(httpEvidence(environment))
            .filteredOn(evidence -> evidence instanceof RequestCompleted completed
                && completed.exchange().equals(attribution.http()))
            .hasSize(1);
        assertThat(httpEvidence(environment))
            .filteredOn(evidence -> evidence instanceof ResponseCompleted completed
                && completed.exchange().equals(attribution.http()))
            .hasSize(1);
        assertThat(postgresqlEvidence(environment))
            .filteredOn(evidence -> evidence instanceof CommitAttempt attempt
                && attempt.transaction().equals(attribution.transaction()))
            .hasSize(1);
        assertThat(postgresqlEvidence(environment))
            .filteredOn(evidence -> evidence instanceof CommitSucceeded succeeded
                && succeeded.transaction().equals(attribution.transaction()))
            .hasSize(1);
    }

    private static void assertPersistedAtomically(
        ControlledAmlEnvironment environment,
        TestSms message
    ) {
        assertThat(environment.database().await().rawAndOutboxVisible(message))
            .satisfies(persistence -> {
                assertThat(persistence.rawCount()).isEqualTo(1);
                assertThat(persistence.outboxCount()).isEqualTo(1);
                assertThat(persistence.rawId()).isEqualTo(persistence.outboxAggregateId());
            });
    }

    private static void assertSecretSafe(
        ControlledAmlEnvironment environment,
        ProofMessage proof
    ) {
        String rendering = new JournalRenderer().render(environment.journalSnapshot())
            + System.lineSeparator() + environment.diagnostics().content()
            + System.lineSeparator() + proof;
        assertThat(rendering).doesNotContain(
            proof.message().id(),
            proof.message().sourceAddress(),
            proof.message().destinationAddress(),
            proof.message().content(),
            SMPP_PASSWORD,
            environment.databasePassword()
        );
    }

    private static void awaitCommitSucceeded(
        ControlledAmlEnvironment environment,
        TransactionRef transaction
    ) {
        Awaitility.await("matching PostgreSQL commit confirmation")
            .atMost(TIMEOUT)
            .untilAsserted(() -> assertThat(commitSuccesses(environment))
                .filteredOn(success -> success.transaction().equals(transaction))
                .hasSize(1));
    }

    private static List<CommitAttempt> commitAttempts(ControlledAmlEnvironment environment) {
        return postgresqlEvidence(environment).stream()
            .filter(CommitAttempt.class::isInstance)
            .map(CommitAttempt.class::cast)
            .toList();
    }

    private static List<CommitSucceeded> commitSuccesses(
        ControlledAmlEnvironment environment
    ) {
        return postgresqlEvidence(environment).stream()
            .filter(CommitSucceeded.class::isInstance)
            .map(CommitSucceeded.class::cast)
            .toList();
    }

    private static List<PostgresqlEvidence> postgresqlEvidence(
        ControlledAmlEnvironment environment
    ) {
        return evidence(environment, environment.postgresqlAdapter().evidenceCodec());
    }

    private static List<HttpEvidence> httpEvidence(ControlledAmlEnvironment environment) {
        return evidence(environment, environment.httpAdapter().evidenceCodec());
    }

    private static List<SmppEvidence> smppEvidence(ControlledAmlEnvironment environment) {
        return evidence(environment, environment.smppAdapter().evidenceCodec());
    }

    private static <T> List<T> evidence(
        ControlledAmlEnvironment environment,
        EvidenceCodec<T> codec
    ) {
        return environment.journalSnapshot().entries().stream()
            .map(entry -> entry.event())
            .filter(InteractionObservationEvent.class::isInstance)
            .map(InteractionObservationEvent.class::cast)
            .filter(event -> event.evidence().schemaId().equals(codec.schemaId()))
            .map(event -> event.evidence().decode(codec))
            .toList();
    }

    private static RequiredObservationProfile requiredProfile(
        EvidenceCodec<?> evidence,
        EvidenceCodec<?> nativeReference
    ) {
        return new RequiredObservationProfile(
            evidence.schemaId(),
            Optional.of(nativeReference.schemaId()),
            Set.of(
                Capability.CORRELATION_CONTRIBUTIONS,
                Capability.SEMANTIC_CONTROL
            ),
            Set.of()
        );
    }

    private record ProofMessage(
        TestSms message,
        ProofSubjectRef subject,
        CorrelationKey key
    ) {
        private static ProofMessage create(ControlledAmlEnvironment environment) {
            TestSms message = TestSms.forProof(UUID.randomUUID().toString());
            CorrelationKey key = SmsMessageFingerprint.of(message);
            ProofSubjectRef subject = environment.proofSubjects().create();
            environment.proofSubjects().arm(subject, key);
            return new ProofMessage(message, subject, key);
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

    private record GuardPair(
        SemanticPredecessorGuard postgresqlToHttp,
        SemanticPredecessorGuard httpToSmpp
    ) {}

    private enum IngestionOrder {
        COMMIT_BEFORE_ACK,
        ACK_WHILE_COMMIT_HELD
    }

    private enum ClientOrder {
        HTTP_BEFORE_SMPP,
        HTTP_AUTHORIZED_BEFORE_EARLY_SMPP
    }

    private static final class ControlledAmlEnvironment extends Environment {
        private final ControlledSmscComponent smsc;
        private final ControlledClientComponent client;
        private final ControlledIngestionComponent ingestion;
        private final PostgresComponent database;
        private final SmppProtocolAdapter smppAdapter;
        private final HttpProtocolAdapter httpAdapter;
        private final PostgresqlProtocolAdapter postgresqlAdapter;

        private ControlledAmlEnvironment(
            EnvironmentTopology topology,
            EnvironmentLogging logging,
            ConnectionRouting routing,
            ControlledSmscComponent smsc,
            ControlledClientComponent client,
            ControlledIngestionComponent ingestion,
            PostgresComponent database,
            SmppProtocolAdapter smppAdapter,
            HttpProtocolAdapter httpAdapter,
            PostgresqlProtocolAdapter postgresqlAdapter
        ) {
            super(topology, logging, routing);
            this.smsc = smsc;
            this.client = client;
            this.ingestion = ingestion;
            this.database = database;
            this.smppAdapter = smppAdapter;
            this.httpAdapter = httpAdapter;
            this.postgresqlAdapter = postgresqlAdapter;
        }

        private static ControlledAmlEnvironment define(
            IngestionOrder ingestionOrder,
            ClientOrder clientOrder
        ) {
            ControlledSmscComponent smsc = new ControlledSmscComponent(
                new ControlledSmscDriver()
            );
            ControlledIngestionComponent ingestion = new ControlledIngestionComponent(
                new ControlledIngestionDriver(ingestionOrder)
            );
            ControlledClientComponent client = new ControlledClientComponent(
                new ControlledClientDriver(clientOrder)
            );
            EnvironmentBuilder builder = new EnvironmentBuilder()
                .components(smsc, ingestion, client);
            PostgresComponent database = builder.component(PostgresComponent.class);
            builder
                .logging(EnvironmentLogging.logs().defaultComponentLevel(LogLevel.OFF))
                .connect(client.smpp, smsc.smpp)
                .connect(client.http, ingestion.http)
                .connect(ingestion.jdbc, database.jdbc());

            SmppProtocolAdapter smppAdapter = new SmppProtocolAdapter(
                SmsMessageFingerprint.smppDeliverCorrelation()
            );
            HttpProtocolAdapter httpAdapter = new HttpProtocolAdapter(
                SmsMessageFingerprint.httpCallbackCorrelation()
            );
            PostgresqlProtocolAdapter postgresqlAdapter = new PostgresqlProtocolAdapter(
                SmsMessageFingerprint.rawWriteCorrelation()
            );
            InteractionGateway gateway = new InteractionGateway();
            ConnectionRouting routing = ConnectionRouting.routed(
                client.smpp.contract(),
                requiredProfile(smppAdapter.evidenceCodec(), SmppExchangeRef.codec()),
                gateway.tcp(endpoint(
                    value -> new InetSocketAddress(value.host(), value.port()),
                    (value, host, port) -> new SmppEndpoint(
                        host,
                        port,
                        value.systemId(),
                        value.password()
                    )
                ), smppAdapter, SMPP_LIMITS)
            ).withRoute(
                client.http.contract(),
                requiredProfile(httpAdapter.evidenceCodec(), HttpExchangeRef.codec()),
                gateway.tcp(endpoint(
                    value -> new InetSocketAddress(value.getHost(), value.getPort()),
                    (value, host, port) -> replaceUri(value, host, port)
                ), httpAdapter, HTTP_LIMITS)
            ).withRoute(
                ingestion.jdbc.contract(),
                requiredProfile(
                    postgresqlAdapter.evidenceCodec(),
                    TransactionRef.codec()
                ),
                gateway.tcp(endpoint(
                    value -> postgresqlAddress(value.url()),
                    (value, host, port) -> replaceJdbcAddress(value, host, port)
                ), postgresqlAdapter, POSTGRESQL_LIMITS)
            );
            return builder.build((topology, logging) -> new ControlledAmlEnvironment(
                topology,
                logging,
                routing,
                smsc,
                client,
                ingestion,
                database,
                smppAdapter,
                httpAdapter,
                postgresqlAdapter
            ));
        }

        private ControlledSmscOperations smsc() {
            return operations(smsc);
        }

        private ControlledClientOperations client() {
            return operations(client);
        }

        private ControlledIngestionOperations ingestion() {
            return operations(ingestion);
        }

        private SmsDatabaseOperations database() {
            return operations(database);
        }

        private String databasePassword() {
            return database.configuration().password().reveal();
        }

        private io.github.jacekkardys.systemproof.topology.ConnectionId smppConnectionId() {
            return connectionFrom(client.smpp).id();
        }

        private io.github.jacekkardys.systemproof.topology.ConnectionId httpConnectionId() {
            return connectionFrom(client.http).id();
        }

        private io.github.jacekkardys.systemproof.topology.ConnectionId databaseConnectionId() {
            return connectionFrom(ingestion.jdbc).id();
        }

        private SmppProtocolAdapter smppAdapter() {
            return smppAdapter;
        }

        private HttpProtocolAdapter httpAdapter() {
            return httpAdapter;
        }

        private PostgresqlProtocolAdapter postgresqlAdapter() {
            return postgresqlAdapter;
        }
    }

    private record EmptyConfig() implements RuntimeConfig {}

    private enum Invocation implements InteractionSpec {
        INSTANCE;

        @Override
        public String id() {
            return "invocation";
        }
    }

    private enum Session implements InteractionSpec {
        INSTANCE;

        @Override
        public String id() {
            return "session";
        }
    }

    private enum ResourceAccess implements InteractionSpec {
        INSTANCE;

        @Override
        public String id() {
            return "resource-access";
        }
    }

    private enum Http implements ProtocolSpec {
        INSTANCE;

        @Override
        public String id() {
            return "http";
        }

        @Override
        public String scheme() {
            return "http";
        }
    }

    private enum Smpp implements ProtocolSpec {
        INSTANCE;

        @Override
        public String id() {
            return "smpp";
        }

        @Override
        public String scheme() {
            return "smpp";
        }
    }

    private enum JdbcPostgresql implements ProtocolSpec {
        INSTANCE;

        @Override
        public String id() {
            return "jdbc-postgresql";
        }

        @Override
        public String scheme() {
            return "jdbc:postgresql";
        }
    }

    private static final class ControlledSmscComponent
        extends AbstractComponent<EmptyConfig, ControlledSmscOperations> {
        private final ProvidedPort<SmppEndpoint> smpp;

        private ControlledSmscComponent(
            ComponentDriver<EmptyConfig, ControlledSmscOperations> driver
        ) {
            super(
                ComponentId.component(ComponentType.of("controlled-smsc")),
                new EmptyConfig(),
                ControlledSmscOperations.class,
                driver
            );
            smpp = provides(this, "smpp", SMPP, Session.INSTANCE, Smpp.INSTANCE);
        }
    }

    private static final class ControlledIngestionComponent
        extends AbstractComponent<EmptyConfig, ControlledIngestionOperations> {
        private final ProvidedPort<URI> http;
        private final RequiredPort<JdbcEndpoint> jdbc;

        private ControlledIngestionComponent(
            ComponentDriver<EmptyConfig, ControlledIngestionOperations> driver
        ) {
            super(
                ComponentId.component(ComponentType.of("controlled-ingestion")),
                new EmptyConfig(),
                ControlledIngestionOperations.class,
                driver
            );
            http = provides(this, "sms", HTTP, Invocation.INSTANCE, Http.INSTANCE);
            jdbc = requiresAtStartup(
                this,
                "jdbc",
                JDBC,
                ResourceAccess.INSTANCE,
                JdbcPostgresql.INSTANCE
            );
        }
    }

    private static final class ControlledClientComponent
        extends AbstractComponent<EmptyConfig, ControlledClientOperations> {
        private final RequiredPort<SmppEndpoint> smpp;
        private final RequiredPort<URI> http;

        private ControlledClientComponent(
            ComponentDriver<EmptyConfig, ControlledClientOperations> driver
        ) {
            super(
                ComponentId.component(ComponentType.of("controlled-aml-client")),
                new EmptyConfig(),
                ControlledClientOperations.class,
                driver
            );
            smpp = requiresAtStartup(this, "smpp", SMPP, Session.INSTANCE, Smpp.INSTANCE);
            http = requiresAtStartup(this, "sms", HTTP, Invocation.INSTANCE, Http.INSTANCE);
        }
    }

    private static final class ControlledSmscDriver
        implements ComponentDriver<EmptyConfig, ControlledSmscOperations> {
        @Override
        public ComponentRuntime<ControlledSmscOperations> start(
            AbstractComponent<EmptyConfig, ControlledSmscOperations> component,
            DriverContext context
        ) {
            ControlledSmscComponent smsc = (ControlledSmscComponent) component;
            try {
                ControlledSmscOperations operations = ControlledSmscOperations.open();
                SmppEndpoint address = new SmppEndpoint(
                    "127.0.0.1",
                    operations.port(),
                    SMPP_SYSTEM_ID,
                    Secret.secret(SMPP_PASSWORD)
                );
                return ComponentRuntime.<ControlledSmscOperations>runtime(operations)
                    .provides(smsc.smpp, binding(address, address))
                    .operations(operations)
                    .build();
            } catch (IOException exception) {
                throw new IllegalStateException("Cannot start the controlled SMSC", exception);
            }
        }
    }

    private static final class ControlledIngestionDriver
        implements ComponentDriver<EmptyConfig, ControlledIngestionOperations> {
        private final IngestionOrder order;

        private ControlledIngestionDriver(IngestionOrder order) {
            this.order = order;
        }

        @Override
        public ComponentRuntime<ControlledIngestionOperations> start(
            AbstractComponent<EmptyConfig, ControlledIngestionOperations> component,
            DriverContext context
        ) {
            ControlledIngestionComponent ingestion = (ControlledIngestionComponent) component;
            JdbcEndpoint resolvedDatabase = context.resolve(ingestion.jdbc);
            JdbcEndpoint database = replaceJdbcAddress(
                resolvedDatabase,
                "127.0.0.1",
                postgresqlAddress(resolvedDatabase.url()).getPort()
            );
            try {
                ControlledIngestionOperations operations = ControlledIngestionOperations.open(
                    database,
                    order
                );
                URI address = URI.create(
                    "http://127.0.0.1:" + operations.port() + "/v1/ingestion/sms"
                );
                return ComponentRuntime.<ControlledIngestionOperations>runtime(operations)
                    .provides(ingestion.http, binding(address, address))
                    .operations(operations)
                    .build();
            } catch (Exception exception) {
                throw new IllegalStateException(
                    "Cannot start the controlled ingestion peer",
                    exception
                );
            }
        }
    }

    private static final class ControlledClientDriver
        implements ComponentDriver<EmptyConfig, ControlledClientOperations> {
        private final ClientOrder order;

        private ControlledClientDriver(ClientOrder order) {
            this.order = order;
        }

        @Override
        public ComponentRuntime<ControlledClientOperations> start(
            AbstractComponent<EmptyConfig, ControlledClientOperations> component,
            DriverContext context
        ) {
            ControlledClientComponent client = (ControlledClientComponent) component;
            SmppEndpoint resolvedSmsc = context.resolve(client.smpp);
            SmppEndpoint smsc = new SmppEndpoint(
                "127.0.0.1",
                resolvedSmsc.port(),
                resolvedSmsc.systemId(),
                resolvedSmsc.password()
            );
            URI resolvedCallback = context.resolve(client.http);
            URI callback = replaceUri(
                resolvedCallback,
                "127.0.0.1",
                resolvedCallback.getPort()
            );
            try {
                ControlledClientOperations operations = ControlledClientOperations.open(
                    smsc,
                    callback,
                    order
                );
                return ComponentRuntime.<ControlledClientOperations>runtime(operations)
                    .operations(operations)
                    .build();
            } catch (IOException exception) {
                throw new IllegalStateException("Cannot start the controlled AML client", exception);
            }
        }
    }

    private static final class ControlledSmscOperations implements AutoCloseable {
        private final ServerSocket listener;
        private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
        private final CountDownLatch bound = new CountDownLatch(1);
        private final AtomicReference<Socket> connection = new AtomicReference<>();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicInteger deliveries = new AtomicInteger();

        private ControlledSmscOperations(ServerSocket listener) {
            this.listener = listener;
            tasks.submit(this::acceptAndBind);
        }

        private static ControlledSmscOperations open() throws IOException {
            ServerSocket listener = new ServerSocket();
            listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            return new ControlledSmscOperations(listener);
        }

        private int port() {
            return listener.getLocalPort();
        }

        private CompletableFuture<byte[]> send(TestSms message) throws Exception {
            Objects.requireNonNull(message, "message must not be null");
            assertAwait(bound, "controlled SMPP bind");
            requireHealthy();
            if (deliveries.incrementAndGet() != 1) {
                throw new IllegalStateException("The controlled SMSC accepts one delivery");
            }
            Socket socket = Objects.requireNonNull(connection.get(), "bound socket");
            byte[] deliver = smppDeliver(1, message);
            socket.getOutputStream().write(deliver);
            socket.getOutputStream().flush();
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return readPdu(socket.getInputStream()).orElseGet(() -> new byte[0]);
                } catch (IOException exception) {
                    return new byte[0];
                }
            }, tasks);
        }

        private void acceptAndBind() {
            try {
                Socket socket = listener.accept();
                socket.setSoTimeout((int) TIMEOUT.toMillis());
                connection.set(socket);
                byte[] request = readPdu(socket.getInputStream()).orElseThrow();
                ByteBuffer header = ByteBuffer.wrap(request).order(ByteOrder.BIG_ENDIAN);
                assertThat(Integer.toUnsignedLong(header.getInt(4))).isEqualTo(0x00000009L);
                long sequence = Integer.toUnsignedLong(header.getInt(12));
                socket.getOutputStream().write(smppBindResponse(sequence));
                socket.getOutputStream().flush();
                bound.countDown();
            } catch (Throwable throwable) {
                failure.compareAndSet(null, throwable);
                bound.countDown();
            }
        }

        private void requireHealthy() {
            Throwable throwable = failure.get();
            if (throwable != null) {
                throw new IllegalStateException("Controlled SMSC failed", throwable);
            }
        }

        @Override
        public void close() throws Exception {
            Socket socket = connection.get();
            if (socket != null) {
                socket.close();
            }
            listener.close();
            tasks.shutdownNow();
            assertThat(tasks.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class ControlledClientOperations implements AutoCloseable {
        private final Socket smpp;
        private final URI callback;
        private final ClientOrder order;
        private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
        private final CountDownLatch allowEarlySmpp = new CountDownLatch(1);
        private final AtomicReference<TestSms> expected = new AtomicReference<>();
        private final AtomicReference<byte[]> httpResponse = new AtomicReference<>(new byte[0]);
        private final CompletableFuture<Void> completion = new CompletableFuture<>();

        private ControlledClientOperations(Socket smpp, URI callback, ClientOrder order) {
            this.smpp = smpp;
            this.callback = callback;
            this.order = order;
            tasks.submit(this::handleDelivery);
        }

        private static ControlledClientOperations open(
            SmppEndpoint endpoint,
            URI callback,
            ClientOrder order
        ) throws IOException {
            Socket smpp = new Socket();
            smpp.connect(new InetSocketAddress(endpoint.host(), endpoint.port()));
            smpp.setSoTimeout((int) TIMEOUT.toMillis());
            smpp.getOutputStream().write(smppBindRequest(
                1,
                endpoint.systemId(),
                endpoint.password().reveal()
            ));
            smpp.getOutputStream().flush();
            byte[] response = readPdu(smpp.getInputStream()).orElseThrow();
            ByteBuffer header = ByteBuffer.wrap(response).order(ByteOrder.BIG_ENDIAN);
            if (Integer.toUnsignedLong(header.getInt(4)) != 0x80000009L
                || header.getInt(8) != 0) {
                throw new IOException("Controlled SMPP bind was rejected");
            }
            return new ControlledClientOperations(smpp, callback, order);
        }

        private void prepare(TestSms message) {
            if (!expected.compareAndSet(null, Objects.requireNonNull(message))) {
                throw new IllegalStateException("The controlled client accepts one message");
            }
        }

        private void allowEarlySmpp() {
            allowEarlySmpp.countDown();
        }

        private CompletableFuture<Void> completion() {
            return completion;
        }

        private byte[] httpResponseBytes() {
            return httpResponse.get().clone();
        }

        private void handleDelivery() {
            try {
                byte[] deliver = readPdu(smpp.getInputStream()).orElseThrow();
                assertThat(Integer.toUnsignedLong(
                    ByteBuffer.wrap(deliver).order(ByteOrder.BIG_ENDIAN).getInt(4)
                )).isEqualTo(0x00000005L);
                TestSms message = awaitExpected();
                CompletableFuture<byte[]> response = CompletableFuture.supplyAsync(
                    () -> exchangeHttp(callback, message),
                    tasks
                );
                if (order == ClientOrder.HTTP_AUTHORIZED_BEFORE_EARLY_SMPP) {
                    assertAwait(allowEarlySmpp, "authorization of the early SMPP response");
                    writeSmppResponse();
                }
                byte[] bytes = response.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                httpResponse.set(bytes.clone());
                if (order == ClientOrder.HTTP_BEFORE_SMPP) {
                    if (isPositiveHttpResponse(bytes)) {
                        writeSmppResponse();
                    } else {
                        smpp.close();
                    }
                }
                completion.complete(null);
            } catch (Throwable throwable) {
                completion.completeExceptionally(throwable);
            }
        }

        private TestSms awaitExpected() throws InterruptedException {
            Awaitility.await("controlled client proof message")
                .atMost(TIMEOUT)
                .until(() -> expected.get() != null);
            return expected.get();
        }

        private void writeSmppResponse() throws IOException {
            smpp.getOutputStream().write(smppDeliverResponse(1));
            smpp.getOutputStream().flush();
        }

        @Override
        public void close() throws Exception {
            smpp.close();
            tasks.shutdownNow();
            assertThat(tasks.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class ControlledIngestionOperations implements AutoCloseable {
        private final JdbcEndpoint database;
        private final IngestionOrder order;
        private final HttpServer server;
        private final ExecutorService requests = Executors.newVirtualThreadPerTaskExecutor();
        private final CountDownLatch allowAcknowledgement = new CountDownLatch(1);
        private final AtomicInteger acknowledgements = new AtomicInteger();
        private final AtomicReference<CompletableFuture<Void>> commit = new AtomicReference<>();

        private ControlledIngestionOperations(
            JdbcEndpoint database,
            IngestionOrder order,
            HttpServer server
        ) {
            this.database = database;
            this.order = order;
            this.server = server;
            server.createContext("/v1/ingestion/sms", this::ingest);
            server.setExecutor(requests);
            server.start();
        }

        private static ControlledIngestionOperations open(
            JdbcEndpoint database,
            IngestionOrder order
        ) throws Exception {
            initializeSchema(database);
            HttpServer server = HttpServer.create(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                0
            );
            return new ControlledIngestionOperations(database, order, server);
        }

        private int port() {
            return server.getAddress().getPort();
        }

        private int acknowledgements() {
            return acknowledgements.get();
        }

        private void allowAcknowledgement() {
            allowAcknowledgement.countDown();
        }

        private void ingest(HttpExchange exchange) throws IOException {
            CompletableFuture<Void> commitFuture = null;
            try (Connection connection = connect(database)) {
                TestSms message = callbackMessage(exchange.getRequestBody().readAllBytes());
                connection.setAutoCommit(false);
                insert(connection, message);
                if (order == IngestionOrder.COMMIT_BEFORE_ACK) {
                    connection.commit();
                } else {
                    commitFuture = CompletableFuture.runAsync(() -> commit(connection), requests);
                    if (!commit.compareAndSet(null, commitFuture)) {
                        throw new IllegalStateException("Only one controlled commit is supported");
                    }
                    assertAwait(
                        allowAcknowledgement,
                        "test authorization of the deliberately early HTTP acknowledgement"
                    );
                }
                exchange.getResponseHeaders().add("Content-Type", "text/plain");
                exchange.getResponseHeaders().add("Connection", "close");
                exchange.sendResponseHeaders(200, HTTP_ACK.length);
                exchange.getResponseBody().write(HTTP_ACK);
                exchange.getResponseBody().flush();
                acknowledgements.incrementAndGet();
                if (commitFuture != null) {
                    commitFuture.join();
                }
            } catch (Throwable throwable) {
                throw new IOException("Controlled ingestion failed", throwable);
            } finally {
                exchange.close();
            }
        }

        private static void initializeSchema(JdbcEndpoint database) throws Exception {
            try (Connection connection = connect(database);
                 Statement statement = connection.createStatement()) {
                statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS raw_sms_event (
                        id UUID PRIMARY KEY,
                        external_message_id VARCHAR(255) NOT NULL,
                        source_address VARCHAR(255) NOT NULL,
                        destination_address VARCHAR(255) NOT NULL,
                        content TEXT NOT NULL
                    )
                    """);
                statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS outbox_event (
                        id UUID PRIMARY KEY,
                        aggregate_id UUID NOT NULL REFERENCES raw_sms_event(id),
                        event_type VARCHAR(255) NOT NULL
                    )
                    """);
            }
        }

        private static void insert(Connection connection, TestSms message) throws Exception {
            UUID rawId = UUID.randomUUID();
            try (PreparedStatement raw = connection.prepareStatement("""
                INSERT INTO raw_sms_event (
                    id, external_message_id, source_address, destination_address, content
                ) VALUES (?, ?, ?, ?, ?)
                """)) {
                raw.setObject(1, rawId);
                raw.setString(2, message.id());
                raw.setString(3, message.sourceAddress());
                raw.setString(4, message.destinationAddress());
                raw.setString(5, message.content());
                raw.executeUpdate();
            }
            try (PreparedStatement outbox = connection.prepareStatement(
                "INSERT INTO outbox_event (id, aggregate_id, event_type) VALUES (?, ?, ?)"
            )) {
                outbox.setObject(1, UUID.randomUUID());
                outbox.setObject(2, rawId);
                outbox.setString(3, "SMS_RECEIVED");
                outbox.executeUpdate();
            }
        }

        private static void commit(Connection connection) {
            try {
                connection.commit();
            } catch (Exception exception) {
                throw new IllegalStateException("Controlled commit failed", exception);
            }
        }

        @Override
        public void close() throws Exception {
            allowAcknowledgement.countDown();
            server.stop(0);
            requests.shutdownNow();
            assertThat(requests.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static Connection connect(JdbcEndpoint endpoint) throws Exception {
        return DriverManager.getConnection(
            endpoint.url(),
            endpoint.username(),
            endpoint.password().reveal()
        );
    }

    private static byte[] exchangeHttp(URI endpoint, TestSms message) {
        byte[] body = callbackBody(message);
        byte[] request = ("POST " + endpoint.getRawPath() + " HTTP/1.1\r\n"
            + "Host: " + endpoint.getHost() + ":" + endpoint.getPort() + "\r\n"
            + "Content-Type: application/x-www-form-urlencoded\r\n"
            + "Content-Length: " + body.length + "\r\n"
            + "Connection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(endpoint.getHost(), endpoint.getPort()));
            socket.setSoTimeout((int) TIMEOUT.toMillis());
            socket.getOutputStream().write(request);
            socket.getOutputStream().write(body);
            socket.getOutputStream().flush();
            return socket.getInputStream().readAllBytes();
        } catch (IOException exception) {
            throw new IllegalStateException("Controlled HTTP callback failed", exception);
        }
    }

    private static boolean isPositiveHttpResponse(byte[] response) {
        return new String(response, StandardCharsets.US_ASCII).startsWith("HTTP/1.1 200");
    }

    private static int countOccurrences(byte[] source, byte[] expected) {
        int count = 0;
        for (int offset = 0; offset <= source.length - expected.length; offset++) {
            boolean matches = true;
            for (int index = 0; index < expected.length; index++) {
                if (source[offset + index] != expected[index]) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                count++;
            }
        }
        return count;
    }

    private static byte[] callbackBody(TestSms message) {
        byte[] content = message.content().getBytes(StandardCharsets.UTF_16BE);
        String body = "id=" + percentEncode(message.id().getBytes(StandardCharsets.UTF_8))
            + "&from=" + percentEncode(message.sourceAddress().getBytes(StandardCharsets.UTF_8))
            + "&to=" + percentEncode(message.destinationAddress().getBytes(StandardCharsets.UTF_8))
            + "&origin-connector=controlled-aml-peer"
            + "&content=" + percentEncode(content)
            + "&binary=" + HexFormat.of().formatHex(content)
            + "&coding=8";
        return body.getBytes(StandardCharsets.US_ASCII);
    }

    private static TestSms callbackMessage(byte[] body) {
        String encoded = new String(body, StandardCharsets.US_ASCII);
        String id = formValue(encoded, "id");
        String source = formValue(encoded, "from");
        String destination = formValue(encoded, "to");
        byte[] content = HexFormat.of().parseHex(formValue(encoded, "binary"));
        return new TestSms(
            id,
            source,
            destination,
            new String(content, StandardCharsets.UTF_16BE)
        );
    }

    private static String formValue(String body, String name) {
        return List.of(body.split("&")).stream()
            .map(field -> field.split("=", 2))
            .filter(field -> field.length == 2 && field[0].equals(name))
            .map(field -> new String(percentDecode(field[1]), StandardCharsets.UTF_8))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Missing callback field " + name));
    }

    private static String percentEncode(byte[] bytes) {
        StringBuilder encoded = new StringBuilder(bytes.length * 3);
        for (byte value : bytes) {
            int unsigned = Byte.toUnsignedInt(value);
            if ((unsigned >= 'a' && unsigned <= 'z')
                || (unsigned >= 'A' && unsigned <= 'Z')
                || (unsigned >= '0' && unsigned <= '9')
                || unsigned == '-' || unsigned == '_' || unsigned == '.') {
                encoded.append((char) unsigned);
            } else {
                encoded.append('%');
                encoded.append(Character.toUpperCase(Character.forDigit(unsigned >>> 4, 16)));
                encoded.append(Character.toUpperCase(Character.forDigit(unsigned & 0xf, 16)));
            }
        }
        return encoded.toString();
    }

    private static byte[] percentDecode(String value) {
        ByteArrayOutputStream decoded = new ByteArrayOutputStream();
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (current == '%') {
                int high = Character.digit(value.charAt(++index), 16);
                int low = Character.digit(value.charAt(++index), 16);
                decoded.write((high << 4) | low);
            } else {
                decoded.write((byte) current);
            }
        }
        return decoded.toByteArray();
    }

    private static byte[] smppBindRequest(long sequence, String systemId, String password) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        cOctet(body, systemId);
        cOctet(body, password);
        cOctet(body, "");
        body.write(0x34);
        body.write(0);
        body.write(0);
        cOctet(body, "");
        return smppPdu(0x00000009L, 0, sequence, body.toByteArray());
    }

    private static byte[] smppBindResponse(long sequence) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        cOctet(body, "controlled-smsc");
        return smppPdu(0x80000009L, 0, sequence, body.toByteArray());
    }

    private static byte[] smppDeliver(long sequence, TestSms message) {
        byte[] content = message.content().getBytes(StandardCharsets.UTF_16BE);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        cOctet(body, "");
        body.write(0);
        body.write(0);
        cOctet(body, message.sourceAddress());
        body.write(0);
        body.write(0);
        cOctet(body, message.destinationAddress());
        body.write(0);
        body.write(0);
        body.write(0);
        cOctet(body, "");
        cOctet(body, "");
        body.write(0);
        body.write(0);
        body.write(8);
        body.write(0);
        body.write(content.length);
        body.writeBytes(content);
        return smppPdu(0x00000005L, 0, sequence, body.toByteArray());
    }

    private static byte[] smppDeliverResponse(long sequence) {
        return smppPdu(0x80000005L, 0, sequence, new byte[] {0});
    }

    private static byte[] smppPdu(
        long commandId,
        long status,
        long sequence,
        byte[] body
    ) {
        return ByteBuffer.allocate(16 + body.length)
            .order(ByteOrder.BIG_ENDIAN)
            .putInt(16 + body.length)
            .putInt((int) commandId)
            .putInt((int) status)
            .putInt((int) sequence)
            .put(body)
            .array();
    }

    private static void cOctet(OutputStream target, String value) {
        try {
            target.write(value.getBytes(StandardCharsets.US_ASCII));
            target.write(0);
        } catch (IOException exception) {
            throw new IllegalStateException("Cannot encode an SMPP C-octet string", exception);
        }
    }

    private static Optional<byte[]> readPdu(InputStream source) throws IOException {
        byte[] header = source.readNBytes(16);
        if (header.length == 0) {
            return Optional.empty();
        }
        if (header.length != 16) {
            throw new IOException("Truncated SMPP header");
        }
        int length = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN).getInt();
        if (length < 16 || length > SMPP_LIMITS.maximumFrameBytes()) {
            throw new IOException("Invalid SMPP command length");
        }
        byte[] body = source.readNBytes(length - 16);
        if (body.length != length - 16) {
            throw new IOException("Truncated SMPP body");
        }
        ByteArrayOutputStream pdu = new ByteArrayOutputStream(length);
        pdu.writeBytes(header);
        pdu.writeBytes(body);
        return Optional.of(pdu.toByteArray());
    }

    private static void assertAwait(CountDownLatch latch, String description)
        throws InterruptedException {
        if (!latch.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting for " + description);
        }
    }

    private static URI replaceUri(URI value, String host, int port) {
        try {
            return new URI(
                value.getScheme(),
                value.getUserInfo(),
                host,
                port,
                value.getPath(),
                value.getQuery(),
                value.getFragment()
            );
        } catch (Exception exception) {
            throw new IllegalArgumentException("Cannot replace the HTTP address", exception);
        }
    }

    private static InetSocketAddress postgresqlAddress(String url) {
        URI uri = URI.create(url.substring("jdbc:".length()));
        return new InetSocketAddress(uri.getHost(), uri.getPort());
    }

    private static JdbcEndpoint replaceJdbcAddress(
        JdbcEndpoint value,
        String host,
        int port
    ) {
        URI uri = URI.create(value.url().substring("jdbc:".length()));
        try {
            URI replaced = new URI(
                uri.getScheme(),
                uri.getUserInfo(),
                host,
                port,
                uri.getPath(),
                uri.getQuery(),
                uri.getFragment()
            );
            return new JdbcEndpoint(
                "jdbc:" + replaced,
                value.username(),
                value.password()
            );
        } catch (Exception exception) {
            throw new IllegalArgumentException("Cannot replace the PostgreSQL address", exception);
        }
    }
}
