package io.github.jacekkardys.systemproof.examples.sms.environment.proof;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticInteractionSelector;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardSpec;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorRequirement;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.SmsMessageFingerprint;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.SmsPersistence;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.TestSms;
import io.github.jacekkardys.systemproof.http.HttpEvidence;
import io.github.jacekkardys.systemproof.http.HttpEvidence.ResponseCompleted;
import io.github.jacekkardys.systemproof.http.HttpExchangeRef;
import io.github.jacekkardys.systemproof.journal.InteractionObservationEvent;
import io.github.jacekkardys.systemproof.journal.SemanticPredecessorGuardEvent;
import io.github.jacekkardys.systemproof.observation.EvidenceCodec;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlDurabilityRequirements;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlDurabilityRequirements.Table;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlDurabilityResult;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlEvidence;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlEvidence.CommitAttempt;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlEvidence.CommitSucceeded;
import io.github.jacekkardys.systemproof.postgresql.TransactionRef;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.CorrelationResult;
import io.github.jacekkardys.systemproof.proof.ProofEvidenceKind;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofInteractionProvenance;
import io.github.jacekkardys.systemproof.proof.ProofInteractionProvenance.Role;
import io.github.jacekkardys.systemproof.proof.ProofObligationResolution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofPrerequisite;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.smpp.SmppEvidence;
import io.github.jacekkardys.systemproof.smpp.SmppEvidence.DeliverSmResponseCompleted;
import io.github.jacekkardys.systemproof.smpp.SmppExchangeRef;

/** Typed AML-owned selectors, controls, proof plan, and evidence assertions for one T1 subject. */
public final class AmlT1ProofPoint {
    public static final Duration TIMEOUT = Duration.ofSeconds(45);
    private static final Duration PLAN_DEADLINE = Duration.ofSeconds(90);
    private static final PostgresqlDurabilityRequirements DURABILITY_REQUIREMENTS =
        new PostgresqlDurabilityRequirements(Set.of(
            new Table("public", "raw_sms_event"),
            new Table("public", "outbox_event")
        ));

    private final AmlT1Environment environment;
    private final TestSms message;
    private final CorrelationKey correlationKey;
    private final ProofSubjectRef subject;
    private final Optional<PostgresqlDurabilityResult> durability;
    private final SemanticHold commitHold;
    private final SemanticHold httpResponseHold;
    private final SemanticPredecessorGuard commitToHttpGuard;
    private final SemanticPredecessorGuard httpToSmppGuard;
    private final ProofPlan plan;

    private AmlT1ProofPoint(
        AmlT1Environment environment,
        TestSms message,
        CorrelationKey correlationKey,
        ProofSubjectRef subject,
        Optional<PostgresqlDurabilityResult> durability,
        SemanticHold commitHold,
        SemanticHold httpResponseHold,
        SemanticPredecessorGuard commitToHttpGuard,
        SemanticPredecessorGuard httpToSmppGuard,
        ProofPlan plan
    ) {
        this.environment = environment;
        this.message = message;
        this.correlationKey = correlationKey;
        this.subject = subject;
        this.durability = durability;
        this.commitHold = commitHold;
        this.httpResponseHold = httpResponseHold;
        this.commitToHttpGuard = commitToHttpGuard;
        this.httpToSmppGuard = httpToSmppGuard;
        this.plan = plan;
    }

    /** Declares the complete immutable proof before any proof traffic is initiated. */
    public static AmlT1ProofPoint prepare(AmlT1Environment environment, TestSms message) {
        CorrelationKey correlationKey = SmsMessageFingerprint.of(message);
        ProofSubjectRef subject = environment.proofSubjects().create();
        environment.proofSubjects().arm(subject, correlationKey);

        DurabilityPrerequisite durability = durability(environment);
        SmsPersistence initial = environment.database().snapshot(message);
        if (initial.rawCount() != 0 || initial.outboxCount() != 0) {
            throw new IllegalStateException(
                "The exact AML proof subject must have no pre-existing persistence rows"
            );
        }

        SemanticInteractionSelector<PostgresqlEvidence> commitAttempt =
            SemanticInteractionSelector.matching(
                environment.databaseConnectionId(),
                FlowDirection.CONSUMER_TO_PROVIDER,
                environment.postgresqlAdapter().evidenceCodec(),
                CommitAttempt.class::isInstance
            ).forSubject(subject).through(
                correlationKey,
                TransactionRef.codec(),
                evidence -> ((CommitAttempt) evidence).transaction()
            );
        SemanticInteractionSelector<HttpEvidence> positiveHttpResponse =
            SemanticInteractionSelector.matching(
                environment.httpConnectionId(),
                FlowDirection.PROVIDER_TO_CONSUMER,
                environment.httpAdapter().evidenceCodec(),
                evidence -> evidence instanceof ResponseCompleted completed
                    && completed.acknowledgement() == HttpEvidence.Acknowledgement.POSITIVE
            ).forSubject(subject).through(
                correlationKey,
                HttpExchangeRef.codec(),
                evidence -> ((ResponseCompleted) evidence).exchange()
            );
        SemanticHold commitHold = environment.controls().declareHold(commitAttempt, TIMEOUT);
        SemanticHold httpResponseHold = environment.controls().declareHold(
            positiveHttpResponse,
            TIMEOUT
        );
        SemanticPredecessorGuard commitToHttpGuard = environment.controls().declareGuard(
            SemanticPredecessorGuardSpec.requiring(
                subject,
                SemanticPredecessorRequirement.confirmed(
                    SemanticInteractionSelector.matching(
                        environment.databaseConnectionId(),
                        FlowDirection.PROVIDER_TO_CONSUMER,
                        environment.postgresqlAdapter().evidenceCodec(),
                        CommitSucceeded.class::isInstance
                    ).forSubject(subject).through(
                        correlationKey,
                        TransactionRef.codec(),
                        evidence -> ((CommitSucceeded) evidence).transaction()
                    )
                ),
                positiveHttpResponse,
                TIMEOUT
            )
        );
        SemanticPredecessorGuard httpToSmppGuard = environment.controls().declareGuard(
            SemanticPredecessorGuardSpec.requiring(
                subject,
                SemanticPredecessorRequirement.forwarded(positiveHttpResponse),
                SemanticInteractionSelector.matching(
                    environment.smppConnectionId(),
                    FlowDirection.CONSUMER_TO_PROVIDER,
                    environment.smppAdapter().evidenceCodec(),
                    evidence -> evidence instanceof DeliverSmResponseCompleted completed
                        && completed.acknowledgement()
                            == SmppEvidence.Acknowledgement.POSITIVE
                ).forSubject(subject).through(
                    correlationKey,
                    SmppExchangeRef.codec(),
                    evidence -> ((DeliverSmResponseCompleted) evidence).exchange()
                ),
                TIMEOUT
            )
        );

        ProofPlan plan = ProofPlan.builder(
            "aml-t1-durable-commit-before-positive-ack",
            "Deterministic AML T1 proof",
            subject,
            PLAN_DEADLINE
        ).prerequisite(
            "postgresql-durability",
            durability.prerequisite()
        ).observation(
            "postgresql-observation",
            environment.databaseConnectionId(),
            environment.postgresqlProfile()
        ).observation(
            "http-observation",
            environment.httpConnectionId(),
            environment.httpProfile()
        ).observation(
            "smpp-observation",
            environment.smppConnectionId(),
            environment.smppProfile()
        ).correlation(
            "postgresql-transaction-correlation",
            environment.databaseConnectionId(),
            correlationKey,
            TransactionRef.codec().schemaId()
        ).correlation(
            "http-exchange-correlation",
            environment.httpConnectionId(),
            correlationKey,
            HttpExchangeRef.codec().schemaId()
        ).correlation(
            "smpp-exchange-correlation",
            environment.smppConnectionId(),
            correlationKey,
            SmppExchangeRef.codec().schemaId()
        ).control(
            "commit-hold-forwarded",
            commitHold,
            SemanticHoldState.FORWARDED
        ).evidence(
            "commit-held-evidence",
            commitHold
        ).control(
            "http-response-hold-forwarded",
            httpResponseHold,
            SemanticHoldState.FORWARDED
        ).evidence(
            "http-response-held-evidence",
            httpResponseHold
        ).control(
            "commit-before-http-guard",
            commitToHttpGuard,
            SemanticPredecessorGuardState.SATISFIED
        ).evidence(
            "commit-before-http-predecessor",
            commitToHttpGuard,
            ProofEvidenceKind.PREDECESSOR_INTERACTION
        ).evidence(
            "commit-before-http-successor",
            commitToHttpGuard,
            ProofEvidenceKind.SUCCESSOR_INTERACTION
        ).causalRelation(
            "commit-before-http-relation",
            commitToHttpGuard
        ).control(
            "http-before-smpp-guard",
            httpToSmppGuard,
            SemanticPredecessorGuardState.SATISFIED
        ).evidence(
            "http-before-smpp-predecessor",
            httpToSmppGuard,
            ProofEvidenceKind.PREDECESSOR_INTERACTION
        ).evidence(
            "http-before-smpp-successor",
            httpToSmppGuard,
            ProofEvidenceKind.SUCCESSOR_INTERACTION
        ).causalRelation(
            "http-before-smpp-relation",
            httpToSmppGuard
        ).build();
        return new AmlT1ProofPoint(
            environment,
            message,
            correlationKey,
            subject,
            durability.result(),
            commitHold,
            httpResponseHold,
            commitToHttpGuard,
            httpToSmppGuard,
            plan
        );
    }

    public TestSms message() {
        return message;
    }

    public CorrelationKey correlationKey() {
        return correlationKey;
    }

    public ProofSubjectRef subject() {
        return subject;
    }

    public PostgresqlDurabilityResult durability() {
        return durability.orElseThrow(() -> new IllegalStateException(
            "PostgreSQL durability verification did not produce a typed result"
        ));
    }

    public SemanticHold commitHold() {
        return commitHold;
    }

    public SemanticHold httpResponseHold() {
        return httpResponseHold;
    }

    public SemanticPredecessorGuard commitToHttpGuard() {
        return commitToHttpGuard;
    }

    public SemanticPredecessorGuard httpToSmppGuard() {
        return httpToSmppGuard;
    }

    public ProofPlan plan() {
        return plan;
    }

    public NativeAttribution awaitCommitHeldAndAssertInvisible() throws Exception {
        NativeAttribution attribution = awaitCommitHeld();
        assertThat(commitToHttpGuard.state()).isNotEqualTo(
            SemanticPredecessorGuardState.VIOLATED
        );
        assertThat(httpToSmppGuard.state()).isNotEqualTo(
            SemanticPredecessorGuardState.VIOLATED
        );
        assertNoPositiveHttpOrSmpp(attribution);
        return attribution;
    }

    /** Awaits the real held commit for the explicit early-ack counterexample. */
    public NativeAttribution awaitCounterexampleCommitHeld() throws Exception {
        return awaitCommitHeld();
    }

    private NativeAttribution awaitCommitHeld() throws Exception {
        commitHold.reached().toCompletableFuture().get(
            TIMEOUT.toSeconds(),
            TimeUnit.SECONDS
        );
        assertThat(commitHold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        NativeAttribution attribution = awaitUniqueAttribution();
        assertThat(postgresqlEvidence())
            .filteredOn(evidence -> evidence instanceof CommitAttempt attempt
                && attempt.transaction().equals(attribution.transaction()))
            .hasSize(1);
        assertThat(postgresqlEvidence())
            .noneMatch(evidence -> evidence instanceof CommitSucceeded succeeded
                && succeeded.transaction().equals(attribution.transaction()));
        assertThat(environment.database().snapshot(message)).satisfies(persistence -> {
            assertThat(persistence.rawCount()).isZero();
            assertThat(persistence.outboxCount()).isZero();
        });
        return attribution;
    }

    public SmsPersistence releaseCommitAndAwaitDurability(NativeAttribution attribution)
        throws Exception {
        commitHold.release().toCompletableFuture().get(
            TIMEOUT.toSeconds(),
            TimeUnit.SECONDS
        );
        assertThat(commitHold.state()).isEqualTo(SemanticHoldState.FORWARDED);
        Awaitility.await("matching PostgreSQL commit confirmation")
            .atMost(TIMEOUT)
            .untilAsserted(() -> assertThat(postgresqlEvidence())
                .filteredOn(evidence -> evidence instanceof CommitSucceeded succeeded
                    && succeeded.transaction().equals(attribution.transaction()))
                .hasSize(1));
        SmsPersistence persistence = environment.database().await()
            .rawAndOutboxVisible(message);
        assertThat(persistence.rawCount()).isOne();
        assertThat(persistence.outboxCount()).isOne();
        assertThat(persistence.rawId()).isEqualTo(persistence.outboxAggregateId());
        assertThat(persistence.externalMessageId()).isNotBlank();
        assertThat(persistence.sourceAddress()).isEqualTo(message.sourceAddress());
        assertThat(persistence.destinationAddress()).isEqualTo(message.destinationAddress());
        assertThat(persistence.content()).isEqualTo(message.content());
        return persistence;
    }

    public void awaitHttpHeldAndAssertNoEarlySmpp(NativeAttribution attribution)
        throws Exception {
        httpResponseHold.reached().toCompletableFuture().get(
            TIMEOUT.toSeconds(),
            TimeUnit.SECONDS
        );
        assertThat(httpResponseHold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertThat(commitToHttpGuard.state()).isEqualTo(
            SemanticPredecessorGuardState.SUCCESSOR_AUTHORIZED
        );
        assertThat(httpToSmppGuard.state()).isEqualTo(
            SemanticPredecessorGuardState.PREDECESSOR_OBSERVED
        );
        assertThat(smppEvidence())
            .noneMatch(evidence -> evidence instanceof DeliverSmResponseCompleted response
                && response.exchange().equals(attribution.smpp())
                && response.acknowledgement() == SmppEvidence.Acknowledgement.POSITIVE);
    }

    public void releaseHttpAndAwaitRelations() throws Exception {
        httpResponseHold.release().toCompletableFuture().get(
            TIMEOUT.toSeconds(),
            TimeUnit.SECONDS
        );
        assertThat(httpResponseHold.state()).isEqualTo(SemanticHoldState.FORWARDED);
        assertThat(commitToHttpGuard.completion().toCompletableFuture().get(
            TIMEOUT.toSeconds(),
            TimeUnit.SECONDS
        )).isEqualTo(SemanticPredecessorGuardState.SATISFIED);
        assertThat(httpToSmppGuard.completion().toCompletableFuture().get(
            TIMEOUT.toSeconds(),
            TimeUnit.SECONDS
        )).isEqualTo(SemanticPredecessorGuardState.SATISFIED);
    }

    public void awaitEarlyHttpViolation() throws Exception {
        assertThat(commitToHttpGuard.completion().toCompletableFuture().get(
            TIMEOUT.toSeconds(),
            TimeUnit.SECONDS
        )).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
        assertThat(guardEvents(commitToHttpGuard).stream()
            .filter(event -> event.kind() == SemanticPredecessorGuardEvent.Kind.VIOLATION)
            .toList()).singleElement().satisfies(event -> {
                assertThat(event.predecessor()).isEmpty();
                assertThat(event.successor()).isPresent();
                assertThat(event.decision()).contains(ForwardingDecision.CLOSE_SESSION);
            });
    }

    public void assertProved(ProofExecution execution, io.github.jacekkardys.systemproof.proof.ProofResult result) {
        result.require(ProofOutcome.PROVED);
        assertThat(result.primarySubject()).isEqualTo(subject);
        assertThat(result.resolutions()).hasSameSizeAs(plan.requirements())
            .allMatch(resolution -> resolution.resolution() == ProofResolution.SATISFIED);
        assertThat(result.resolutions().stream().map(value -> value.id().value()).toList())
            .containsExactlyElementsOf(plan.requirements().stream()
                .map(value -> value.id().value())
                .toList());
        assertCorrelation(result, "postgresql-transaction-correlation", environment.databaseConnectionId());
        assertCorrelation(result, "http-exchange-correlation", environment.httpConnectionId());
        assertCorrelation(result, "smpp-exchange-correlation", environment.smppConnectionId());
        assertRole(result, "commit-held-evidence", Role.HOLD, environment.databaseConnectionId());
        assertRole(result, "http-response-held-evidence", Role.HOLD, environment.httpConnectionId());
        assertRole(result, "commit-before-http-predecessor", Role.PREDECESSOR, environment.databaseConnectionId());
        assertRole(result, "commit-before-http-successor", Role.SUCCESSOR, environment.httpConnectionId());
        assertEstablishedRelation(result, "commit-before-http-relation", environment.databaseConnectionId(), environment.httpConnectionId());
        assertRole(result, "http-before-smpp-predecessor", Role.PREDECESSOR, environment.httpConnectionId());
        assertRole(result, "http-before-smpp-successor", Role.SUCCESSOR, environment.smppConnectionId());
        assertEstablishedRelation(result, "http-before-smpp-relation", environment.httpConnectionId(), environment.smppConnectionId());
        assertThat(execution.result()).isSameAs(result);
        assertThat(execution.evaluate()).isSameAs(result);
        assertThat(result.primaryFailure()).isEmpty();
        assertThat(result.secondaryDiagnostics()).isEmpty();
        assertSecretSafe(result);
    }

    public void assertViolated(ProofExecution execution, io.github.jacekkardys.systemproof.proof.ProofResult result) {
        result.require(ProofOutcome.VIOLATED);
        assertThat(result.primarySubject()).isEqualTo(subject);
        assertThat(commitToHttpGuard.state()).isEqualTo(
            SemanticPredecessorGuardState.VIOLATED
        );
        assertViolatedRelation(result, "commit-before-http-guard");
        assertViolatedRelation(result, "commit-before-http-relation");
        assertThat(result.primaryFailure()).isEmpty();
        assertThat(execution.result()).isSameAs(result);
        assertThat(execution.evaluate()).isSameAs(result);
        assertNoPositiveSmpp(awaitUniqueAttribution());
        assertSecretSafe(result);
    }

    private NativeAttribution awaitUniqueAttribution() {
        Awaitility.await("unique three-protocol AML attribution")
            .atMost(TIMEOUT)
            .until(() -> unique(SmppExchangeRef.codec())
                && unique(HttpExchangeRef.codec())
                && unique(TransactionRef.codec()));
        return new NativeAttribution(
            uniqueCorrelation(SmppExchangeRef.codec()),
            uniqueCorrelation(HttpExchangeRef.codec()),
            uniqueCorrelation(TransactionRef.codec())
        );
    }

    private <T> boolean unique(EvidenceCodec<T> codec) {
        return environment.proofSubjects().correlation(subject, correlationKey, codec)
            instanceof CorrelationResult.Unique<?>;
    }

    private <T> T uniqueCorrelation(EvidenceCodec<T> codec) {
        CorrelationResult<T> result = environment.proofSubjects().correlation(
            subject,
            correlationKey,
            codec
        );
        assertThat(result).isInstanceOf(CorrelationResult.Unique.class);
        return ((CorrelationResult.Unique<T>) result).nativeReference();
    }

    private void assertNoPositiveHttpOrSmpp(NativeAttribution attribution) {
        assertThat(httpEvidence())
            .noneMatch(evidence -> evidence instanceof ResponseCompleted response
                && response.exchange().equals(attribution.http())
                && response.acknowledgement() == HttpEvidence.Acknowledgement.POSITIVE);
        assertNoPositiveSmpp(attribution);
    }

    private void assertNoPositiveSmpp(NativeAttribution attribution) {
        assertThat(smppEvidence())
            .noneMatch(evidence -> evidence instanceof DeliverSmResponseCompleted response
                && response.exchange().equals(attribution.smpp())
                && response.acknowledgement() == SmppEvidence.Acknowledgement.POSITIVE);
    }

    private List<PostgresqlEvidence> postgresqlEvidence() {
        return evidence(environment.postgresqlAdapter().evidenceCodec());
    }

    private List<HttpEvidence> httpEvidence() {
        return evidence(environment.httpAdapter().evidenceCodec());
    }

    private List<SmppEvidence> smppEvidence() {
        return evidence(environment.smppAdapter().evidenceCodec());
    }

    private <T> List<T> evidence(EvidenceCodec<T> codec) {
        return environment.journalSnapshot().entries().stream()
            .map(entry -> entry.event())
            .filter(InteractionObservationEvent.class::isInstance)
            .map(InteractionObservationEvent.class::cast)
            .filter(event -> event.evidence().schemaId().equals(codec.schemaId()))
            .map(event -> event.evidence().decode(codec))
            .toList();
    }

    private List<SemanticPredecessorGuardEvent> guardEvents(
        SemanticPredecessorGuard guard
    ) {
        return environment.journalSnapshot().entries().stream()
            .map(entry -> entry.event())
            .filter(SemanticPredecessorGuardEvent.class::isInstance)
            .map(SemanticPredecessorGuardEvent.class::cast)
            .filter(event -> event.guardRef().equals(guard.ref()))
            .toList();
    }

    private void assertCorrelation(
        io.github.jacekkardys.systemproof.proof.ProofResult result,
        String id,
        io.github.jacekkardys.systemproof.topology.ConnectionId connectionId
    ) {
        assertRole(result, id, Role.CORRELATION, connectionId);
    }

    private void assertRole(
        io.github.jacekkardys.systemproof.proof.ProofResult result,
        String id,
        Role role,
        io.github.jacekkardys.systemproof.topology.ConnectionId connectionId
    ) {
        assertThat(resolution(result, id).provenance()).singleElement()
            .satisfies(value -> assertProvenance(value, role, connectionId));
    }

    private void assertEstablishedRelation(
        io.github.jacekkardys.systemproof.proof.ProofResult result,
        String id,
        io.github.jacekkardys.systemproof.topology.ConnectionId predecessor,
        io.github.jacekkardys.systemproof.topology.ConnectionId successor
    ) {
        assertThat(resolution(result, id).provenance()).satisfiesExactly(
            value -> assertProvenance(value, Role.PREDECESSOR, predecessor),
            value -> assertProvenance(value, Role.SUCCESSOR, successor)
        );
    }

    private void assertViolatedRelation(
        io.github.jacekkardys.systemproof.proof.ProofResult result,
        String id
    ) {
        ProofObligationResolution resolution = resolution(result, id);
        assertThat(resolution.resolution()).isEqualTo(ProofResolution.VIOLATED);
        assertThat(resolution.connectionId()).contains(environment.httpConnectionId());
        assertThat(resolution.provenance()).singleElement().satisfies(value ->
            assertProvenance(value, Role.SUCCESSOR, environment.httpConnectionId())
        );
    }

    private static void assertProvenance(
        ProofInteractionProvenance provenance,
        Role role,
        io.github.jacekkardys.systemproof.topology.ConnectionId connectionId
    ) {
        assertThat(provenance.role()).isEqualTo(role);
        assertThat(provenance.interaction().connectionId()).isEqualTo(connectionId);
    }

    private static ProofObligationResolution resolution(
        io.github.jacekkardys.systemproof.proof.ProofResult result,
        String id
    ) {
        return result.resolutions().stream()
            .filter(value -> value.id().value().equals(id))
            .findFirst()
            .orElseThrow();
    }

    private void assertSecretSafe(io.github.jacekkardys.systemproof.proof.ProofResult result) {
        assertThat(result.report().content()).doesNotContain(
            message.id(),
            message.sourceAddress(),
            message.destinationAddress(),
            message.content(),
            "ACK/Jasmin"
        );
        assertThat(result.report().content()).doesNotContain(
            environment.credentials().toArray(String[]::new)
        );
    }

    private static DurabilityPrerequisite durability(AmlT1Environment environment) {
        try {
            PostgresqlDurabilityResult result = environment.database()
                .durabilityPreflight(DURABILITY_REQUIREMENTS);
            ProofPrerequisite prerequisite = result.satisfied()
                ? environment.proofs().satisfiedPrerequisite()
                : environment.proofs().unsupportedPrerequisite();
            return new DurabilityPrerequisite(Optional.of(result), prerequisite);
        } catch (RuntimeException failure) {
            return new DurabilityPrerequisite(
                Optional.empty(),
                environment.proofs().failedPrerequisite(failure)
            );
        }
    }

    public record NativeAttribution(
        SmppExchangeRef smpp,
        HttpExchangeRef http,
        TransactionRef transaction
    ) {}

    private record DurabilityPrerequisite(
        Optional<PostgresqlDurabilityResult> result,
        ProofPrerequisite prerequisite
    ) {}
}
