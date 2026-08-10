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
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.smpp.SmppEvidence;
import io.github.jacekkardys.systemproof.smpp.SmppEvidence.DeliverSmResponseCompleted;
import io.github.jacekkardys.systemproof.smpp.SmppExchangeRef;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/** AML-owned direct T1 proof and explicitly separate causal characterization plans. */
public final class AmlT1ProofPoint {
    public static final Duration TIMEOUT = Duration.ofSeconds(45);
    private static final Duration PLAN_DEADLINE = Duration.ofSeconds(90);
    private static final String T1_DIRECT_GUARD =
        "t1-direct-commit-before-smpp-guard";
    private static final String T1_DIRECT_RELATION =
        "t1-direct-commit-before-smpp-relation";
    private static final String ARCH_HTTP_TO_SMPP_GUARD =
        "arch-http-before-smpp-guard";
    private static final String ARCH_HTTP_TO_SMPP_RELATION =
        "arch-http-before-smpp-relation";
    private static final String NEGATIVE_COMMIT_TO_HTTP_GUARD =
        "negative-commit-before-http-guard";
    private static final String NEGATIVE_COMMIT_TO_HTTP_RELATION =
        "negative-commit-before-http-relation";
    private static final PostgresqlDurabilityRequirements DURABILITY_REQUIREMENTS =
        new PostgresqlDurabilityRequirements(Set.of(
            new Table("public", "raw_sms_event"),
            new Table("public", "outbox_event")
        ));

    private final ProofContext context;
    private final SemanticHold commitHold;
    private final SemanticPredecessorGuard directGuard;
    private final ProofPlan plan;

    private AmlT1ProofPoint(
        ProofContext context,
        SemanticHold commitHold,
        SemanticPredecessorGuard directGuard,
        ProofPlan plan
    ) {
        this.context = context;
        this.commitHold = commitHold;
        this.directGuard = directGuard;
        this.plan = plan;
    }

    /** Declares the authoritative direct CommitSucceeded -> positive SMPP relation. */
    public static AmlT1ProofPoint prepare(AmlT1Environment environment, TestSms message) {
        ProofContext context = ProofContext.create(environment, message);
        SemanticHold commitHold = environment.controls().declareHold(
            context.commitAttempt(),
            TIMEOUT
        );
        SemanticPredecessorGuard directGuard = environment.controls().declareGuard(
            SemanticPredecessorGuardSpec.requiring(
                context.subject,
                SemanticPredecessorRequirement.confirmed(context.commitSucceeded()),
                context.positiveSmppResponse(),
                TIMEOUT
            )
        );
        ProofPlan plan = context.directPlan(
            "aml-t1-direct-commit-before-positive-smpp",
            "Direct AML T1 proof"
        ).control(
            "t1-direct-commit-hold-forwarded",
            commitHold,
            SemanticHoldState.FORWARDED
        ).evidence(
            "t1-direct-commit-held-evidence",
            commitHold
        ).control(
            T1_DIRECT_GUARD,
            directGuard,
            SemanticPredecessorGuardState.SATISFIED
        ).evidence(
            "t1-direct-commit-evidence",
            directGuard,
            ProofEvidenceKind.PREDECESSOR_INTERACTION
        ).evidence(
            "t1-direct-smpp-evidence",
            directGuard,
            ProofEvidenceKind.SUCCESSOR_INTERACTION
        ).causalRelation(
            T1_DIRECT_RELATION,
            directGuard
        ).build();
        return new AmlT1ProofPoint(context, commitHold, directGuard, plan);
    }

    /** Declares a non-verdict experiment that captures both sides at a held commit. */
    public static CommitHoldExperiment prepareCommitHoldExperiment(
        AmlT1Environment environment,
        TestSms message
    ) {
        ProofContext context = ProofContext.create(environment, message);
        SemanticHold commitHold = environment.controls().declareHold(
            context.commitAttempt(),
            TIMEOUT
        );
        ProofPlan plan = context.directPlan(
            "aml-t1-held-commit-characterization",
            "AML T1 held-commit characterization"
        ).control(
            "characterization-commit-hold-forwarded",
            commitHold,
            SemanticHoldState.FORWARDED
        ).evidence(
            "characterization-commit-held-evidence",
            commitHold
        ).build();
        return new CommitHoldExperiment(context, commitHold, plan);
    }

    /** Declares a real-Jasmin experiment with the HTTP positive response held at forwarding. */
    public static HttpHoldCharacterization prepareHttpHoldCharacterization(
        AmlT1Environment environment,
        TestSms message
    ) {
        ProofContext context = ProofContext.create(environment, message);
        SemanticHold httpHold = environment.controls().declareHold(
            context.positiveHttpResponse(),
            TIMEOUT
        );
        ProofPlan plan = context.httpSmppPlan(
            "jasmin-http-held-smpp-characterization",
            "Jasmin HTTP-held SMPP characterization"
        ).control(
            "characterization-http-response-hold-forwarded",
            httpHold,
            SemanticHoldState.FORWARDED
        ).evidence(
            "characterization-http-response-held-evidence",
            httpHold
        ).build();
        return new HttpHoldCharacterization(context, httpHold, plan);
    }

    /** Declares the HTTP-forwarded -> positive SMPP architectural hypothesis only. */
    public static ArchitecturalHypothesis prepareHttpToSmppHypothesis(
        AmlT1Environment environment,
        TestSms message
    ) {
        ProofContext context = ProofContext.create(environment, message);
        SemanticPredecessorGuard guard = environment.controls().declareGuard(
            SemanticPredecessorGuardSpec.requiring(
                context.subject,
                SemanticPredecessorRequirement.forwarded(context.positiveHttpResponse()),
                context.positiveSmppResponse(),
                TIMEOUT
            )
        );
        ProofPlan plan = context.httpSmppPlan(
            "aml-architecture-http-before-smpp",
            "AML HTTP before SMPP architectural hypothesis"
        ).control(
            ARCH_HTTP_TO_SMPP_GUARD,
            guard,
            SemanticPredecessorGuardState.SATISFIED
        ).evidence(
            "arch-http-before-smpp-predecessor",
            guard,
            ProofEvidenceKind.PREDECESSOR_INTERACTION
        ).evidence(
            "arch-http-before-smpp-successor",
            guard,
            ProofEvidenceKind.SUCCESSOR_INTERACTION
        ).causalRelation(
            ARCH_HTTP_TO_SMPP_RELATION,
            guard
        ).build();
        return new ArchitecturalHypothesis(context, guard, plan);
    }

    /** Declares only the CommitSucceeded -> positive HTTP negative-control relation. */
    public static EarlyHttpNegative prepareEarlyHttpNegative(
        AmlT1Environment environment,
        TestSms message
    ) {
        ProofContext context = ProofContext.create(environment, message);
        SemanticPredecessorGuard guard = environment.controls().declareGuard(
            SemanticPredecessorGuardSpec.requiring(
                context.subject,
                SemanticPredecessorRequirement.confirmed(context.commitSucceeded()),
                context.positiveHttpResponse(),
                TIMEOUT
            )
        );
        ProofPlan plan = context.commitHttpPlan(
            "aml-negative-commit-before-http",
            "AML deliberately early HTTP acknowledgement"
        ).control(
            NEGATIVE_COMMIT_TO_HTTP_GUARD,
            guard,
            SemanticPredecessorGuardState.SATISFIED
        ).evidence(
            "negative-commit-before-http-predecessor",
            guard,
            ProofEvidenceKind.PREDECESSOR_INTERACTION
        ).evidence(
            "negative-commit-before-http-successor",
            guard,
            ProofEvidenceKind.SUCCESSOR_INTERACTION
        ).causalRelation(
            NEGATIVE_COMMIT_TO_HTTP_RELATION,
            guard
        ).build();
        return new EarlyHttpNegative(context, guard, plan);
    }

    public ProofSubjectRef subject() {
        return context.subject;
    }

    public CorrelationKey correlationKey() {
        return context.correlationKey;
    }

    public PostgresqlDurabilityResult durability() {
        return context.durability();
    }

    public SemanticHold commitHold() {
        return commitHold;
    }

    public SemanticPredecessorGuard directGuard() {
        return directGuard;
    }

    public ProofPlan plan() {
        return plan;
    }

    public ProofResult awaitDirectViolation(ProofExecution execution) throws Exception {
        assertThat(directGuard.completion().toCompletableFuture().get(
            TIMEOUT.toSeconds(),
            TimeUnit.SECONDS
        )).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
        return execution.result();
    }

    /** Verifies that the terminal result is the exact direct T1 counterexample. */
    public void assertDirectViolation(ProofExecution execution, ProofResult result) {
        result.require(ProofOutcome.VIOLATED);
        assertThat(result.primarySubject()).isEqualTo(context.subject);
        assertThat(directGuard.state()).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
        context.assertViolation(
            directGuard,
            result,
            T1_DIRECT_GUARD,
            T1_DIRECT_RELATION,
            context.environment.smppConnectionId()
        );
        assertThat(context.environment.database().snapshot(context.message)).satisfies(value -> {
            assertThat(value.rawCount()).isZero();
            assertThat(value.outboxCount()).isZero();
        });
        context.assertMatchingPositiveSmpp();
        assertThat(execution.result()).isSameAs(result);
        assertThat(execution.evaluate()).isSameAs(result);
        context.assertSecretSafe(result);
    }

    /** Controlled state witness; it deliberately contains no causal verdict obligation. */
    public static final class CommitHoldExperiment {
        private final ProofContext context;
        private final SemanticHold commitHold;
        private final ProofPlan plan;

        private CommitHoldExperiment(
            ProofContext context,
            SemanticHold commitHold,
            ProofPlan plan
        ) {
            this.context = context;
            this.commitHold = commitHold;
            this.plan = plan;
        }

        public ProofPlan plan() {
            return plan;
        }

        public CounterexampleState awaitCounterexampleState() throws Exception {
            commitHold.reached().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            );
            assertThat(commitHold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
            DirectAttribution attribution = context.awaitDirectAttribution();
            assertThat(context.postgresqlEvidence())
                .filteredOn(value -> value instanceof CommitAttempt attempt
                    && attempt.transaction().equals(attribution.transaction()))
                .hasSize(1);
            assertThat(context.postgresqlEvidence())
                .noneMatch(value -> value instanceof CommitSucceeded succeeded
                    && succeeded.transaction().equals(attribution.transaction()));
            SmsPersistence invisible = context.environment.database().snapshot(context.message);
            assertThat(invisible.rawCount()).isZero();
            assertThat(invisible.outboxCount()).isZero();
            context.assertMatchingPositiveSmpp(attribution.smpp());
            return new CounterexampleState(attribution, invisible);
        }

        public SmsPersistence releaseAndAwaitDurability(CounterexampleState state)
            throws Exception {
            commitHold.release().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            );
            assertThat(commitHold.state()).isEqualTo(SemanticHoldState.FORWARDED);
            context.awaitCommitSucceeded(state.attribution.transaction());
            return context.awaitVisiblePersistence();
        }

        public void assertCoverage(ProofExecution execution, ProofResult result) {
            context.assertCoverage(execution, result, plan);
        }
    }

    /** Real HTTP-boundary hold used only to characterize stock Jasmin behavior. */
    public static final class HttpHoldCharacterization {
        private final ProofContext context;
        private final SemanticHold httpHold;
        private final ProofPlan plan;

        private HttpHoldCharacterization(
            ProofContext context,
            SemanticHold httpHold,
            ProofPlan plan
        ) {
            this.context = context;
            this.httpHold = httpHold;
            this.plan = plan;
        }

        public ProofPlan plan() {
            return plan;
        }

        public void awaitHeldHttpWithPositiveSmpp() throws Exception {
            httpHold.reached().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            );
            assertThat(httpHold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
            HttpSmppAttribution attribution = context.awaitHttpSmppAttribution();
            context.assertMatchingPositiveSmpp(attribution.smpp());
            assertThat(context.httpEvidence())
                .filteredOn(value -> value instanceof ResponseCompleted response
                    && response.exchange().equals(attribution.http())
                    && response.acknowledgement()
                        == HttpEvidence.Acknowledgement.POSITIVE)
                .hasSize(1);
        }

        public void release() throws Exception {
            httpHold.release().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            );
            assertThat(httpHold.state()).isEqualTo(SemanticHoldState.FORWARDED);
        }

        public void assertCoverage(ProofExecution execution, ProofResult result) {
            context.assertCoverage(execution, result, plan);
        }
    }

    /** Separate diagnostic verdict for the stronger HTTP -> SMPP hypothesis. */
    public static final class ArchitecturalHypothesis {
        private final ProofContext context;
        private final SemanticPredecessorGuard guard;
        private final ProofPlan plan;

        private ArchitecturalHypothesis(
            ProofContext context,
            SemanticPredecessorGuard guard,
            ProofPlan plan
        ) {
            this.context = context;
            this.guard = guard;
            this.plan = plan;
        }

        public ProofPlan plan() {
            return plan;
        }

        public ProofResult awaitViolation(ProofExecution execution) throws Exception {
            assertThat(guard.completion().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            )).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
            return execution.result();
        }

        public void assertFalsified(ProofExecution execution, ProofResult result) {
            result.require(ProofOutcome.VIOLATED);
            context.assertViolation(
                guard,
                result,
                ARCH_HTTP_TO_SMPP_GUARD,
                ARCH_HTTP_TO_SMPP_RELATION,
                context.environment.smppConnectionId()
            );
            context.assertMatchingPositiveSmpp();
            assertThat(execution.result()).isSameAs(result);
            assertThat(execution.evaluate()).isSameAs(result);
            context.assertSecretSafe(result);
        }
    }

    /** Separate negative-control verdict for a deliberately early application response. */
    public static final class EarlyHttpNegative {
        private final ProofContext context;
        private final SemanticPredecessorGuard guard;
        private final ProofPlan plan;

        private EarlyHttpNegative(
            ProofContext context,
            SemanticPredecessorGuard guard,
            ProofPlan plan
        ) {
            this.context = context;
            this.guard = guard;
            this.plan = plan;
        }

        public ProofPlan plan() {
            return plan;
        }

        public ProofResult awaitViolation(ProofExecution execution) throws Exception {
            assertThat(guard.completion().toCompletableFuture().get(
                TIMEOUT.toSeconds(),
                TimeUnit.SECONDS
            )).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
            return execution.result();
        }

        public void assertCommitBeforeHttpViolation(
            ProofExecution execution,
            ProofResult result
        ) {
            result.require(ProofOutcome.VIOLATED);
            context.assertViolation(
                guard,
                result,
                NEGATIVE_COMMIT_TO_HTTP_GUARD,
                NEGATIVE_COMMIT_TO_HTTP_RELATION,
                context.environment.httpConnectionId()
            );
            assertThat(execution.result()).isSameAs(result);
            assertThat(execution.evaluate()).isSameAs(result);
            context.assertSecretSafe(result);
        }

        public SmsPersistence awaitEventuallyCommitted() {
            return context.awaitVisiblePersistence();
        }
    }

    public record DirectAttribution(
        SmppExchangeRef smpp,
        TransactionRef transaction
    ) {}

    public record HttpSmppAttribution(
        SmppExchangeRef smpp,
        HttpExchangeRef http
    ) {}

    public record CounterexampleState(
        DirectAttribution attribution,
        SmsPersistence invisiblePersistence
    ) {}

    private static final class ProofContext {
        private final AmlT1Environment environment;
        private final TestSms message;
        private final CorrelationKey correlationKey;
        private final ProofSubjectRef subject;
        private final Optional<PostgresqlDurabilityResult> durability;
        private final ProofPrerequisite durabilityPrerequisite;

        private ProofContext(
            AmlT1Environment environment,
            TestSms message,
            CorrelationKey correlationKey,
            ProofSubjectRef subject,
            Optional<PostgresqlDurabilityResult> durability,
            ProofPrerequisite durabilityPrerequisite
        ) {
            this.environment = environment;
            this.message = message;
            this.correlationKey = correlationKey;
            this.subject = subject;
            this.durability = durability;
            this.durabilityPrerequisite = durabilityPrerequisite;
        }

        private static ProofContext create(AmlT1Environment environment, TestSms message) {
            CorrelationKey correlationKey = SmsMessageFingerprint.of(message);
            ProofSubjectRef subject = environment.proofSubjects().create();
            environment.proofSubjects().arm(subject, correlationKey);
            SmsPersistence initial = environment.database().snapshot(message);
            if (initial.rawCount() != 0 || initial.outboxCount() != 0) {
                throw new IllegalStateException(
                    "The exact AML proof subject must have no pre-existing persistence rows"
                );
            }
            DurabilityPrerequisite durability = durability(environment);
            return new ProofContext(
                environment,
                message,
                correlationKey,
                subject,
                durability.result,
                durability.prerequisite
            );
        }

        private ProofPlan.Builder directPlan(String id, String title) {
            return ProofPlan.builder(id, title, subject, PLAN_DEADLINE)
                .prerequisite("postgresql-durability", durabilityPrerequisite)
                .observation(
                    "postgresql-observation",
                    environment.databaseConnectionId(),
                    environment.postgresqlProfile()
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
                    "smpp-exchange-correlation",
                    environment.smppConnectionId(),
                    correlationKey,
                    SmppExchangeRef.codec().schemaId()
                );
        }

        private ProofPlan.Builder httpSmppPlan(String id, String title) {
            return ProofPlan.builder(id, title, subject, PLAN_DEADLINE)
                .observation(
                    "http-observation",
                    environment.httpConnectionId(),
                    environment.httpProfile()
                ).observation(
                    "smpp-observation",
                    environment.smppConnectionId(),
                    environment.smppProfile()
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
                );
        }

        private ProofPlan.Builder commitHttpPlan(String id, String title) {
            return ProofPlan.builder(id, title, subject, PLAN_DEADLINE)
                .prerequisite("postgresql-durability", durabilityPrerequisite)
                .observation(
                    "postgresql-observation",
                    environment.databaseConnectionId(),
                    environment.postgresqlProfile()
                ).observation(
                    "http-observation",
                    environment.httpConnectionId(),
                    environment.httpProfile()
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
                );
        }

        private SemanticInteractionSelector<PostgresqlEvidence> commitAttempt() {
            return SemanticInteractionSelector.matching(
                environment.databaseConnectionId(),
                FlowDirection.CONSUMER_TO_PROVIDER,
                environment.postgresqlAdapter().evidenceCodec(),
                CommitAttempt.class::isInstance
            ).forSubject(subject).through(
                correlationKey,
                TransactionRef.codec(),
                value -> ((CommitAttempt) value).transaction()
            );
        }

        private SemanticInteractionSelector<PostgresqlEvidence> commitSucceeded() {
            return SemanticInteractionSelector.matching(
                environment.databaseConnectionId(),
                FlowDirection.PROVIDER_TO_CONSUMER,
                environment.postgresqlAdapter().evidenceCodec(),
                CommitSucceeded.class::isInstance
            ).forSubject(subject).through(
                correlationKey,
                TransactionRef.codec(),
                value -> ((CommitSucceeded) value).transaction()
            );
        }

        private SemanticInteractionSelector<HttpEvidence> positiveHttpResponse() {
            return SemanticInteractionSelector.matching(
                environment.httpConnectionId(),
                FlowDirection.PROVIDER_TO_CONSUMER,
                environment.httpAdapter().evidenceCodec(),
                value -> value instanceof ResponseCompleted response
                    && response.acknowledgement()
                        == HttpEvidence.Acknowledgement.POSITIVE
            ).forSubject(subject).through(
                correlationKey,
                HttpExchangeRef.codec(),
                value -> ((ResponseCompleted) value).exchange()
            );
        }

        private SemanticInteractionSelector<SmppEvidence> positiveSmppResponse() {
            return SemanticInteractionSelector.matching(
                environment.smppConnectionId(),
                FlowDirection.CONSUMER_TO_PROVIDER,
                environment.smppAdapter().evidenceCodec(),
                value -> value instanceof DeliverSmResponseCompleted response
                    && response.acknowledgement()
                        == SmppEvidence.Acknowledgement.POSITIVE
            ).forSubject(subject).through(
                correlationKey,
                SmppExchangeRef.codec(),
                value -> ((DeliverSmResponseCompleted) value).exchange()
            );
        }

        private PostgresqlDurabilityResult durability() {
            return durability.orElseThrow(() -> new IllegalStateException(
                "PostgreSQL durability verification did not produce a typed result"
            ));
        }

        private DirectAttribution awaitDirectAttribution() {
            Awaitility.await("unique direct AML attribution")
                .atMost(TIMEOUT)
                .until(() -> unique(SmppExchangeRef.codec())
                    && unique(TransactionRef.codec()));
            return new DirectAttribution(
                uniqueCorrelation(SmppExchangeRef.codec()),
                uniqueCorrelation(TransactionRef.codec())
            );
        }

        private HttpSmppAttribution awaitHttpSmppAttribution() {
            Awaitility.await("unique HTTP and SMPP attribution")
                .atMost(TIMEOUT)
                .until(() -> unique(SmppExchangeRef.codec())
                    && unique(HttpExchangeRef.codec()));
            return new HttpSmppAttribution(
                uniqueCorrelation(SmppExchangeRef.codec()),
                uniqueCorrelation(HttpExchangeRef.codec())
            );
        }

        private void assertMatchingPositiveSmpp() {
            Awaitility.await("unique SMPP attribution")
                .atMost(TIMEOUT)
                .until(() -> unique(SmppExchangeRef.codec()));
            assertMatchingPositiveSmpp(uniqueCorrelation(SmppExchangeRef.codec()));
        }

        private void assertMatchingPositiveSmpp(SmppExchangeRef exchange) {
            assertThat(smppEvidence())
                .filteredOn(value -> value instanceof DeliverSmResponseCompleted response
                    && response.exchange().equals(exchange)
                    && response.acknowledgement()
                        == SmppEvidence.Acknowledgement.POSITIVE)
                .hasSize(1);
        }

        private void awaitCommitSucceeded(TransactionRef transaction) {
            Awaitility.await("matching PostgreSQL commit confirmation")
                .atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(postgresqlEvidence())
                    .filteredOn(value -> value instanceof CommitSucceeded succeeded
                        && succeeded.transaction().equals(transaction))
                    .hasSize(1));
        }

        private SmsPersistence awaitVisiblePersistence() {
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

        private void assertViolation(
            SemanticPredecessorGuard guard,
            ProofResult result,
            String guardId,
            String relationId,
            ConnectionId successorConnection
        ) {
            assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
            assertThat(guardEvents(guard).stream()
                .filter(value -> value.kind()
                    == SemanticPredecessorGuardEvent.Kind.TERMINAL)
                .filter(value -> value.state()
                    == SemanticPredecessorGuardState.VIOLATED)
                .toList()).singleElement().satisfies(value -> {
                    assertThat(value.predecessor()).isEmpty();
                    assertThat(value.successor()).isPresent();
                    assertThat(value.decision()).contains(ForwardingDecision.CLOSE_SESSION);
                });
            assertViolatedResolution(result, guardId, successorConnection);
            assertViolatedResolution(result, relationId, successorConnection);
        }

        private static void assertViolatedResolution(
            ProofResult result,
            String id,
            ConnectionId successorConnection
        ) {
            ProofObligationResolution resolution = resolution(result, id);
            assertThat(resolution.resolution()).isEqualTo(ProofResolution.VIOLATED);
            assertThat(resolution.reason())
                .isEqualTo(ProofResolutionReason.CAUSAL_RELATION_VIOLATED);
            assertThat(resolution.connectionId()).contains(successorConnection);
            assertThat(resolution.provenance()).singleElement().satisfies(value ->
                assertProvenance(value, Role.SUCCESSOR, successorConnection)
            );
        }

        private void assertCoverage(
            ProofExecution execution,
            ProofResult result,
            ProofPlan plan
        ) {
            result.require(ProofOutcome.PROVED);
            assertThat(result.primarySubject()).isEqualTo(subject);
            assertThat(result.resolutions()).hasSameSizeAs(plan.requirements())
                .allMatch(value -> value.resolution() == ProofResolution.SATISFIED);
            assertThat(execution.result()).isSameAs(result);
            assertThat(execution.evaluate()).isSameAs(result);
            assertSecretSafe(result);
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
                .map(value -> value.event())
                .filter(InteractionObservationEvent.class::isInstance)
                .map(InteractionObservationEvent.class::cast)
                .filter(value -> value.evidence().schemaId().equals(codec.schemaId()))
                .map(value -> value.evidence().decode(codec))
                .toList();
        }

        private List<SemanticPredecessorGuardEvent> guardEvents(
            SemanticPredecessorGuard guard
        ) {
            return environment.journalSnapshot().entries().stream()
                .map(value -> value.event())
                .filter(SemanticPredecessorGuardEvent.class::isInstance)
                .map(SemanticPredecessorGuardEvent.class::cast)
                .filter(value -> value.guardRef().equals(guard.ref()))
                .toList();
        }

        private void assertSecretSafe(ProofResult result) {
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
    }

    private static void assertProvenance(
        ProofInteractionProvenance provenance,
        Role role,
        ConnectionId connectionId
    ) {
        assertThat(provenance.role()).isEqualTo(role);
        assertThat(provenance.interaction().connectionId()).isEqualTo(connectionId);
    }

    private static ProofObligationResolution resolution(ProofResult result, String id) {
        return result.resolutions().stream()
            .filter(value -> value.id().value().equals(id))
            .findFirst()
            .orElseThrow();
    }

    private record DurabilityPrerequisite(
        Optional<PostgresqlDurabilityResult> result,
        ProofPrerequisite prerequisite
    ) {}
}
