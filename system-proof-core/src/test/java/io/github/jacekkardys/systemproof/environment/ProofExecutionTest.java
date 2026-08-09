package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.observation.EffectiveObservationStatus;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.observation.RecordedInteraction;
import io.github.jacekkardys.systemproof.proof.ProofConfigurationException;
import io.github.jacekkardys.systemproof.proof.ProofEvidenceKind;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofFailureStage;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResult;

class ProofExecutionTest {
    private static final Duration DEADLINE = Duration.ofSeconds(5);

    @Test
    void shouldProveEveryDeclaredGuardCorrelationEvidenceAndCausalObligation()
        throws Exception {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            SemanticPredecessorGuard guard = fixture.declareGuard(DEADLINE);
            ProofPlan plan = completeGuardPlan(fixture, guard);

            ProofExecution execution = fixture.environment.proofs().activate(plan);
            assertThat(execution.state()).isEqualTo(
                io.github.jacekkardys.systemproof.proof.ProofExecutionState.ACTIVE
            );
            execution.runStimulus(() -> {
                forward(fixture, fixture.correlated("predecessor"));
                forward(fixture, fixture.correlated("successor"));
            });

            ProofResult result = execution.evaluate().require(ProofOutcome.PROVED);

            assertThat(result.resolutions())
                .allMatch(value -> value.resolution() == ProofResolution.SATISFIED);
            assertThat(result.report().content()).contains(
                "decisive=all-required-items-satisfied"
            );
            assertThat(execution.evaluate()).isSameAs(result);
            assertThat(execution.result()).isSameAs(result);
        }
    }

    @Test
    void shouldLinearizeExplicitEarlySuccessorAsViolation() throws Exception {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            SemanticPredecessorGuard guard = fixture.declareGuard(DEADLINE);
            ProofExecution execution = fixture.environment.proofs().activate(
                completeGuardPlan(fixture, guard)
            );

            ForwardingPermit rejected = fixture.permit(fixture.correlated("successor"));

            assertThat(rejected.awaitDecision()).isEqualTo(
                ForwardingDecision.CLOSE_SESSION
            );
            ProofResult result = execution.result().require(ProofOutcome.VIOLATED);
            assertThat(result.resolutions()).anyMatch(
                value -> value.resolution() == ProofResolution.VIOLATED
            );
            assertThat(result.primaryFailure()).isEmpty();
        }
    }

    @Test
    void shouldKeepMissingAndAmbiguousCorrelationsInconclusive() {
        try (ProofTestFixture missing = ProofTestFixture.start()) {
            ProofExecution execution = missing.environment.proofs().activate(
                correlationPlan(missing, "missing-correlation")
            );
            execution.runStimulus(() -> {});

            assertThat(execution.evaluate().outcome()).isEqualTo(
                ProofOutcome.INCONCLUSIVE
            );
            assertThat(execution.result().resolutions()).anyMatch(
                value -> value.resolution() == ProofResolution.MISSING
            );
        }

        try (ProofTestFixture ambiguous = ProofTestFixture.start()) {
            ambiguous.addSubjectForSameKey();
            ProofExecution execution = ambiguous.environment.proofs().activate(
                correlationPlan(ambiguous, "ambiguous-correlation")
            );
            execution.runStimulus(() -> ambiguous.correlated("predecessor"));

            assertThat(execution.evaluate().outcome()).isEqualTo(
                ProofOutcome.INCONCLUSIVE
            );
            assertThat(execution.result().resolutions()).anyMatch(
                value -> value.resolution() == ProofResolution.AMBIGUOUS
            );
        }
    }

    @Test
    void shouldCompleteUnsupportedPrerequisiteBeforeStimulus() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            AtomicInteger stimulusCalls = new AtomicInteger();
            SemanticHold hold = fixture.declareHold("held", DEADLINE);
            ProofPlan plan = ProofPlan.builder(
                "unsupported-prerequisite",
                "Unsupported prerequisite",
                fixture.subject,
                DEADLINE
            ).prerequisite(
                "runtime-prerequisite",
                fixture.environment.proofs().unsupportedPrerequisite()
            ).observation(
                "observation",
                fixture.connectionId,
                ProofTestFixture.PROFILE
            ).control(
                "hold-control",
                hold,
                SemanticHoldState.FORWARDED
            ).evidence("hold-evidence", hold).build();

            ProofExecution execution = fixture.environment.proofs().activate(plan);
            execution.runStimulus(stimulusCalls::incrementAndGet);

            assertThat(execution.result().outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            assertThat(stimulusCalls).hasValue(0);
            assertThat(hold.state()).isEqualTo(SemanticHoldState.CANCELLED);
        }
    }

    @Test
    void shouldRejectMalformedAndIncompatiblePlansBeforeStimulus() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            AtomicInteger stimulusCalls = new AtomicInteger();
            assertThatThrownBy(() -> ProofPlan.builder(
                "malformed",
                "Malformed plan",
                fixture.subject,
                DEADLINE
            ).build()).isInstanceOf(ProofConfigurationException.class);

            ProofPlan incompatible = ProofPlan.builder(
                "profile-mismatch",
                "Profile mismatch",
                fixture.subject,
                DEADLINE
            ).observation(
                "observation",
                fixture.connectionId,
                new io.github.jacekkardys.systemproof.observation.RequiredObservationProfile(
                    ProofTestFixture.EVIDENCE_SCHEMA,
                    java.util.Optional.empty(),
                    java.util.Set.of(),
                    java.util.Set.of()
                )
            ).build();

            assertThatThrownBy(() -> fixture.environment.proofs().activate(incompatible))
                .isInstanceOf(ProofConfigurationException.class);
            ProofPlan schemaMismatch = ProofPlan.builder(
                "schema-mismatch",
                "Schema mismatch",
                fixture.subject,
                DEADLINE
            ).observation(
                "observation",
                fixture.connectionId,
                ProofTestFixture.PROFILE
            ).correlation(
                "correlation",
                fixture.connectionId,
                fixture.key,
                new io.github.jacekkardys.systemproof.observation.EvidenceSchemaId(
                    "system-proof-test",
                    "wrong-native-reference",
                    1
                )
            ).build();
            assertThatThrownBy(() -> fixture.environment.proofs().activate(schemaMismatch))
                .isInstanceOf(ProofConfigurationException.class);
            assertThat(stimulusCalls).hasValue(0);
        }
    }

    @Test
    void shouldFailClosedOnStimulusGatewayControlAndObservationFailures()
        throws Exception {
        try (ProofTestFixture stimulus = ProofTestFixture.start()) {
            ProofExecution execution = stimulus.environment.proofs().activate(
                satisfiedPrerequisitePlan(stimulus, "stimulus-failure")
            );
            execution.runStimulus(() -> {
                throw new IllegalStateException("untrusted stimulus details");
            });
            assertErrorAt(execution.result(), ProofFailureStage.STIMULUS);
        }

        try (ProofTestFixture control = ProofTestFixture.start()) {
            SemanticPredecessorGuard guard = control.declareGuard(DEADLINE);
            ProofExecution execution = control.environment.proofs().activate(
                completeGuardPlan(control, guard)
            );
            forward(control, control.correlated("predecessor"));
            ForwardingPermit successor = control.permit(control.correlated("successor"));
            assertThat(successor.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
            successor.writeFailed();
            assertErrorAt(execution.result(), ProofFailureStage.CONTROL);
        }

        try (ProofTestFixture observation = ProofTestFixture.start()) {
            ProofExecution execution = observation.environment.proofs().activate(
                correlationPlan(observation, "observation-failure")
            );
            observation.observationStatus(EffectiveObservationStatus.FAILED);
            assertErrorAt(execution.result(), ProofFailureStage.OBSERVATION);
        }
    }

    @Test
    void shouldTreatDeadlineAndPrematureSessionEndAsInconclusive() throws Exception {
        try (ProofTestFixture timeout = ProofTestFixture.start()) {
            SemanticPredecessorGuard guard = timeout.declareGuard(
                Duration.ofMillis(1)
            );
            ProofExecution execution = timeout.environment.proofs().activate(
                completeGuardPlan(timeout, guard)
            );

            assertThat(guard.completion().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isEqualTo(SemanticPredecessorGuardState.TIMED_OUT);
            assertThat(execution.result().outcome()).isEqualTo(
                ProofOutcome.INCONCLUSIVE
            );
        }

        try (ProofTestFixture abandoned = ProofTestFixture.start()) {
            SemanticHold hold = abandoned.declareHold("held", DEADLINE);
            ProofExecution execution = abandoned.environment.proofs().activate(
                holdPlan(abandoned, hold)
            );
            ForwardingPermit permit = abandoned.permit(abandoned.correlated("held"));
            assertThat(hold.reached().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isNotNull();
            permit.abandoned();

            assertThat(execution.result().outcome()).isEqualTo(
                ProofOutcome.INCONCLUSIVE
            );
        }
    }

    @Test
    void shouldIgnorePreActivationAndUnrelatedEvidence() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            fixture.correlated("predecessor");
            ProofExecution execution = fixture.environment.proofs().activate(
                correlationPlan(fixture, "preactivation-evidence")
            );
            execution.runStimulus(() -> {});

            assertThat(execution.evaluate().outcome()).isEqualTo(
                ProofOutcome.INCONCLUSIVE
            );
            assertThat(execution.result().resolutions()).anyMatch(
                value -> value.resolution() == ProofResolution.MISSING
            );
        }

        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            ProofExecution execution = fixture.environment.proofs().activate(
                correlationPlan(fixture, "unrelated-evidence")
            );
            fixture.correlated("successor");
            execution.runStimulus(() -> {});

            assertThat(execution.evaluate().outcome()).isEqualTo(
                ProofOutcome.INCONCLUSIVE
            );
        }
    }

    @Test
    void shouldNeverRestoreObservationCoverageAfterLoss() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            ProofExecution execution = fixture.environment.proofs().activate(
                correlationPlan(fixture, "sticky-observation-loss")
            );

            fixture.observationStatus(EffectiveObservationStatus.DEGRADED);
            fixture.observationStatus(EffectiveObservationStatus.ACTIVE);

            assertThat(execution.result().outcome()).isEqualTo(
                ProofOutcome.INCONCLUSIVE
            );
            assertThat(execution.evaluate()).isSameAs(execution.result());
        }
    }

    @Test
    void shouldFailAControlFreePlanOnAsynchronousRequiredObservationFailure() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            ProofExecution execution = fixture.environment.proofs().activate(
                correlationPlan(fixture, "asynchronous-observation-failure")
            );

            fixture.route.coordinator().observationFailed(fixture.connectionId);

            assertErrorAt(execution.result(), ProofFailureStage.OBSERVATION);
        }
    }

    @Test
    void shouldRejectBuilderReuseAndSecondExecution() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            ProofPlan.Builder builder = ProofPlan.builder(
                "one-shot-plan",
                "One-shot plan",
                fixture.subject,
                DEADLINE
            ).prerequisite(
                "prerequisite",
                fixture.environment.proofs().satisfiedPrerequisite()
            );
            ProofPlan plan = builder.build();
            ProofExecution execution = fixture.environment.proofs().activate(plan);
            execution.runStimulus(() -> {});
            execution.evaluate();

            assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> fixture.environment.proofs().activate(plan))
                .isInstanceOf(ProofConfigurationException.class);
        }
    }

    @Test
    void shouldExposeUnfinishedExecutionAsDeterministicTeardownFailure() {
        ProofTestFixture fixture = ProofTestFixture.start();
        ProofExecution execution = fixture.environment.proofs().activate(
            correlationPlan(fixture, "unfinished-proof")
        );

        assertThatThrownBy(fixture::close)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("unfinished active proof execution");
        assertErrorAt(execution.result(), ProofFailureStage.TEARDOWN);
    }

    private static ProofPlan completeGuardPlan(
        ProofTestFixture fixture,
        SemanticPredecessorGuard guard
    ) {
        return ProofPlan.builder(
            "complete-guard-proof",
            "Complete guard proof",
            fixture.subject,
            DEADLINE
        ).prerequisite(
            "runtime-prerequisite",
            fixture.environment.proofs().satisfiedPrerequisite()
        ).observation(
            "observation",
            fixture.connectionId,
            ProofTestFixture.PROFILE
        ).correlation(
            "correlation",
            fixture.connectionId,
            fixture.key,
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
    }

    private static ProofPlan correlationPlan(ProofTestFixture fixture, String id) {
        return ProofPlan.builder(id, "Correlation proof", fixture.subject, DEADLINE)
            .observation("observation", fixture.connectionId, ProofTestFixture.PROFILE)
            .correlation(
                "correlation",
                fixture.connectionId,
                fixture.key,
                ProofTestFixture.NATIVE_SCHEMA
            ).build();
    }

    private static ProofPlan holdPlan(ProofTestFixture fixture, SemanticHold hold) {
        return ProofPlan.builder("hold-proof", "Hold proof", fixture.subject, DEADLINE)
            .observation("observation", fixture.connectionId, ProofTestFixture.PROFILE)
            .correlation(
                "correlation",
                fixture.connectionId,
                fixture.key,
                ProofTestFixture.NATIVE_SCHEMA
            ).control("hold-control", hold, SemanticHoldState.FORWARDED)
            .evidence("hold-evidence", hold)
            .build();
    }

    private static ProofPlan satisfiedPrerequisitePlan(
        ProofTestFixture fixture,
        String id
    ) {
        return ProofPlan.builder(id, "Satisfied prerequisite", fixture.subject, DEADLINE)
            .prerequisite(
                "prerequisite",
                fixture.environment.proofs().satisfiedPrerequisite()
            ).build();
    }

    private static void forward(
        ProofTestFixture fixture,
        RecordedInteraction interaction
    ) {
        try {
            ForwardingPermit permit = fixture.permit(interaction);
            if (permit.awaitDecision() != ForwardingDecision.FORWARD) {
                throw new AssertionError("Interaction was not authorized for forwarding");
            }
            permit.forwarded();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while forwarding test interaction", interrupted);
        }
    }

    private static void assertErrorAt(ProofResult result, ProofFailureStage stage) {
        assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
        assertThat(result.primaryFailure()).hasValueSatisfying(
            diagnostic -> assertThat(diagnostic.stage()).isEqualTo(stage)
        );
    }
}
