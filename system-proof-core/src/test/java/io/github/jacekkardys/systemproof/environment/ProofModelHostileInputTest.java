package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.proof.ProofConfigurationException;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;

class ProofModelHostileInputTest {
    static final String CANARY = "PROOF_CANARY_91e135ab9b6d";
    private static final Duration DEADLINE = Duration.ofSeconds(5);

    @Test
    void shouldRejectUnboundedIdentityTitleAndRequirementCount() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            assertThatThrownBy(() -> ProofPlan.builder(
                "x".repeat(129),
                "Bounded title",
                fixture.subject,
                DEADLINE
            )).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ProofPlan.builder(
                "bounded-id",
                "x".repeat(257),
                fixture.subject,
                DEADLINE
            )).isInstanceOf(IllegalArgumentException.class);

            ProofPlan.Builder oversized = ProofPlan.builder(
                "too-many-requirements",
                "Too many requirements",
                fixture.subject,
                DEADLINE
            );
            for (int index = 0; index < 256; index++) {
                oversized.prerequisite(
                    "prerequisite-" + index,
                    fixture.environment.proofs().satisfiedPrerequisite()
                );
            }
            assertThatThrownBy(() -> oversized.prerequisite(
                "prerequisite-256",
                fixture.environment.proofs().satisfiedPrerequisite()
            ))
                .isInstanceOf(ProofConfigurationException.class)
                .hasMessageContaining("at most 256");
        }
    }

    @Test
    void shouldNeverRenderArbitraryFailureMessage() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            ProofPlan plan = ProofPlan.builder(
                "canary-failure",
                "Canary failure",
                fixture.subject,
                DEADLINE
            ).prerequisite(
                "failed-prerequisite",
                fixture.environment.proofs().failedPrerequisite(
                    new IllegalStateException(CANARY)
                )
            ).build();

            ProofResult result = fixture.environment.proofs().activate(plan).result();

            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(result.report().content()).doesNotContain(CANARY);
            assertThat(result.toString()).doesNotContain(CANARY);
            assertThat(result.primaryFailure().orElseThrow().toString())
                .doesNotContain(CANARY);
        }
    }

    @Test
    void shouldNotInvokeHostileOpaqueReferenceToString() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            ToStringProbe probe = new ToStringProbe();
            ProofSubjectRef hostile = new ProofSubjectRef() {
                @Override
                public String toString() {
                    probe.calls.incrementAndGet();
                    throw new AssertionError("Opaque subject toString must not be called");
                }
            };
            ProofPlan plan = ProofPlan.builder(
                "hostile-reference",
                "Hostile reference",
                hostile,
                DEADLINE
            ).prerequisite(
                "prerequisite",
                fixture.environment.proofs().satisfiedPrerequisite()
            ).build();

            assertThat(plan.toString()).contains("primarySubject=opaque");
            assertThatThrownBy(() -> fixture.environment.proofs().activate(plan))
                .isInstanceOf(ProofConfigurationException.class);
            assertThat(probe.calls).hasValue(0);
        }
    }

    @Test
    void shouldBoundSecondaryFailuresWithoutChangingPrimaryViolation() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(ProofPlan.builder(
                "bounded-secondary",
                "Bounded secondary diagnostics",
                harness.subject,
                DEADLINE
            ).observation(
                "observation",
                harness.connectionId,
                ProofTestFixture.PROFILE
            ).control(
                "guard-control",
                guard,
                SemanticPredecessorGuardState.SATISFIED
            ).build());
            harness.publish("successor");
            for (int index = 0; index < 100; index++) {
                harness.cleanupFailure();
            }

            ProofResult result = execution.result();

            assertThat(result.outcome()).isEqualTo(ProofOutcome.VIOLATED);
            assertThat(result.secondaryDiagnostics()).isEmpty();
            assertThat(result.report().content()).doesNotContain(CANARY);
        }
    }

    @Test
    void shouldDetachAndDeeplyFreezeCompletedResult() {
        ProofResult retained;
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            ProofExecution execution = fixture.environment.proofs().activate(
                ProofPlan.builder(
                    "immutable-result",
                    "Immutable result",
                    fixture.subject,
                    DEADLINE
                ).prerequisite(
                    "prerequisite",
                    fixture.environment.proofs().satisfiedPrerequisite()
                ).build()
            );
            execution.runStimulus(() -> {});
            retained = execution.evaluate();
            assertThatThrownBy(() -> retained.resolutions().clear())
                .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> retained.secondaryDiagnostics().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        }

        assertThat(retained.outcome()).isEqualTo(ProofOutcome.PROVED);
        assertThat(retained.report().content()).contains("outcome=PROVED");
    }

    @Test
    void shouldRenderEquivalentDecisiveExplanationsByteForByte() {
        String first;
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            first = provedReport(harness);
        }
        String second;
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            second = provedReport(harness);
        }

        assertThat(second.getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .containsExactly(first.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String provedReport(ProofRuntimeHarness harness) {
        ProofExecution execution = harness.activate(ProofPlan.builder(
            "deterministic-report",
            "Deterministic report",
            harness.subject,
            DEADLINE
        ).prerequisite("prerequisite", harness.prerequisite()).build());
        execution.runStimulus(() -> {});
        return execution.evaluate()
            .report()
            .content();
    }

    private static final class ToStringProbe {
        private final AtomicInteger calls = new AtomicInteger();
    }
}
