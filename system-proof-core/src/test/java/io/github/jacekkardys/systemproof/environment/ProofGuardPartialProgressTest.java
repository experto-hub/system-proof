package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.observation.SessionId;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofObligationId;
import io.github.jacekkardys.systemproof.proof.ProofObligationResolution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofRequirementDescriptor;
import io.github.jacekkardys.systemproof.proof.ProofRequirementKind;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.topology.ConnectionId;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class ProofGuardPartialProgressTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);

    @RepeatedTest(20)
    void shouldMaterializePredecessorOnlyTimeoutAsInconclusive() throws Exception {
        ProofRuntimeHarness.ManualControlTimeoutScheduler timeouts =
            new ProofRuntimeHarness.ManualControlTimeoutScheduler();
        try (ProofRuntimeHarness harness =
                 ProofRuntimeHarness.startWithManualControlTimeout(timeouts)) {
            SemanticPredecessorGuard guard = harness.declareForwardedGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));

            execution.runStimulus(() -> {
                forward(predecessorPermit(harness));
                timeouts.fire();
            });

            assertClosedResult(
                execution,
                ProofOutcome.INCONCLUSIVE,
                ProofResolution.TIMED_OUT,
                ProofResolutionReason.CONTROL_TIMED_OUT,
                harness.connectionId
            );
        }
    }

    @RepeatedTest(20)
    void shouldMaterializePredecessorOnlySessionAbandonmentAsInconclusive()
        throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareForwardedGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));

            execution.runStimulus(() -> abandon(predecessorPermit(harness)));

            assertClosedResult(
                execution,
                ProofOutcome.INCONCLUSIVE,
                ProofResolution.MISSING,
                ProofResolutionReason.CONTROL_SESSION_ENDED,
                harness.connectionId
            );
        }
    }

    @RepeatedTest(20)
    void shouldMaterializePredecessorOnlyCorrelationInvalidationAsInconclusive()
        throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareForwardedGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));

            execution.runStimulus(() -> {
                ForwardingPermit predecessor = predecessorPermit(harness);
                harness.proofSubjects.arm(harness.proofSubjects.create(), harness.key);
                forward(predecessor);
            });

            assertClosedResult(
                execution,
                ProofOutcome.INCONCLUSIVE,
                ProofResolution.AMBIGUOUS,
                ProofResolutionReason.CONTROL_CORRELATION_INVALIDATED,
                harness.connectionId
            );
        }
    }

    @RepeatedTest(20)
    void shouldMaterializePredecessorForwardingFailureAsError() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareForwardedGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));

            execution.runStimulus(() -> failWrite(predecessorPermit(harness)));

            assertClosedResult(
                execution,
                ProofOutcome.ERROR,
                ProofResolution.FAILED,
                ProofResolutionReason.CONTROL_FAILED,
                harness.connectionId
            );
        }
    }

    @RepeatedTest(20)
    void shouldFreezeEvaluationAfterPredecessorProgressAsOneInconclusiveResult()
        throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareForwardedGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));
            execution.runStimulus(() -> forward(predecessorPermit(harness)));

            ProofResult frozen = execution.evaluate();

            assertThat(frozen.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            assertThat(execution.result()).isSameAs(frozen);
            assertThat(execution.evaluate()).isSameAs(frozen);
            assertThat(controlResolution(frozen).resolution())
                .isEqualTo(ProofResolution.UNREACHED);
            assertThat(guard.completion().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isEqualTo(SemanticPredecessorGuardState.CANCELLED);
        }
    }

    @Test
    void shouldRejectSuccessorOnlyAndContradictoryPartialGuardProvenance() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ConnectionId predecessor = ConnectionId.of(
                "predecessor[].required->provider[].provided"
            );
            ConnectionId successor = ConnectionId.of(
                "successor[].required->provider[].provided"
            );
            InteractionRef predecessorInteraction = interaction(predecessor);
            InteractionRef successorInteraction = interaction(successor);
            ProofRequirementDescriptor.GuardControl descriptor =
                new ProofRequirementDescriptor.GuardControl(
                    guard.ref(),
                    SemanticPredecessorGuardState.SATISFIED,
                    predecessor,
                    successor
                );

            assertThatThrownBy(() -> guardResolution(
                descriptor,
                ProofResolution.TIMED_OUT,
                ProofResolutionReason.CONTROL_TIMED_OUT,
                List.of(successorInteraction)
            )).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> guardResolution(
                descriptor,
                ProofResolution.SATISFIED,
                ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE,
                List.of(successorInteraction, predecessorInteraction)
            )).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> guardResolution(
                descriptor,
                ProofResolution.TIMED_OUT,
                ProofResolutionReason.CONTROL_TIMED_OUT,
                List.of(predecessorInteraction, successorInteraction)
            )).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static ProofPlan guardPlan(
        ProofRuntimeHarness harness,
        SemanticPredecessorGuard guard
    ) {
        return ProofPlan.builder(
            "partial-guard-progress",
            "Partial guard progress",
            harness.subject,
            DEADLINE
        ).prerequisite(
            "prerequisite",
            harness.prerequisite()
        ).observation(
            "observation",
            harness.connectionId,
            ProofTestFixture.PROFILE
        ).correlation(
            "correlation",
            harness.connectionId,
            harness.key,
            ProofTestFixture.NATIVE_SCHEMA
        ).control(
            "guard-control",
            guard,
            SemanticPredecessorGuardState.SATISFIED
        ).build();
    }

    private static ForwardingPermit predecessorPermit(ProofRuntimeHarness harness) {
        ProofRuntimeHarness.Recorded predecessor = harness.record("predecessor");
        harness.correlate(predecessor, "predecessor");
        return harness.route.coordinator().permit(predecessor.interaction());
    }

    private static void forward(ForwardingPermit permit) {
        assertForward(permit);
        permit.forwarded();
    }

    private static void abandon(ForwardingPermit permit) {
        assertForward(permit);
        permit.abandoned();
    }

    private static void failWrite(ForwardingPermit permit) {
        assertForward(permit);
        permit.writeFailed();
    }

    private static void assertForward(ForwardingPermit permit) {
        try {
            assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while awaiting forwarding decision", interrupted);
        }
    }

    private static void assertClosedResult(
        ProofExecution execution,
        ProofOutcome expectedOutcome,
        ProofResolution expectedResolution,
        ProofResolutionReason expectedReason,
        ConnectionId predecessorConnection
    ) {
        ProofResult result = execution.result();
        ProofObligationResolution control = controlResolution(result);

        assertThat(result.outcome()).isEqualTo(expectedOutcome);
        assertThat(control.resolution()).isEqualTo(expectedResolution);
        assertThat(control.reason()).isEqualTo(expectedReason);
        assertThat(control.interactions()).singleElement().satisfies(
            interaction -> assertThat(interaction.connectionId())
                .isEqualTo(predecessorConnection)
        );
        assertThat(execution.result()).isSameAs(result);
        assertThat(execution.evaluate()).isSameAs(result);
    }

    private static ProofObligationResolution controlResolution(ProofResult result) {
        return result.resolutions().stream()
            .filter(value -> value.id().toString().equals("guard-control"))
            .findFirst()
            .orElseThrow();
    }

    private static InteractionRef interaction(ConnectionId connectionId) {
        return new InteractionRef(
            new SessionId(connectionId, 1),
            FlowDirection.CONSUMER_TO_PROVIDER,
            1
        );
    }

    private static ProofObligationResolution guardResolution(
        ProofRequirementDescriptor.GuardControl descriptor,
        ProofResolution resolution,
        ProofResolutionReason reason,
        List<InteractionRef> interactions
    ) {
        return new ProofObligationResolution(
            new ProofObligationId("forged-guard-provenance"),
            ProofRequirementKind.CONTROL,
            descriptor,
            resolution,
            reason,
            Optional.empty(),
            interactions
        );
    }
}
