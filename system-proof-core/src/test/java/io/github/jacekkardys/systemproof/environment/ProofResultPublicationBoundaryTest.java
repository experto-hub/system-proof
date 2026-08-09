package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofFailureStage;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.proof.ProofResult;

class ProofResultPublicationBoundaryTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);

    @Test
    void shouldRetainIndependentFailureAfterPrimarySelectionUntilResultFreeze()
        throws Exception {
        AtomicReference<ProofRuntimeHarness> runtime = new AtomicReference<>();
        AtomicBoolean injectOnce = new AtomicBoolean();
        CountDownLatch injected = new CountDownLatch(1);
        AtomicReference<Thread> injector = new AtomicReference<>();
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void authoritativeOutcomeSelectedBeforeFinalization() {
                if (!injectOnce.compareAndSet(false, true)) {
                    return;
                }
                Thread thread = Thread.ofPlatform().daemon(true).unstarted(() -> {
                    runtime.get().cleanupFailure();
                    injected.countDown();
                });
                injector.set(thread);
                thread.start();
                await(injected, "independent failure retention");
            }
        };

        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks)) {
            runtime.set(harness);
            SemanticHold first = harness.declareHold("shared");
            SemanticHold second = harness.declareHold("shared");
            ProofExecution execution = harness.activate(twoHoldPlan(harness, first, second));

            ForwardingPermit permit = permit(harness, "shared");
            assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            injector.get().join();
            ProofResult result = execution.result();

            assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            assertThat(result.primaryFailure()).isEmpty();
            for (String id : List.of("first", "second")) {
                assertThat(resolution(result, id).resolution())
                    .isEqualTo(ProofResolution.AMBIGUOUS);
                assertThat(resolution(result, id).reason())
                    .isEqualTo(ProofResolutionReason.CONTROL_MATCH_AMBIGUOUS);
            }
            assertThat(result.secondaryDiagnostics())
                .singleElement()
                .satisfies(value -> assertThat(value.stage())
                    .isEqualTo(ProofFailureStage.CLEANUP));
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    @Test
    void shouldSelectIndependentFailureAsPrimaryWhenItLinearizesBeforeAuthoritativeOperation()
        throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticHold first = harness.declareHold("shared");
            SemanticHold second = harness.declareHold("shared");
            ProofExecution execution = harness.activate(twoHoldPlan(harness, first, second));

            harness.cleanupFailure();
            ForwardingPermit permit = permit(harness, "shared");
            assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            ProofResult result = execution.result();

            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).get().satisfies(value ->
                assertThat(value.stage()).isEqualTo(ProofFailureStage.CLEANUP)
            );
            assertThat(resolution(result, "prerequisite").resolution())
                .isEqualTo(ProofResolution.SATISFIED);
            assertThat(resolution(result, "observation").resolution())
                .isEqualTo(ProofResolution.SATISFIED);
            for (String id : List.of("first", "second")) {
                assertThat(resolution(result, id).resolution())
                    .isEqualTo(ProofResolution.UNREACHED);
                assertThat(resolution(result, id).reason())
                    .isEqualTo(ProofResolutionReason.CONTROL_UNREACHED);
            }
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    @Test
    void shouldRecoverOnlyFromDeliberatelyInjectedResultConstructionCorruption()
        throws Exception {
        AtomicBoolean injectOnce = new AtomicBoolean();
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public RuntimeException resultConstructionFailure() {
                return injectOnce.compareAndSet(false, true)
                    ? new InjectedResultConstructionCorruption()
                    : null;
            }
        };

        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks)) {
            SemanticHold first = harness.declareHold("shared");
            SemanticHold second = harness.declareHold("shared");
            ProofExecution execution = harness.activate(twoHoldPlan(harness, first, second));

            ForwardingPermit permit = permit(harness, "shared");
            assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            ProofResult result = execution.result();

            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).get().satisfies(value ->
                assertThat(value.stage()).isEqualTo(ProofFailureStage.EVALUATION)
            );
            assertThat(result.resolutions()).allSatisfy(value -> {
                assertThat(value.resolution()).isEqualTo(ProofResolution.NOT_EVALUATED);
                assertThat(value.reason()).isEqualTo(
                    ProofResolutionReason.NOT_EVALUATED_AFTER_TERMINAL_OUTCOME
                );
            });
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result, 1);
        }
    }

    @Test
    void shouldNeverNormalizeFatalJvmFailureAsProofResult() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = harness.activate(ProofPlan.builder(
                "fatal-stimulus-failure",
                "Fatal stimulus failure",
                harness.subject,
                DEADLINE
            ).prerequisite(
                "prerequisite",
                harness.prerequisite()
            ).observation(
                "observation",
                harness.connectionId,
                ProofTestFixture.PROFILE
            ).build());
            assertThatThrownBy(() -> execution.runStimulus(() -> {
                throw new InjectedLinkageFailure();
            }))
                .isInstanceOf(InjectedLinkageFailure.class);
            assertThat(ProofExecutionCoordinator.publicationInvariant(execution)
                .resultConstructionRecoveryCount()).isZero();
        }
    }

    private static ProofPlan twoHoldPlan(
        ProofRuntimeHarness harness,
        SemanticHold first,
        SemanticHold second
    ) {
        return ProofPlan.builder(
            "result-publication-boundary",
            "Result publication boundary",
            harness.subject,
            DEADLINE
        ).prerequisite(
            "prerequisite",
            harness.prerequisite()
        ).observation(
            "observation",
            harness.connectionId,
            ProofTestFixture.PROFILE
        ).control(
            "first",
            first,
            SemanticHoldState.FORWARDED
        ).control(
            "second",
            second,
            SemanticHoldState.FORWARDED
        ).build();
    }

    private static ForwardingPermit permit(ProofRuntimeHarness harness, String value) {
        ProofRuntimeHarness.Recorded recorded = harness.record(value);
        harness.correlate(recorded, value);
        return harness.route.coordinator().permit(recorded.interaction());
    }

    private static io.github.jacekkardys.systemproof.proof.ProofObligationResolution resolution(
        ProofResult result,
        String id
    ) {
        return result.resolutions().stream()
            .filter(value -> value.id().value().equals(id))
            .findFirst()
            .orElseThrow();
    }

    private static void await(CountDownLatch latch, String description) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out awaiting " + description);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while awaiting " + description, interrupted);
        }
    }

    private static final class InjectedResultConstructionCorruption
        extends RuntimeException {}

    private static final class InjectedLinkageFailure extends LinkageError {}
}
