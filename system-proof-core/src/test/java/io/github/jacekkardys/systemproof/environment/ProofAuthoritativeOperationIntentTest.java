package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorBoundary;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofFailureStage;
import io.github.jacekkardys.systemproof.proof.ProofObligationResolution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.proof.ProofResult;

class ProofAuthoritativeOperationIntentTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);

    @RepeatedTest(20)
    void shouldBatchRequiredObservationFailureWithBothGuardsInEitherDeclarationOrder() {
        assertRequiredObservationFailure(false);
        assertRequiredObservationFailure(true);
    }

    @Test
    void shouldNotFreezeBeforeAJournalFailureAtEitherSiblingBoundary() {
        assertJournalFailureAfterCommittedGuards(0);
        assertJournalFailureAfterCommittedGuards(1);
    }

    @Test
    void shouldKeepNestedOperationsInTheParentTokenUntilTheOuterBoundary() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = harness.activate(observationOnlyPlan(harness));

            harness.events.proofFactBatch(() -> {
                harness.events.proofFactBatch(() -> {
                    harness.proofs.requiredObservationFailed(harness.connectionId);
                    assertThat(ProofExecutionCoordinator.publicationInvariant(execution)
                        .outcomeSelected()).isFalse();
                    return null;
                });
                assertThat(ProofExecutionCoordinator.publicationInvariant(execution)
                    .outcomeSelected()).isFalse();
                return null;
            });

            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertResolution(
                result,
                "observation",
                ProofResolution.FAILED,
                ProofResolutionReason.OBSERVATION_FAILED
            );
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    @Test
    void shouldClearOperationOwnershipAfterAnExceptionalActionExit() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = harness.activate(observationOnlyPlan(harness));

            assertThatThrownBy(() -> harness.events.proofFactBatch(() -> {
                throw new InjectedOperationFailure();
            })).isInstanceOf(InjectedOperationFailure.class);

            ProofExecutionCoordinator.PublicationInvariant active =
                ProofExecutionCoordinator.publicationInvariant(execution);
            assertThat(active.outcomeSelected()).isFalse();
            assertThat(active.authoritativeOperationOwnerPresent()).isFalse();
            assertThat(active.factBatchActive()).isFalse();
            assertThat(active.pendingCompletionPresent()).isFalse();

            harness.controls.observationFailed(harness.connectionId);
            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    private static void assertRequiredObservationFailure(boolean reverseDeclarationOrder) {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            NamedGuards guards = declareGuards(harness, reverseDeclarationOrder);
            ProofExecution execution = harness.activate(plan(harness, guards));

            harness.controls.observationFailed(harness.connectionId);

            assertThat(guards.alpha.state())
                .isEqualTo(SemanticPredecessorGuardState.FAILED);
            assertThat(guards.beta.state())
                .isEqualTo(SemanticPredecessorGuardState.FAILED);
            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).get().extracting(value -> value.stage())
                .isEqualTo(ProofFailureStage.OBSERVATION);
            assertResolution(
                result,
                "observation",
                ProofResolution.FAILED,
                ProofResolutionReason.OBSERVATION_FAILED
            );
            assertResolution(
                result,
                "alpha",
                ProofResolution.FAILED,
                ProofResolutionReason.CONTROL_FAILED
            );
            assertResolution(
                result,
                "beta",
                ProofResolution.FAILED,
                ProofResolutionReason.CONTROL_FAILED
            );
            assertThat(resolution(result, "alpha").provenance()).isEmpty();
            assertThat(resolution(result, "beta").provenance()).isEmpty();
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertNoDeadlockedThread();
        }
    }

    private static void assertJournalFailureAfterCommittedGuards(int committedGuards) {
        FailingNanoTime nanoTime = new FailingNanoTime();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithJournal(
            new ScenarioJournal(nanoTime)
        )) {
            NamedGuards guards = declareGuards(harness, false);
            ProofExecution execution = harness.activate(plan(harness, guards));
            nanoTime.failAfterSuccessfulAppends(committedGuards);

            assertThatThrownBy(() ->
                harness.controls.observationFailed(harness.connectionId)
            ).isInstanceOf(InjectedJournalFailure.class);

            assertThat(guards.alpha.state()).isEqualTo(
                committedGuards == 0
                    ? SemanticPredecessorGuardState.CANCELLED
                    : SemanticPredecessorGuardState.FAILED
            );
            assertThat(guards.beta.state())
                .isEqualTo(SemanticPredecessorGuardState.CANCELLED);
            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).get().extracting(value -> value.stage())
                .isEqualTo(ProofFailureStage.OBSERVATION);
            assertResolution(
                result,
                "observation",
                ProofResolution.FAILED,
                ProofResolutionReason.OBSERVATION_FAILED
            );
            assertResolution(
                result,
                "alpha",
                committedGuards == 0
                    ? ProofResolution.UNREACHED
                    : ProofResolution.FAILED,
                committedGuards == 0
                    ? ProofResolutionReason.CONTROL_UNREACHED
                    : ProofResolutionReason.CONTROL_FAILED
            );
            assertResolution(
                result,
                "beta",
                ProofResolution.UNREACHED,
                ProofResolutionReason.CONTROL_UNREACHED
            );
            assertThat(result.secondaryDiagnostics()).anySatisfy(diagnostic ->
                assertThat(diagnostic.stage()).isEqualTo(ProofFailureStage.JOURNAL)
            );
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertNoDeadlockedThread();
        }
    }

    private static NamedGuards declareGuards(
        ProofRuntimeHarness harness,
        boolean reverseDeclarationOrder
    ) {
        if (reverseDeclarationOrder) {
            SemanticPredecessorGuard beta = guard(harness, "beta");
            SemanticPredecessorGuard alpha = guard(harness, "alpha");
            return new NamedGuards(alpha, beta, true);
        }
        SemanticPredecessorGuard alpha = guard(harness, "alpha");
        SemanticPredecessorGuard beta = guard(harness, "beta");
        return new NamedGuards(alpha, beta, false);
    }

    private static SemanticPredecessorGuard guard(
        ProofRuntimeHarness harness,
        String name
    ) {
        return harness.declareGuard(
            SemanticPredecessorBoundary.CONFIRMED,
            value -> value.equals(name + "-predecessor"),
            value -> value.equals(name + "-successor")
        );
    }

    private static ProofPlan plan(ProofRuntimeHarness harness, NamedGuards guards) {
        ProofPlan.Builder builder = ProofPlan.builder(
            "direct-intent-batch",
            "Authoritative direct proof intent batch",
            harness.subject,
            DEADLINE
        ).prerequisite("prerequisite", harness.prerequisite())
            .observation("observation", harness.connectionId, ProofTestFixture.PROFILE);
        if (guards.reverseDeclarationOrder) {
            builder.control(
                "beta",
                guards.beta,
                SemanticPredecessorGuardState.SATISFIED
            ).control(
                "alpha",
                guards.alpha,
                SemanticPredecessorGuardState.SATISFIED
            );
        } else {
            builder.control(
                "alpha",
                guards.alpha,
                SemanticPredecessorGuardState.SATISFIED
            ).control(
                "beta",
                guards.beta,
                SemanticPredecessorGuardState.SATISFIED
            );
        }
        return builder.build();
    }

    private static ProofPlan observationOnlyPlan(ProofRuntimeHarness harness) {
        return ProofPlan.builder(
            "direct-intent-owner",
            "Authoritative direct proof intent ownership",
            harness.subject,
            DEADLINE
        ).prerequisite("prerequisite", harness.prerequisite())
            .observation("observation", harness.connectionId, ProofTestFixture.PROFILE)
            .build();
    }

    private static void assertResolution(
        ProofResult result,
        String id,
        ProofResolution expectedResolution,
        ProofResolutionReason expectedReason
    ) {
        ProofObligationResolution resolution = resolution(result, id);
        assertThat(resolution.resolution()).isEqualTo(expectedResolution);
        assertThat(resolution.reason()).isEqualTo(expectedReason);
    }

    private static ProofObligationResolution resolution(ProofResult result, String id) {
        return result.resolutions().stream()
            .filter(value -> value.id().value().equals(id))
            .findFirst()
            .orElseThrow();
    }

    private static void assertNoDeadlockedThread() {
        assertThat(ManagementFactory.getThreadMXBean().findDeadlockedThreads()).isNull();
    }

    private record NamedGuards(
        SemanticPredecessorGuard alpha,
        SemanticPredecessorGuard beta,
        boolean reverseDeclarationOrder
    ) {}

    private static final class FailingNanoTime implements LongSupplier {
        private static final int DISABLED = -1;
        private final AtomicInteger successfulCallsBeforeFailure =
            new AtomicInteger(DISABLED);

        @Override
        public long getAsLong() {
            int remaining = successfulCallsBeforeFailure.get();
            if (remaining == DISABLED) {
                return 0L;
            }
            if (remaining == 0
                && successfulCallsBeforeFailure.compareAndSet(0, DISABLED)) {
                throw new InjectedJournalFailure();
            }
            successfulCallsBeforeFailure.decrementAndGet();
            return 0L;
        }

        private void failAfterSuccessfulAppends(int successfulAppends) {
            assertThat(successfulAppends).isBetween(0, 1);
            assertThat(successfulCallsBeforeFailure.compareAndSet(
                DISABLED,
                successfulAppends
            )).isTrue();
        }
    }

    private static final class InjectedJournalFailure extends RuntimeException {}

    private static final class InjectedOperationFailure extends RuntimeException {}
}
