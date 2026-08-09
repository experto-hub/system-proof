package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorBoundary;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardFailure;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.observation.RecordedInteraction;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofFailureStage;
import io.github.jacekkardys.systemproof.proof.ProofInteractionProvenance;
import io.github.jacekkardys.systemproof.proof.ProofObligationResolution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.proof.ProofResult;

class ProofSemanticControlAuthoritativeBatchTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final String SECRET_CANARY =
        "proof-16-selector-batch-secret-canary-6f4d2c91";

    @Test
    void shouldFailClosedWhenPredecessorSelectorThrowsAndSuccessorDoesNotMatch()
        throws Exception {
        assertSelectorFailure(false);
    }

    @Test
    void shouldNotLetMatchingHoldObscureRequiredGuardSelectorFailure() throws Exception {
        assertSelectorFailure(true);
    }

    @RepeatedTest(20)
    void shouldPreferRequiredSelectorFailureOverPotentialViolationInEitherDeclarationOrder()
        throws Exception {
        Map<String, ResolutionProjection> forward = selectorFailureAndViolation(false);
        Map<String, ResolutionProjection> reverse = selectorFailureAndViolation(true);

        assertThat(reverse).isEqualTo(forward);
    }

    @Test
    void shouldIgnoreUnplannedGuardFailureWhenRequiredGuardIsViolated() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard unplanned = failingGuard(harness);
            SemanticPredecessorGuard required = harness.declareGuard(
                SemanticPredecessorBoundary.CONFIRMED,
                value -> false,
                "candidate"::equals
            );
            ProofExecution execution = harness.activate(singleGuardPlan(
                harness,
                "required-violation",
                required
            ));
            harness.controls.activatePreparedControls(
                List.of(),
                List.of(unplanned.ref()),
                () -> ignored -> true
            );

            ForwardingPermit permit = permit(harness, "candidate");
            assertThat(await(permit)).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            ProofResult result = execution.result();

            assertThat(unplanned.state()).isEqualTo(SemanticPredecessorGuardState.FAILED);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.VIOLATED);
            assertThat(result.primaryFailure()).isEmpty();
            assertResolution(
                result,
                "guard-control",
                ProofResolution.VIOLATED,
                ProofResolutionReason.CAUSAL_RELATION_VIOLATED
            );
            assertFrozen(execution, result);
        }
    }

    @RepeatedTest(20)
    void shouldBatchAllSiblingWriteFailuresInEitherDeclarationOrder() throws Exception {
        Map<String, ResolutionProjection> forward = siblingPermitFailure(false, false);
        Map<String, ResolutionProjection> reverse = siblingPermitFailure(true, false);

        assertThat(reverse).isEqualTo(forward);
        assertThat(forward.values()).allSatisfy(value -> {
            assertThat(value.resolution()).isEqualTo(ProofResolution.FAILED);
            assertThat(value.reason()).isEqualTo(ProofResolutionReason.CONTROL_FAILED);
        });
    }

    @RepeatedTest(20)
    void shouldBatchAllSiblingSessionAbandonmentsInEitherDeclarationOrder() throws Exception {
        Map<String, ResolutionProjection> forward = siblingPermitFailure(false, true);
        Map<String, ResolutionProjection> reverse = siblingPermitFailure(true, true);

        assertThat(reverse).isEqualTo(forward);
        assertThat(forward.values()).allSatisfy(value -> {
            assertThat(value.resolution()).isEqualTo(ProofResolution.MISSING);
            assertThat(value.reason()).isEqualTo(ProofResolutionReason.CONTROL_SESSION_ENDED);
        });
    }

    @RepeatedTest(20)
    void shouldBatchSatisfiedAndCorrelationInvalidatedGuardsInEitherDeclarationOrder()
        throws Exception {
        Map<String, ResolutionProjection> forward = mixedForwardingOutcome(false);
        Map<String, ResolutionProjection> reverse = mixedForwardingOutcome(true);

        assertThat(reverse).isEqualTo(forward);
        assertThat(forward.get("invalidated").resolution())
            .isEqualTo(ProofResolution.AMBIGUOUS);
        assertThat(forward.get("invalidated").reason())
            .isEqualTo(ProofResolutionReason.CONTROL_CORRELATION_INVALIDATED);
        assertThat(forward.get("satisfied").resolution())
            .isEqualTo(ProofResolution.SATISFIED);
        assertThat(forward.get("satisfied").reason())
            .isEqualTo(ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE);
    }

    @RepeatedTest(20)
    void shouldBatchReachedHoldAndAssociatedGuardForEveryPermitTerminalCallback()
        throws Exception {
        for (AssociatedTerminal terminal : AssociatedTerminal.values()) {
            AssociatedProjection result = associatedTerminalResult(terminal);

            assertThat(result.outcome()).isEqualTo(terminal.outcome);
            assertThat(result.hold().resolution()).isEqualTo(terminal.holdResolution);
            assertThat(result.hold().reason()).isEqualTo(terminal.holdReason);
            assertThat(result.guard().resolution()).isEqualTo(terminal.guardResolution);
            assertThat(result.guard().reason()).isEqualTo(terminal.guardReason);
            assertThat(result.hold().provenance()).singleElement().satisfies(value ->
                assertThat(value.role()).isEqualTo(ProofInteractionProvenance.Role.HOLD)
            );
            assertThat(result.guard().provenance()).singleElement().satisfies(value ->
                assertThat(value.role())
                    .isEqualTo(ProofInteractionProvenance.Role.PREDECESSOR)
            );
        }
    }

    private static void assertSelectorFailure(boolean matchingHold) throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareGuard(
                SemanticPredecessorBoundary.CONFIRMED,
                value -> {
                    throw new SelectorFailure(SECRET_CANARY);
                },
                value -> false
            );
            SemanticHold hold = matchingHold ? harness.declareHold("candidate") : null;
            ProofPlan.Builder plan = basePlan(harness, "selector-failure")
                .control("guard-control", guard, SemanticPredecessorGuardState.SATISFIED);
            if (hold != null) {
                plan.control("hold-control", hold, SemanticHoldState.FORWARDED);
            }
            ProofExecution execution = harness.activate(plan.build());

            ForwardingPermit permit = permit(harness, "candidate");
            assertThat(await(permit)).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            ProofResult result = execution.result();

            assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.FAILED);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).get().extracting(value -> value.stage())
                .isEqualTo(ProofFailureStage.CONTROL);
            assertResolution(
                result,
                "guard-control",
                ProofResolution.FAILED,
                ProofResolutionReason.CONTROL_SELECTOR_FAILED
            );
            assertThat(result.resolutions()).noneMatch(
                value -> value.resolution() == ProofResolution.VIOLATED
            );
            assertThat(harness.journal.snapshot().entries()).extracting(value -> value.event())
                .filteredOn(io.github.jacekkardys.systemproof.journal
                    .SemanticPredecessorGuardEvent.class::isInstance)
                .map(io.github.jacekkardys.systemproof.journal
                    .SemanticPredecessorGuardEvent.class::cast)
                .filteredOn(value -> value.guardRef().equals(guard.ref()))
                .filteredOn(value -> value.state() == SemanticPredecessorGuardState.FAILED)
                .singleElement()
                .satisfies(value -> assertThat(value.failure())
                    .contains(SemanticPredecessorGuardFailure.SELECTOR_EVALUATION));
            assertThat(harness.journal.snapshot().entries()).extracting(value -> value.event())
                .filteredOn(io.github.jacekkardys.systemproof.journal
                    .SemanticPredecessorGuardEvent.class::isInstance)
                .map(io.github.jacekkardys.systemproof.journal
                    .SemanticPredecessorGuardEvent.class::cast)
                .filteredOn(value -> value.guardRef().equals(guard.ref()))
                .filteredOn(value -> value.decision()
                    .filter(ForwardingDecision.FORWARD::equals).isPresent())
                .isEmpty();
            if (hold != null) {
                assertResolution(
                    result,
                    "hold-control",
                    ProofResolution.UNREACHED,
                    ProofResolutionReason.CONTROL_UNREACHED
                );
            }
            assertFrozen(execution, result);
        }
    }

    private static Map<String, ResolutionProjection> selectorFailureAndViolation(
        boolean reverseDeclarationOrder
    ) throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard failing;
            SemanticPredecessorGuard violating;
            if (reverseDeclarationOrder) {
                violating = violatingGuard(harness);
                failing = failingGuard(harness);
            } else {
                failing = failingGuard(harness);
                violating = violatingGuard(harness);
            }
            ProofExecution execution = harness.activate(basePlan(
                harness,
                "selector-arbitration"
            ).control(
                "failing",
                failing,
                SemanticPredecessorGuardState.SATISFIED
            ).control(
                "violating",
                violating,
                SemanticPredecessorGuardState.SATISFIED
            ).build());

            ForwardingPermit permit = permit(harness, "candidate");
            assertThat(await(permit)).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            ProofResult result = execution.result();

            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).get().extracting(value -> value.stage())
                .isEqualTo(ProofFailureStage.CONTROL);
            assertResolution(
                result,
                "failing",
                ProofResolution.FAILED,
                ProofResolutionReason.CONTROL_SELECTOR_FAILED
            );
            assertThat(result.resolutions()).noneMatch(
                value -> value.resolution() == ProofResolution.VIOLATED
            );
            assertFrozen(execution, result);
            return projections(result, "failing", "violating");
        }
    }

    private static SemanticPredecessorGuard failingGuard(ProofRuntimeHarness harness) {
        return harness.declareGuard(
            SemanticPredecessorBoundary.CONFIRMED,
            value -> {
                throw new SelectorFailure(SECRET_CANARY);
            },
            value -> false
        );
    }

    private static SemanticPredecessorGuard violatingGuard(ProofRuntimeHarness harness) {
        return harness.declareGuard(
            SemanticPredecessorBoundary.CONFIRMED,
            value -> false,
            "candidate"::equals
        );
    }

    private static Map<String, ResolutionProjection> siblingPermitFailure(
        boolean reverseDeclarationOrder,
        boolean abandon
    ) throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard first;
            SemanticPredecessorGuard second;
            if (reverseDeclarationOrder) {
                second = harness.declareForwardedGuard();
                first = harness.declareForwardedGuard();
            } else {
                first = harness.declareForwardedGuard();
                second = harness.declareForwardedGuard();
            }
            ProofExecution execution = harness.activate(twoGuardPlan(
                harness,
                "sibling-permit-terminal",
                first,
                second
            ));

            ForwardingPermit predecessor = permit(harness, "predecessor");
            assertThat(await(predecessor)).isEqualTo(ForwardingDecision.FORWARD);
            predecessor.forwarded();
            ForwardingPermit permit = permit(harness, "successor");
            assertThat(await(permit)).isEqualTo(ForwardingDecision.FORWARD);
            if (abandon) {
                permit.abandoned();
            } else {
                permit.writeFailed();
            }
            ProofResult result = execution.result();

            assertThat(first.state()).isEqualTo(SemanticPredecessorGuardState.FAILED);
            assertThat(second.state()).isEqualTo(SemanticPredecessorGuardState.FAILED);
            assertThat(result.outcome()).isEqualTo(
                abandon ? ProofOutcome.INCONCLUSIVE : ProofOutcome.ERROR
            );
            if (abandon) {
                assertThat(result.primaryFailure()).isEmpty();
            } else {
                assertThat(result.primaryFailure()).get().extracting(value -> value.stage())
                    .isEqualTo(ProofFailureStage.CONTROL);
            }
            for (String id : List.of("first", "second")) {
                assertThat(resolution(result, id).provenance())
                    .extracting(ProofInteractionProvenance::role)
                    .containsExactly(
                        ProofInteractionProvenance.Role.PREDECESSOR,
                        ProofInteractionProvenance.Role.SUCCESSOR
                    );
            }
            assertFrozen(execution, result);
            return projections(result, "first", "second");
        }
    }

    private static Map<String, ResolutionProjection> mixedForwardingOutcome(
        boolean reverseDeclarationOrder
    ) throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard invalidated;
            SemanticPredecessorGuard satisfied;
            if (reverseDeclarationOrder) {
                satisfied = harness.declareNativeFlowGuard(
                    harness.successorKey,
                    "predecessor"::equals,
                    "successor"::equals,
                    "flow"
                );
                invalidated = harness.declareNativeFlowGuard(
                    harness.key,
                    "predecessor"::equals,
                    "successor"::equals,
                    "flow"
                );
            } else {
                invalidated = harness.declareNativeFlowGuard(
                    harness.key,
                    "predecessor"::equals,
                    "successor"::equals,
                    "flow"
                );
                satisfied = harness.declareNativeFlowGuard(
                    harness.successorKey,
                    "predecessor"::equals,
                    "successor"::equals,
                    "flow"
                );
            }
            ProofExecution execution = harness.activate(basePlan(
                harness,
                "mixed-forwarding"
            ).control(
                "invalidated",
                invalidated,
                SemanticPredecessorGuardState.SATISFIED
            ).control(
                "satisfied",
                satisfied,
                SemanticPredecessorGuardState.SATISFIED
            ).build());
            ProofRuntimeHarness.Recorded predecessor = harness.record("predecessor");
            harness.correlate(predecessor, harness.key, "flow");
            harness.correlate(predecessor, harness.successorKey, "flow");
            ForwardingPermit predecessorPermit = harness.route.coordinator()
                .permit(predecessor.interaction());
            assertThat(await(predecessorPermit)).isEqualTo(ForwardingDecision.FORWARD);
            predecessorPermit.forwarded();
            RecordedInteraction successorInteraction = predecessor.session().record(
                FlowDirection.CONSUMER_TO_PROVIDER,
                ProofTestFixture.TextCodec.INSTANCE,
                "successor"
            );
            ForwardingPermit successorPermit = harness.route.coordinator()
                .permit(successorInteraction);
            assertThat(await(successorPermit)).isEqualTo(ForwardingDecision.FORWARD);

            harness.proofSubjects.arm(harness.proofSubjects.create(), harness.key);
            successorPermit.forwarded();
            ProofResult result = execution.result();

            assertThat(invalidated.state()).isEqualTo(SemanticPredecessorGuardState.FAILED);
            assertThat(satisfied.state()).isEqualTo(SemanticPredecessorGuardState.SATISFIED);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            assertThat(result.primaryFailure()).isEmpty();
            assertThat(resolution(result, "invalidated").provenance()).hasSize(2);
            assertThat(resolution(result, "satisfied").provenance()).hasSize(2);
            assertFrozen(execution, result);
            return projections(result, "invalidated", "satisfied");
        }
    }

    private static AssociatedProjection associatedTerminalResult(
        AssociatedTerminal terminal
    ) throws Exception {
        MultipleControlTimeoutScheduler timeouts = new MultipleControlTimeoutScheduler();
        try (ProofRuntimeHarness harness = terminal == AssociatedTerminal.TIMEOUT
            ? ProofRuntimeHarness.startWithControlTimeoutScheduler(timeouts)
            : ProofRuntimeHarness.start()) {
            SemanticHold hold = harness.declareHold("predecessor");
            SemanticPredecessorGuard guard = harness.declareForwardedGuard();
            ProofExecution execution = harness.activate(basePlan(
                harness,
                "associated-terminal"
            ).control(
                "hold",
                hold,
                SemanticHoldState.FORWARDED
            ).control(
                "guard",
                guard,
                SemanticPredecessorGuardState.SATISFIED
            ).build());
            ForwardingPermit permit = permit(harness, "predecessor");

            switch (terminal) {
                case WRITE_FAILED -> {
                    hold.release();
                    assertThat(await(permit)).isEqualTo(ForwardingDecision.FORWARD);
                    permit.writeFailed();
                }
                case ABANDONED -> {
                    hold.release();
                    assertThat(await(permit)).isEqualTo(ForwardingDecision.FORWARD);
                    permit.abandoned();
                }
                case TIMEOUT -> {
                    timeouts.fireLast();
                    assertThat(await(permit)).isEqualTo(ForwardingDecision.CLOSE_SESSION);
                }
                case CANCELLED -> {
                    assertThat(hold.cancel()).isTrue();
                    assertThat(await(permit)).isEqualTo(ForwardingDecision.CLOSE_SESSION);
                }
            }
            ProofResult result = execution.result();
            assertThat(hold.state()).isEqualTo(terminal.holdState);
            assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.FAILED);
            assertFrozen(execution, result);
            return new AssociatedProjection(
                result.outcome(),
                resolution(result, "hold"),
                resolution(result, "guard")
            );
        }
    }

    private static ProofPlan singleGuardPlan(
        ProofRuntimeHarness harness,
        String id,
        SemanticPredecessorGuard guard
    ) {
        return basePlan(harness, id)
            .control("guard-control", guard, SemanticPredecessorGuardState.SATISFIED)
            .build();
    }

    private static ProofPlan twoGuardPlan(
        ProofRuntimeHarness harness,
        String id,
        SemanticPredecessorGuard first,
        SemanticPredecessorGuard second
    ) {
        return basePlan(harness, id)
            .control("first", first, SemanticPredecessorGuardState.SATISFIED)
            .control("second", second, SemanticPredecessorGuardState.SATISFIED)
            .build();
    }

    private static ProofPlan.Builder basePlan(ProofRuntimeHarness harness, String id) {
        return ProofPlan.builder(id, "Authoritative semantic control batch", harness.subject, DEADLINE)
            .prerequisite("prerequisite", harness.prerequisite())
            .observation("observation", harness.connectionId, ProofTestFixture.PROFILE);
    }

    private static ForwardingPermit permit(ProofRuntimeHarness harness, String value) {
        ProofRuntimeHarness.Recorded recorded = harness.record(value);
        harness.correlate(recorded, value);
        return harness.route.coordinator().permit(recorded.interaction());
    }

    private static ForwardingDecision await(ForwardingPermit permit) {
        try {
            return permit.awaitDecision();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while awaiting forwarding decision", interrupted);
        }
    }

    private static void assertFrozen(ProofExecution execution, ProofResult result) {
        assertThat(result.resolutions()).allSatisfy(value -> {
            assertThat(value.resolution()).isNotNull();
            assertThat(value.reason()).isNotNull();
            assertThat(value.provenance()).doesNotHaveDuplicates();
        });
        ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
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
            .orElseThrow(() -> new AssertionError("Missing proof resolution " + id));
    }

    private static Map<String, ResolutionProjection> projections(
        ProofResult result,
        String first,
        String second
    ) {
        return Map.of(
            first,
            ResolutionProjection.of(resolution(result, first)),
            second,
            ResolutionProjection.of(resolution(result, second))
        );
    }

    private record ResolutionProjection(
        ProofResolution resolution,
        ProofResolutionReason reason,
        List<ProofInteractionProvenance> provenance
    ) {
        private ResolutionProjection {
            provenance = List.copyOf(provenance);
        }

        private static ResolutionProjection of(ProofObligationResolution resolution) {
            return new ResolutionProjection(
                resolution.resolution(),
                resolution.reason(),
                resolution.provenance()
            );
        }
    }

    private record AssociatedProjection(
        ProofOutcome outcome,
        ProofObligationResolution hold,
        ProofObligationResolution guard
    ) {}

    private enum AssociatedTerminal {
        WRITE_FAILED(
            ProofOutcome.ERROR,
            SemanticHoldState.FAILED,
            ProofResolution.FAILED,
            ProofResolutionReason.CONTROL_FAILED,
            ProofResolution.FAILED,
            ProofResolutionReason.CONTROL_FAILED
        ),
        ABANDONED(
            ProofOutcome.INCONCLUSIVE,
            SemanticHoldState.FAILED,
            ProofResolution.MISSING,
            ProofResolutionReason.CONTROL_SESSION_ENDED,
            ProofResolution.MISSING,
            ProofResolutionReason.CONTROL_SESSION_ENDED
        ),
        TIMEOUT(
            ProofOutcome.INCONCLUSIVE,
            SemanticHoldState.TIMED_OUT,
            ProofResolution.TIMED_OUT,
            ProofResolutionReason.CONTROL_TIMED_OUT,
            ProofResolution.MISSING,
            ProofResolutionReason.CONTROL_SESSION_ENDED
        ),
        CANCELLED(
            ProofOutcome.INCONCLUSIVE,
            SemanticHoldState.CANCELLED,
            ProofResolution.UNREACHED,
            ProofResolutionReason.CONTROL_UNREACHED,
            ProofResolution.MISSING,
            ProofResolutionReason.CONTROL_SESSION_ENDED
        );

        private final ProofOutcome outcome;
        private final SemanticHoldState holdState;
        private final ProofResolution holdResolution;
        private final ProofResolutionReason holdReason;
        private final ProofResolution guardResolution;
        private final ProofResolutionReason guardReason;

        AssociatedTerminal(
            ProofOutcome outcome,
            SemanticHoldState holdState,
            ProofResolution holdResolution,
            ProofResolutionReason holdReason,
            ProofResolution guardResolution,
            ProofResolutionReason guardReason
        ) {
            this.outcome = outcome;
            this.holdState = holdState;
            this.holdResolution = holdResolution;
            this.holdReason = holdReason;
            this.guardResolution = guardResolution;
            this.guardReason = guardReason;
        }
    }

    private static final class MultipleControlTimeoutScheduler
        implements SemanticControlCoordinator.TimeoutScheduler {
        private final List<ScheduledTimeout> scheduled = new ArrayList<>();

        @Override
        public SemanticControlCoordinator.TimeoutTask schedule(
            Duration delay,
            Runnable action
        ) {
            ScheduledTimeout timeout = new ScheduledTimeout(action);
            scheduled.add(timeout);
            return () -> timeout.active = false;
        }

        private void fireLast() {
            for (int index = scheduled.size() - 1; index >= 0; index--) {
                ScheduledTimeout timeout = scheduled.get(index);
                if (timeout.active) {
                    timeout.active = false;
                    timeout.action.run();
                    return;
                }
            }
            throw new AssertionError("No active control timeout was scheduled");
        }

        @Override
        public void close() {
            scheduled.forEach(timeout -> timeout.active = false);
        }

        private static final class ScheduledTimeout {
            private final Runnable action;
            private boolean active = true;

            private ScheduledTimeout(Runnable action) {
                this.action = action;
            }
        }
    }

    private static final class SelectorFailure extends RuntimeException {
        private SelectorFailure(String message) {
            super(message);
        }
    }
}
