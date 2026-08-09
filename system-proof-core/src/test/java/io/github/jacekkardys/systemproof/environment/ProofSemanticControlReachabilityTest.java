package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldFailure;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorBoundary;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorViolation;
import io.github.jacekkardys.systemproof.journal.SemanticHoldEvent;
import io.github.jacekkardys.systemproof.journal.SemanticPredecessorGuardEvent;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.proof.ProofEvidenceKind;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofExecutionState;
import io.github.jacekkardys.systemproof.proof.ProofFailureStage;
import io.github.jacekkardys.systemproof.proof.ProofInteractionProvenance;
import io.github.jacekkardys.systemproof.proof.ProofObligationResolution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

class ProofSemanticControlReachabilityTest {

    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final Set<String> GUARD_IDS = Set.of(
        "prerequisite",
        "observation",
        "guard-control",
        "predecessor-evidence",
        "successor-evidence",
        "causal-relation"
    );
    private static final Set<String> HOLD_IDS = Set.of(
        "prerequisite",
        "observation",
        "hold-a-control",
        "hold-a-evidence",
        "hold-b-control",
        "hold-b-evidence"
    );

    @Test
    void shouldTreatOneConfirmedInteractionMatchingIdenticalGuardSelectorsAsEarlySuccessor()
        throws Exception {
        assertEarlySuccessor(
            SemanticPredecessorBoundary.CONFIRMED,
            "shared"::equals,
            "shared"::equals,
            "shared"
        );
    }

    @Test
    void shouldTreatOneConfirmedInteractionMatchingOverlappingGuardSelectorsAsEarlySuccessor()
        throws Exception {
        assertEarlySuccessor(
            SemanticPredecessorBoundary.CONFIRMED,
            value -> value.startsWith("shared"),
            value -> value.endsWith("interaction"),
            "shared-interaction"
        );
    }

    @Test
    void shouldTreatOneForwardedInteractionMatchingBothGuardRolesAsEarlySuccessor()
        throws Exception {
        assertEarlySuccessor(
            SemanticPredecessorBoundary.FORWARDED,
            "shared"::equals,
            "shared"::equals,
            "shared"
        );
    }

    @Test
    void shouldProveDistinctPredecessorAndSuccessorOnTheSameConnection() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareGuard(
                SemanticPredecessorBoundary.CONFIRMED,
                "predecessor"::equals,
                "successor"::equals
            );
            ProofExecution execution = harness.activate(guardPlan(harness, guard));

            InteractionDecision predecessor = decide(harness, "predecessor");
            InteractionDecision successor = decide(harness, "successor");

            assertThat(predecessor.decision()).isEqualTo(ForwardingDecision.FORWARD);
            assertThat(successor.decision()).isEqualTo(ForwardingDecision.FORWARD);
            assertThat(predecessor.interaction()).isNotEqualTo(successor.interaction());
            execution.runStimulus(() -> {});
            ProofResult result = execution.evaluate();

            assertThat(result.outcome()).isEqualTo(ProofOutcome.PROVED);
            assertThat(execution.result()).isSameAs(result);
            assertCompleteResult(execution, result, GUARD_IDS);
            assertEstablishedProvenance(
                resolution(result, "guard-control"),
                predecessor.interaction(),
                successor.interaction()
            );
            assertEstablishedProvenance(
                resolution(result, "causal-relation"),
                predecessor.interaction(),
                successor.interaction()
            );
            assertThat(result.resolutions()).allMatch(
                value -> value.resolution() == ProofResolution.SATISFIED
            );
        }
    }

    @Test
    void shouldRejectOneInteractionInBothRolesOfPublicGuardFact() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            InteractionRef interaction = harness.record("shared").interaction().interactionRef();

            assertThatThrownBy(() -> new SemanticPredecessorGuardEvent(
                guard.ref(),
                SemanticPredecessorGuardEvent.Kind.TERMINAL,
                harness.subject,
                SemanticPredecessorGuardState.VIOLATED,
                SemanticPredecessorBoundary.CONFIRMED,
                Optional.of(interaction),
                Optional.of(interaction),
                Optional.of(ForwardingDecision.CLOSE_SESSION),
                Optional.of(SemanticPredecessorViolation.PREDECESSOR_NOT_ESTABLISHED),
                Optional.empty()
            )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both roles");
        }
    }

    @Test
    void shouldFailHoldSelectorBeforeReachWithoutHeldProvenance() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticHold hold = harness.declareHold(value -> {
                throw new SelectorFailure();
            });
            ProofExecution execution = harness.activate(singleHoldPlan(harness, hold));

            InteractionDecision decision = decide(harness, "candidate");
            ProofResult result = execution.result();

            assertThat(decision.decision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).get().extracting(value -> value.stage())
                .isEqualTo(ProofFailureStage.CONTROL);
            assertThat(execution.evaluate()).isSameAs(result);
            assertCompleteResult(
                execution,
                result,
                Set.of("prerequisite", "observation", "hold-control", "hold-evidence")
            );
            assertResolution(
                result,
                "hold-control",
                ProofResolution.FAILED,
                ProofResolutionReason.CONTROL_SELECTOR_FAILED
            );
            assertResolution(
                result,
                "hold-evidence",
                ProofResolution.MISSING,
                ProofResolutionReason.EVIDENCE_MISSING
            );
            assertThat(hold.state()).isEqualTo(SemanticHoldState.FAILED);
            assertThat(hold.completion().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isEqualTo(SemanticHoldState.FAILED);
            assertExceptionalReached(hold);
            assertThat(terminalHoldEvents(harness, hold)).singleElement().satisfies(event -> {
                assertThat(event.failure()).contains(SemanticHoldFailure.SELECTOR_EVALUATION);
                assertThat(event.interactionRef()).isEmpty();
            });
        }
    }

    @RepeatedTest(20)
    void shouldAtomicallyFreezeAllAmbiguousHoldsInEitherDeclarationOrder() throws Exception {
        Map<String, ResolutionProjection> declaredForward = ambiguousHoldResult(false);
        Map<String, ResolutionProjection> declaredReverse = ambiguousHoldResult(true);

        assertThat(declaredReverse).isEqualTo(declaredForward);
    }

    @Test
    void shouldNeverLeaveCoordinatorOutcomeWithIncompleteOrExceptionalResultPublication()
        throws Exception {
        Map<String, ResolutionProjection> resolutions = ambiguousHoldResult(false);

        assertThat(resolutions.keySet()).containsExactlyInAnyOrderElementsOf(HOLD_IDS);
        assertThat(resolutions).allSatisfy((id, value) -> {
            assertThat(id).isNotBlank();
            assertThat(value.resolution()).isNotNull();
            assertThat(value.reason()).isNotNull();
        });
    }

    @Test
    void shouldKeepAuthoritativeHoldBatchAheadOfConcurrentIndependentTerminalFact()
        throws Exception {
        AtomicReference<ProofRuntimeHarness> runtime = new AtomicReference<>();
        AtomicReference<Thread> racer = new AtomicReference<>();
        AtomicBoolean started = new AtomicBoolean();
        CountDownLatch attempted = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void beforeProofFact(
                io.github.jacekkardys.systemproof.journal.ScenarioEvent event
            ) {
                if (!(event instanceof SemanticHoldEvent holdEvent)
                    || holdEvent.state() != SemanticHoldState.FAILED
                    || !started.compareAndSet(false, true)) {
                    return;
                }
                Thread thread = Thread.ofPlatform().daemon(true).unstarted(() -> {
                    attempted.countDown();
                    runtime.get().proofs.journalFailure(new ConcurrentJournalFailure());
                    completed.countDown();
                });
                racer.set(thread);
                thread.start();
                await(attempted, "concurrent proof fact attempt");
                assertThat(completed.getCount()).isEqualTo(1L);
            }
        };
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks)) {
            runtime.set(harness);
            SemanticHold holdA = harness.declareHold("shared");
            SemanticHold holdB = harness.declareHold("shared");
            ProofExecution execution = harness.activate(twoHoldPlan(harness, holdA, holdB));

            InteractionDecision interaction = decide(harness, "shared");

            await(completed, "concurrent proof fact completion");
            racer.get().join();
            ProofResult result = execution.result();
            assertThat(interaction.decision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            assertThat(execution.evaluate()).isSameAs(result);
            assertCompleteResult(execution, result, HOLD_IDS);
            assertResolution(
                result,
                "hold-a-control",
                ProofResolution.AMBIGUOUS,
                ProofResolutionReason.CONTROL_MATCH_AMBIGUOUS
            );
            assertResolution(
                result,
                "hold-b-control",
                ProofResolution.AMBIGUOUS,
                ProofResolutionReason.CONTROL_MATCH_AMBIGUOUS
            );
        }
    }

    private static void assertEarlySuccessor(
        SemanticPredecessorBoundary boundary,
        Predicate<String> predecessor,
        Predicate<String> successor,
        String value
    ) throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareGuard(
                boundary,
                predecessor,
                successor
            );
            ProofExecution execution = harness.activate(guardPlan(harness, guard));

            InteractionDecision interaction = decide(harness, value);
            ProofResult result = execution.result();

            assertThat(interaction.decision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.VIOLATED);
            assertThat(result.primaryFailure()).isEmpty();
            assertThat(execution.evaluate()).isSameAs(result);
            assertCompleteResult(execution, result, GUARD_IDS);
            assertSuccessorOnly(resolution(result, "guard-control"), interaction.interaction());
            assertSuccessorOnly(resolution(result, "causal-relation"), interaction.interaction());
            assertResolution(
                result,
                "predecessor-evidence",
                ProofResolution.MISSING,
                ProofResolutionReason.EVIDENCE_MISSING
            );
            ProofObligationResolution successorEvidence = resolution(
                result,
                "successor-evidence"
            );
            assertThat(successorEvidence.resolution()).isEqualTo(ProofResolution.SATISFIED);
            assertThat(successorEvidence.provenance()).containsExactly(
                ProofInteractionProvenance.successor(interaction.interaction())
            );
        }
    }

    private static Map<String, ResolutionProjection> ambiguousHoldResult(
        boolean reverseDeclarationOrder
    ) throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticHold holdA;
            SemanticHold holdB;
            if (reverseDeclarationOrder) {
                holdB = harness.declareHold("shared");
                holdA = harness.declareHold("shared");
            } else {
                holdA = harness.declareHold("shared");
                holdB = harness.declareHold("shared");
            }
            ProofExecution execution = harness.activate(twoHoldPlan(harness, holdA, holdB));

            InteractionDecision interaction = decide(harness, "shared");
            ProofResult result = execution.result();

            assertThat(interaction.decision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            assertThat(holdA.state()).isEqualTo(SemanticHoldState.FAILED);
            assertThat(holdB.state()).isEqualTo(SemanticHoldState.FAILED);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            assertThat(result.primaryFailure()).isEmpty();
            assertThat(execution.evaluate()).isSameAs(result);
            assertCompleteResult(execution, result, HOLD_IDS);
            for (String id : List.of("hold-a-control", "hold-b-control")) {
                assertResolution(
                    result,
                    id,
                    ProofResolution.AMBIGUOUS,
                    ProofResolutionReason.CONTROL_MATCH_AMBIGUOUS
                );
            }
            for (String id : List.of("hold-a-evidence", "hold-b-evidence")) {
                assertResolution(
                    result,
                    id,
                    ProofResolution.MISSING,
                    ProofResolutionReason.EVIDENCE_MISSING
                );
            }
            assertExceptionalReached(holdA);
            assertExceptionalReached(holdB);
            assertThat(terminalHoldEvents(harness, holdA)).singleElement().satisfies(event -> {
                assertThat(event.failure()).contains(SemanticHoldFailure.AMBIGUOUS_MATCH);
                assertThat(event.interactionRef()).isEmpty();
            });
            assertThat(terminalHoldEvents(harness, holdB)).singleElement().satisfies(event -> {
                assertThat(event.failure()).contains(SemanticHoldFailure.AMBIGUOUS_MATCH);
                assertThat(event.interactionRef()).isEmpty();
            });
            return resolutionMapping(result);
        }
    }

    private static ProofPlan guardPlan(
        ProofRuntimeHarness harness,
        SemanticPredecessorGuard guard
    ) {
        return basePlan(harness, "guard-reachability")
            .control("guard-control", guard, SemanticPredecessorGuardState.SATISFIED)
            .evidence("predecessor-evidence", guard, ProofEvidenceKind.PREDECESSOR_INTERACTION)
            .evidence("successor-evidence", guard, ProofEvidenceKind.SUCCESSOR_INTERACTION)
            .causalRelation("causal-relation", guard)
            .build();
    }

    private static ProofPlan singleHoldPlan(ProofRuntimeHarness harness, SemanticHold hold) {
        return basePlan(harness, "hold-selector-failure")
            .control("hold-control", hold, SemanticHoldState.FORWARDED)
            .evidence("hold-evidence", hold)
            .build();
    }

    private static ProofPlan twoHoldPlan(
        ProofRuntimeHarness harness,
        SemanticHold holdA,
        SemanticHold holdB
    ) {
        return basePlan(harness, "ambiguous-hold-batch")
            .control("hold-a-control", holdA, SemanticHoldState.FORWARDED)
            .evidence("hold-a-evidence", holdA)
            .control("hold-b-control", holdB, SemanticHoldState.FORWARDED)
            .evidence("hold-b-evidence", holdB)
            .build();
    }

    private static ProofPlan.Builder basePlan(ProofRuntimeHarness harness, String id) {
        return ProofPlan.builder(id, "Semantic control reachability", harness.subject, DEADLINE)
            .prerequisite("prerequisite", harness.prerequisite())
            .observation("observation", harness.connectionId, ProofTestFixture.PROFILE);
    }

    private static InteractionDecision decide(ProofRuntimeHarness harness, String value)
        throws Exception {
        ProofRuntimeHarness.Recorded recorded = harness.record(value);
        harness.correlate(recorded, value);
        ForwardingPermit permit = harness.route.coordinator().permit(recorded.interaction());
        ForwardingDecision decision = permit.awaitDecision();
        if (decision == ForwardingDecision.FORWARD) {
            permit.forwarded();
        }
        return new InteractionDecision(recorded.interaction().interactionRef(), decision);
    }

    private static void assertCompleteResult(
        ProofExecution execution,
        ProofResult result,
        Set<String> expectedIds
    ) {
        assertThat(execution.state()).isEqualTo(ProofExecutionState.COMPLETED);
        assertThat(result.resolutions()).extracting(value -> value.id().value())
            .containsExactlyInAnyOrderElementsOf(expectedIds);
        assertThat(result.resolutions()).allSatisfy(value -> {
            assertThat(value.resolution()).isNotNull();
            assertThat(value.reason()).isNotNull();
            assertThat(value.provenance()).doesNotHaveDuplicates();
        });
    }

    private static void assertEstablishedProvenance(
        ProofObligationResolution resolution,
        InteractionRef predecessor,
        InteractionRef successor
    ) {
        assertThat(resolution.provenance()).containsExactly(
            ProofInteractionProvenance.predecessor(predecessor),
            ProofInteractionProvenance.successor(successor)
        );
    }

    private static void assertSuccessorOnly(
        ProofObligationResolution resolution,
        InteractionRef successor
    ) {
        assertThat(resolution.resolution()).isEqualTo(ProofResolution.VIOLATED);
        assertThat(resolution.reason())
            .isEqualTo(ProofResolutionReason.CAUSAL_RELATION_VIOLATED);
        assertThat(resolution.provenance()).containsExactly(
            ProofInteractionProvenance.successor(successor)
        );
        assertThat(resolution.connectionId()).contains(successor.connectionId());
    }

    private static void assertResolution(
        ProofResult result,
        String id,
        ProofResolution resolution,
        ProofResolutionReason reason
    ) {
        ProofObligationResolution value = resolution(result, id);
        assertThat(value.resolution()).isEqualTo(resolution);
        assertThat(value.reason()).isEqualTo(reason);
        assertThat(value.provenance()).isEmpty();
    }

    private static ProofObligationResolution resolution(ProofResult result, String id) {
        return result.resolutions().stream()
            .filter(value -> value.id().value().equals(id))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Missing proof resolution " + id));
    }

    private static void assertExceptionalReached(SemanticHold hold) {
        assertThatThrownBy(() -> hold.reached().toCompletableFuture().get(5, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(IllegalStateException.class);
    }

    private static List<SemanticHoldEvent> terminalHoldEvents(
        ProofRuntimeHarness harness,
        SemanticHold hold
    ) {
        return harness.journal.snapshot().entries().stream()
            .map(entry -> entry.event())
            .filter(SemanticHoldEvent.class::isInstance)
            .map(SemanticHoldEvent.class::cast)
            .filter(event -> event.holdRef().equals(hold.ref()))
            .filter(event -> event.state() == SemanticHoldState.FAILED)
            .toList();
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

    private static Map<String, ResolutionProjection> resolutionMapping(ProofResult result) {
        Map<String, ResolutionProjection> mapping = new LinkedHashMap<>();
        for (ProofObligationResolution value : result.resolutions()) {
            mapping.put(value.id().value(), new ResolutionProjection(
                value.resolution(),
                value.reason(),
                value.connectionId(),
                value.provenance()
            ));
        }
        return Map.copyOf(mapping);
    }

    private record InteractionDecision(
        InteractionRef interaction,
        ForwardingDecision decision
    ) {}

    private record ResolutionProjection(
        ProofResolution resolution,
        ProofResolutionReason reason,
        Optional<ConnectionId> connectionId,
        List<ProofInteractionProvenance> provenance
    ) {
        private ResolutionProjection {
            provenance = List.copyOf(provenance);
        }
    }

    private static final class SelectorFailure extends RuntimeException {}

    private static final class ConcurrentJournalFailure extends RuntimeException {}
}
