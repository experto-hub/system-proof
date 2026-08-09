package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.junit.jupiter.api.RepeatedTest;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticInteractionSelector;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardSpec;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorRequirement;
import io.github.jacekkardys.systemproof.journal.SemanticPredecessorGuardEvent;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.ProofEvidenceKind;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofInteractionProvenance;
import io.github.jacekkardys.systemproof.proof.ProofObligationResolution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;

class ProofPermitCorrelationBoundaryTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final String FLOW = "permit-boundary-flow";

    @RepeatedTest(20)
    void shouldLinearizeMissingToUniqueGuardSelectionInBothOrders() throws Exception {
        assertMissingToUniqueGuard(true);
        assertMissingToUniqueGuard(false);
    }

    @RepeatedTest(20)
    void shouldLinearizeMissingToUniqueHoldSelectionInBothOrders() throws Exception {
        assertMissingToUniqueHold(true);
        assertMissingToUniqueHold(false);
    }

    @RepeatedTest(20)
    void shouldLinearizeUniqueGuardSelectionAgainstArmAndCandidateInBothOrders()
        throws Exception {
        for (Invalidation invalidation : Invalidation.values()) {
            assertUniqueToAmbiguousGuard(invalidation, true);
            assertUniqueToAmbiguousGuard(invalidation, false);
        }
    }

    @RepeatedTest(20)
    void shouldLinearizeUniqueHoldSelectionAgainstArmAndCandidateInBothOrders()
        throws Exception {
        for (Invalidation invalidation : Invalidation.values()) {
            assertUniqueToAmbiguousHold(invalidation, true);
            assertUniqueToAmbiguousHold(invalidation, false);
        }
    }

    @RepeatedTest(20)
    void shouldNotEstablishAConfirmedPredecessorFromInvalidatedCorrelation()
        throws Exception {
        assertConfirmedPredecessorInvalidation(true);
        assertConfirmedPredecessorInvalidation(false);
    }

    private static void assertMissingToUniqueGuard(boolean correlationFirst)
        throws Exception {
        PermitBoundaryHooks hooks = new PermitBoundaryHooks();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks)) {
            SemanticPredecessorGuard guard = harness.declareGuard(
                io.github.jacekkardys.systemproof.control.SemanticPredecessorBoundary.CONFIRMED,
                value -> false,
                "successor"::equals
            );
            ProofExecution execution = harness.activate(guardPlan(harness, guard));
            ProofRuntimeHarness.Recorded successor = harness.record("successor");

            ForwardingPermit permit = racePermit(
                hooks,
                () -> harness.route.coordinator().permit(successor.interaction()),
                () -> harness.correlate(successor, harness.successorKey, "successor"),
                correlationFirst
            );

            ProofResult result;
            if (correlationFirst) {
                assertThat(awaitDecision(permit)).isEqualTo(ForwardingDecision.CLOSE_SESSION);
                assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
                assertNoForwardDecision(harness, guard);
                result = execution.result();
                assertThat(result.outcome()).isEqualTo(ProofOutcome.VIOLATED);
                assertResolution(
                    result,
                    "guard-control",
                    ProofResolution.VIOLATED,
                    ProofResolutionReason.CAUSAL_RELATION_VIOLATED
                );
                assertSuccessorOnly(result, successor);
            } else {
                assertThat(awaitDecision(permit)).isEqualTo(ForwardingDecision.FORWARD);
                permit.forwarded();
                assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.ARMED);
                assertThat(guard.cancel()).isTrue();
                execution.runStimulus(() -> {});
                result = execution.evaluate();
                assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
                assertResolution(
                    result,
                    "guard-control",
                    ProofResolution.UNREACHED,
                    ProofResolutionReason.CONTROL_UNREACHED
                );
            }
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertNoDeadlockedThread();
        }
    }

    private static void assertMissingToUniqueHold(boolean correlationFirst)
        throws Exception {
        PermitBoundaryHooks hooks = new PermitBoundaryHooks();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks)) {
            SemanticHold hold = harness.declareHold("held");
            ProofExecution execution = harness.activate(holdPlan(harness, hold));
            ProofRuntimeHarness.Recorded held = harness.record("held");

            ForwardingPermit permit = racePermit(
                hooks,
                () -> harness.route.coordinator().permit(held.interaction()),
                () -> harness.correlate(held, harness.key, "held"),
                correlationFirst
            );

            ProofResult result;
            if (correlationFirst) {
                assertThat(hold.reached().toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .isEqualTo(held.interaction().interactionRef());
                assertThat(hold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
                CompletionStage<Void> release = hold.release();
                assertThat(awaitDecision(permit)).isEqualTo(ForwardingDecision.FORWARD);
                permit.forwarded();
                assertThat(release.toCompletableFuture().get(5, TimeUnit.SECONDS)).isNull();
                execution.runStimulus(() -> {});
                result = execution.evaluate();
                assertThat(result.outcome()).isEqualTo(ProofOutcome.PROVED);
                assertResolution(
                    result,
                    "hold-control",
                    ProofResolution.SATISFIED,
                    ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE
                );
            } else {
                assertThat(awaitDecision(permit)).isEqualTo(ForwardingDecision.FORWARD);
                permit.forwarded();
                assertThat(hold.state()).isEqualTo(SemanticHoldState.ARMED);
                assertThat(hold.cancel()).isTrue();
                execution.runStimulus(() -> {});
                result = execution.evaluate();
                assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
                assertResolution(
                    result,
                    "hold-control",
                    ProofResolution.UNREACHED,
                    ProofResolutionReason.CONTROL_UNREACHED
                );
            }
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertNoDeadlockedThread();
        }
    }

    private static void assertUniqueToAmbiguousGuard(
        Invalidation invalidation,
        boolean invalidationFirst
    ) throws Exception {
        PermitBoundaryHooks hooks = new PermitBoundaryHooks();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks)) {
            SemanticPredecessorGuard guard = harness.declareNativeFlowGuard(
                harness.key,
                value -> false,
                "successor"::equals,
                FLOW
            );
            ProofExecution execution = harness.activate(guardPlan(harness, guard));
            ProofRuntimeHarness.Recorded successor = harness.record("successor");
            harness.correlate(successor, harness.key, FLOW);
            Runnable mutation = invalidation(harness, invalidation, harness.key);

            ForwardingPermit permit = racePermit(
                hooks,
                () -> harness.route.coordinator().permit(successor.interaction()),
                mutation,
                invalidationFirst
            );

            ProofResult result;
            if (invalidationFirst) {
                assertThat(awaitDecision(permit)).isEqualTo(ForwardingDecision.FORWARD);
                permit.forwarded();
                assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.ARMED);
                assertThat(guard.cancel()).isTrue();
                execution.runStimulus(() -> {});
                result = execution.evaluate();
                assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
                assertResolution(
                    result,
                    "guard-control",
                    ProofResolution.UNREACHED,
                    ProofResolutionReason.CONTROL_UNREACHED
                );
            } else {
                assertThat(awaitDecision(permit)).isEqualTo(ForwardingDecision.CLOSE_SESSION);
                assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
                assertNoForwardDecision(harness, guard);
                result = execution.result();
                assertThat(result.outcome()).isEqualTo(ProofOutcome.VIOLATED);
                assertSuccessorOnly(result, successor);
            }
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertNoDeadlockedThread();
        }
    }

    private static void assertUniqueToAmbiguousHold(
        Invalidation invalidation,
        boolean invalidationFirst
    ) throws Exception {
        PermitBoundaryHooks hooks = new PermitBoundaryHooks();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks)) {
            SemanticHold hold = harness.declareNativeFlowHold(
                harness.key,
                "held"::equals,
                FLOW
            );
            ProofExecution execution = harness.activate(holdPlan(harness, hold));
            ProofRuntimeHarness.Recorded held = harness.record("held");
            harness.correlate(held, harness.key, FLOW);
            Runnable mutation = invalidation(harness, invalidation, harness.key);

            ForwardingPermit permit = racePermit(
                hooks,
                () -> harness.route.coordinator().permit(held.interaction()),
                mutation,
                invalidationFirst
            );

            ProofResult result;
            if (invalidationFirst) {
                assertThat(awaitDecision(permit)).isEqualTo(ForwardingDecision.FORWARD);
                permit.forwarded();
                assertThat(hold.state()).isEqualTo(SemanticHoldState.ARMED);
                assertThat(hold.cancel()).isTrue();
                execution.runStimulus(() -> {});
                result = execution.evaluate();
                assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
                assertResolution(
                    result,
                    "hold-control",
                    ProofResolution.UNREACHED,
                    ProofResolutionReason.CONTROL_UNREACHED
                );
            } else {
                assertThat(hold.reached().toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .isEqualTo(held.interaction().interactionRef());
                CompletionStage<Void> release = hold.release();
                assertThat(awaitDecision(permit)).isEqualTo(ForwardingDecision.CLOSE_SESSION);
                assertThatThrownBy(release.toCompletableFuture()::join)
                    .isInstanceOf(CompletionException.class);
                result = execution.result();
                assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
                assertResolution(
                    result,
                    "hold-control",
                    ProofResolution.AMBIGUOUS,
                    ProofResolutionReason.CONTROL_CORRELATION_INVALIDATED
                );
                assertThat(resolution(result, "hold-evidence").provenance())
                    .singleElement().satisfies(value ->
                        assertThat(value.role()).isEqualTo(
                            ProofInteractionProvenance.Role.HOLD
                        )
                    );
            }
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertNoDeadlockedThread();
        }
    }

    private static void assertConfirmedPredecessorInvalidation(boolean invalidationFirst)
        throws Exception {
        PermitBoundaryHooks hooks = new PermitBoundaryHooks();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks)) {
            SemanticPredecessorGuard guard = harness.controls.declareGuard(
                SemanticPredecessorGuardSpec.requiring(
                    harness.subject,
                    SemanticPredecessorRequirement.confirmed(nativeSelector(
                        harness,
                        harness.key,
                        "predecessor"::equals,
                        FLOW
                    )),
                    nativeSelector(
                        harness,
                        harness.successorKey,
                        "successor"::equals,
                        FLOW
                    ),
                    DEADLINE
                )
            );
            ProofExecution execution = harness.activate(guardPlan(harness, guard));
            ProofRuntimeHarness.Recorded predecessor = harness.record("predecessor");
            harness.correlate(predecessor, harness.key, FLOW);
            ProofRuntimeHarness.Recorded competing = harness.record("competing-predecessor");

            ForwardingPermit predecessorPermit = racePermit(
                hooks,
                () -> harness.route.coordinator().permit(predecessor.interaction()),
                () -> harness.correlate(competing, harness.key, FLOW),
                invalidationFirst
            );
            assertThat(awaitDecision(predecessorPermit)).isEqualTo(ForwardingDecision.FORWARD);
            predecessorPermit.forwarded();

            if (invalidationFirst) {
                assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.ARMED);
            } else {
                assertThat(guard.state())
                    .isEqualTo(SemanticPredecessorGuardState.PREDECESSOR_SATISFIED);
            }

            ProofRuntimeHarness.Recorded successor = harness.record("successor");
            harness.correlate(successor, harness.successorKey, FLOW);
            ForwardingPermit successorPermit = harness.route.coordinator()
                .permit(successor.interaction());
            ProofResult result;
            if (invalidationFirst) {
                assertThat(awaitDecision(successorPermit))
                    .isEqualTo(ForwardingDecision.CLOSE_SESSION);
                assertNoForwardDecision(harness, guard);
                result = execution.result();
                assertThat(result.outcome()).isEqualTo(ProofOutcome.VIOLATED);
                assertSuccessorOnly(result, successor);
            } else {
                assertThat(awaitDecision(successorPermit))
                    .isEqualTo(ForwardingDecision.FORWARD);
                successorPermit.forwarded();
                execution.runStimulus(() -> {});
                result = execution.evaluate();
                assertThat(result.outcome()).isEqualTo(ProofOutcome.PROVED);
                assertThat(resolution(result, "guard-control").provenance())
                    .extracting(ProofInteractionProvenance::role)
                    .containsExactly(
                        ProofInteractionProvenance.Role.PREDECESSOR,
                        ProofInteractionProvenance.Role.SUCCESSOR
                    );
            }
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertNoDeadlockedThread();
        }
    }

    private static ForwardingPermit racePermit(
        PermitBoundaryHooks hooks,
        Callable<ForwardingPermit> permit,
        Runnable mutation,
        boolean mutationFirst
    ) throws Exception {
        return mutationFirst
            ? mutationFirst(hooks, permit, mutation)
            : permitFirst(permit, mutation);
    }

    private static ForwardingPermit mutationFirst(
        PermitBoundaryHooks hooks,
        Callable<ForwardingPermit> permit,
        Runnable mutation
    ) throws Exception {
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            hooks.enabled.set(true);
            Future<ForwardingPermit> permitting = executor.submit(() -> {
                hooks.permitThread.set(Thread.currentThread());
                return permit.call();
            });
            await(hooks.permitBoundaryAttempted, "permit boundary attempt");
            Future<?> mutating = executor.submit(mutation);
            try {
                mutating.get(5, TimeUnit.SECONDS);
            } finally {
                hooks.allowPermit.countDown();
            }
            ForwardingPermit result = permitting.get(5, TimeUnit.SECONDS);
            hooks.enabled.set(false);
            return result;
        }
    }

    private static ForwardingPermit permitFirst(
        Callable<ForwardingPermit> permit,
        Runnable mutation
    ) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch permitCompleted = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<ForwardingPermit> permitting = executor.submit(() -> {
                await(start, "permit-first race start");
                try {
                    return permit.call();
                } finally {
                    permitCompleted.countDown();
                }
            });
            Future<?> mutating = executor.submit(() -> {
                await(start, "permit-first race start");
                await(permitCompleted, "permit operation completion");
                mutation.run();
            });
            start.countDown();
            ForwardingPermit result = permitting.get(5, TimeUnit.SECONDS);
            mutating.get(5, TimeUnit.SECONDS);
            return result;
        }
    }

    private static Runnable invalidation(
        ProofRuntimeHarness harness,
        Invalidation invalidation,
        CorrelationKey key
    ) {
        return switch (invalidation) {
            case ARM -> {
                ProofSubjectRef competingSubject = harness.proofSubjects.create();
                yield () -> harness.proofSubjects.arm(competingSubject, key);
            }
            case CANDIDATE -> {
                ProofRuntimeHarness.Recorded competing = harness.record("competing");
                yield () -> harness.correlate(competing, key, FLOW);
            }
        };
    }

    private static SemanticInteractionSelector<String> nativeSelector(
        ProofRuntimeHarness harness,
        CorrelationKey key,
        Predicate<String> predicate,
        String nativeReference
    ) {
        return SemanticInteractionSelector.matching(
            harness.connectionId,
            FlowDirection.CONSUMER_TO_PROVIDER,
            ProofTestFixture.TextCodec.INSTANCE,
            predicate
        ).forSubject(harness.subject).through(
            key,
            ProofTestFixture.NativeCodec.INSTANCE,
            ignored -> nativeReference
        );
    }

    private static ProofPlan guardPlan(
        ProofRuntimeHarness harness,
        SemanticPredecessorGuard guard
    ) {
        return basePlan(harness, "permit-guard")
            .control("guard-control", guard, SemanticPredecessorGuardState.SATISFIED)
            .evidence(
                "predecessor-evidence",
                guard,
                ProofEvidenceKind.PREDECESSOR_INTERACTION
            ).evidence(
                "successor-evidence",
                guard,
                ProofEvidenceKind.SUCCESSOR_INTERACTION
            ).causalRelation("causal-relation", guard)
            .build();
    }

    private static ProofPlan holdPlan(
        ProofRuntimeHarness harness,
        SemanticHold hold
    ) {
        return basePlan(harness, "permit-hold")
            .control("hold-control", hold, SemanticHoldState.FORWARDED)
            .evidence("hold-evidence", hold)
            .build();
    }

    private static ProofPlan.Builder basePlan(
        ProofRuntimeHarness harness,
        String id
    ) {
        return ProofPlan.builder(id, "Permit correlation boundary", harness.subject, DEADLINE)
            .prerequisite("prerequisite", harness.prerequisite())
            .observation("observation", harness.connectionId, ProofTestFixture.PROFILE);
    }

    private static void assertSuccessorOnly(
        ProofResult result,
        ProofRuntimeHarness.Recorded successor
    ) {
        assertThat(resolution(result, "guard-control").provenance())
            .singleElement().satisfies(value -> {
                assertThat(value.role())
                    .isEqualTo(ProofInteractionProvenance.Role.SUCCESSOR);
                assertThat(value.interaction())
                    .isEqualTo(successor.interaction().interactionRef());
            });
    }

    private static void assertNoForwardDecision(
        ProofRuntimeHarness harness,
        SemanticPredecessorGuard guard
    ) {
        assertThat(harness.journal.snapshot().entries())
            .extracting(value -> value.event())
            .filteredOn(SemanticPredecessorGuardEvent.class::isInstance)
            .map(SemanticPredecessorGuardEvent.class::cast)
            .filteredOn(value -> value.guardRef().equals(guard.ref()))
            .filteredOn(value -> value.decision()
                .filter(ForwardingDecision.FORWARD::equals).isPresent())
            .isEmpty();
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

    private static ForwardingDecision awaitDecision(ForwardingPermit permit) {
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            try {
                return executor.submit(permit::awaitDecision).get(5, TimeUnit.SECONDS);
            } catch (Exception failure) {
                throw new AssertionError("Forwarding decision did not complete", failure);
            }
        }
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

    private static void assertNoDeadlockedThread() {
        assertThat(ManagementFactory.getThreadMXBean().findDeadlockedThreads()).isNull();
    }

    private enum Invalidation {
        ARM,
        CANDIDATE
    }

    private static final class PermitBoundaryHooks
        implements ProofRuntimeHarness.BoundaryHooks {
        private final AtomicBoolean enabled = new AtomicBoolean();
        private final AtomicReference<Thread> permitThread = new AtomicReference<>();
        private final CountDownLatch permitBoundaryAttempted = new CountDownLatch(1);
        private final CountDownLatch allowPermit = new CountDownLatch(1);

        @Override
        public void beforeAuthoritativeOperationBoundary() {
            if (!enabled.get() || Thread.currentThread() != permitThread.get()) {
                return;
            }
            permitBoundaryAttempted.countDown();
            await(allowPermit, "permit boundary release");
        }
    }
}
