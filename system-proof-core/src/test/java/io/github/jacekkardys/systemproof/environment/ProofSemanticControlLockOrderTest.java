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
import org.junit.jupiter.api.RepeatedTest;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.journal.CorrelationCandidateEvent;
import io.github.jacekkardys.systemproof.journal.ProofSubjectArmedEvent;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.observation.RecordedInteraction;
import io.github.jacekkardys.systemproof.proof.ProofEvidenceKind;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofObligationResolution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;

class ProofSemanticControlLockOrderTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final String FLOW = "lock-order-flow";

    @RepeatedTest(20)
    void shouldLinearizeReachedHoldReleaseAgainstSubjectArmInBothOrders()
        throws Exception {
        assertHoldRace(Invalidation.ARM, true);
        assertHoldRace(Invalidation.ARM, false);
    }

    @RepeatedTest(20)
    void shouldLinearizeReachedHoldReleaseAgainstCandidatePublicationInBothOrders()
        throws Exception {
        assertHoldRace(Invalidation.CANDIDATE, true);
        assertHoldRace(Invalidation.CANDIDATE, false);
    }

    @RepeatedTest(20)
    void shouldLinearizePredecessorForwardedAgainstArmAndPublicationInBothOrders()
        throws Exception {
        for (Invalidation invalidation : Invalidation.values()) {
            assertGuardRace(GuardBoundary.PREDECESSOR, invalidation, true);
            assertGuardRace(GuardBoundary.PREDECESSOR, invalidation, false);
        }
    }

    @RepeatedTest(20)
    void shouldLinearizeSuccessorForwardedAgainstArmAndPublicationInBothOrders()
        throws Exception {
        for (Invalidation invalidation : Invalidation.values()) {
            assertGuardRace(GuardBoundary.SUCCESSOR, invalidation, true);
            assertGuardRace(GuardBoundary.SUCCESSOR, invalidation, false);
        }
    }

    private static void assertHoldRace(
        Invalidation invalidation,
        boolean invalidationFirst
    ) throws Exception {
        OldOrderHooks hooks = new OldOrderHooks(invalidation);
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks)) {
            SemanticHold hold = harness.declareNativeFlowHold(
                harness.key,
                "held"::equals,
                FLOW
            );
            ProofExecution execution = harness.activate(holdPlan(harness, hold));
            ProofRuntimeHarness.Recorded held = harness.record("held");
            harness.correlate(held, harness.key, FLOW);
            ForwardingPermit permit = harness.route.coordinator().permit(held.interaction());
            assertThat(hold.reached().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isEqualTo(held.interaction().interactionRef());
            Runnable publication = invalidationAction(harness, invalidation);

            CompletionStage<Void> release = invalidationFirst
                ? publicationFirst(hooks, hold::release, publication)
                : controlFirst(hold::release, publication);

            ProofResult result;
            if (invalidationFirst) {
                assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
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
            } else {
                assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
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
            }
            assertThat(resolution(result, "hold-evidence").provenance()).hasSize(1);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertNoDeadlockedThread();
        }
    }

    private static void assertGuardRace(
        GuardBoundary boundary,
        Invalidation invalidation,
        boolean invalidationFirst
    ) throws Exception {
        OldOrderHooks hooks = new OldOrderHooks(invalidation);
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks)) {
            SemanticPredecessorGuard guard = harness.declareNativeFlowGuard(
                harness.key,
                "predecessor"::equals,
                "successor"::equals,
                FLOW
            );
            ProofExecution execution = harness.activate(guardPlan(harness, guard));
            ProofRuntimeHarness.Recorded predecessor = harness.record("predecessor");
            harness.correlate(predecessor, harness.key, FLOW);
            ForwardingPermit predecessorPermit = harness.route.coordinator()
                .permit(predecessor.interaction());
            assertThat(predecessorPermit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);

            ForwardingPermit racedPermit;
            if (boundary == GuardBoundary.PREDECESSOR) {
                racedPermit = predecessorPermit;
            } else {
                predecessorPermit.forwarded();
                RecordedInteraction successor = predecessor.session().record(
                    FlowDirection.CONSUMER_TO_PROVIDER,
                    ProofTestFixture.TextCodec.INSTANCE,
                    "successor"
                );
                racedPermit = harness.route.coordinator().permit(successor);
                assertThat(racedPermit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
            }
            Runnable publication = invalidationAction(harness, invalidation);
            Callable<Void> forwarded = () -> {
                racedPermit.forwarded();
                return null;
            };

            if (invalidationFirst) {
                publicationFirst(hooks, forwarded, publication);
            } else {
                controlFirst(forwarded, publication);
            }

            ProofResult result;
            if (invalidationFirst) {
                result = execution.result();
                assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
                assertResolution(
                    result,
                    "guard-control",
                    ProofResolution.AMBIGUOUS,
                    ProofResolutionReason.CONTROL_CORRELATION_INVALIDATED
                );
            } else if (boundary == GuardBoundary.SUCCESSOR) {
                execution.runStimulus(() -> {});
                result = execution.evaluate();
                assertThat(result.outcome()).isEqualTo(ProofOutcome.PROVED);
                assertResolution(
                    result,
                    "guard-control",
                    ProofResolution.SATISFIED,
                    ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE
                );
            } else {
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
            assertThat(resolution(result, "predecessor-evidence").provenance()).hasSize(1);
            if (boundary == GuardBoundary.SUCCESSOR) {
                assertThat(resolution(result, "successor-evidence").provenance()).hasSize(1);
            }
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertNoDeadlockedThread();
        }
    }

    private static Runnable invalidationAction(
        ProofRuntimeHarness harness,
        Invalidation invalidation
    ) {
        return switch (invalidation) {
            case ARM -> {
                ProofSubjectRef competing = harness.proofSubjects.create();
                yield () -> harness.proofSubjects.arm(competing, harness.key);
            }
            case CANDIDATE -> {
                ProofRuntimeHarness.Recorded competing = harness.record("competing");
                yield () -> harness.correlate(competing, harness.key, FLOW);
            }
        };
    }

    private static <T> T publicationFirst(
        OldOrderHooks hooks,
        Callable<T> control,
        Runnable publication
    ) throws Exception {
        hooks.enabled.set(true);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> publishing = executor.submit(publication);
            await(hooks.publicationFactEntered, "subject publication while owning its monitor");
            Future<T> controlling = executor.submit(() -> {
                hooks.controlThread.set(Thread.currentThread());
                return control.call();
            });
            await(hooks.controlBoundaryAttempted, "semantic control boundary attempt");
            assertThat(controlling.isDone()).isFalse();
            hooks.allowPublication.countDown();
            publishing.get(5, TimeUnit.SECONDS);
            T result = controlling.get(5, TimeUnit.SECONDS);
            hooks.enabled.set(false);
            return result;
        }
    }

    private static <T> T controlFirst(Callable<T> control, Runnable publication)
        throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch controlCompleted = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<T> controlling = executor.submit(() -> {
                await(start, "ordered race start");
                try {
                    return control.call();
                } finally {
                    controlCompleted.countDown();
                }
            });
            Future<?> publishing = executor.submit(() -> {
                await(start, "ordered race start");
                await(controlCompleted, "control operation linearization");
                publication.run();
            });
            start.countDown();
            T result = controlling.get(5, TimeUnit.SECONDS);
            publishing.get(5, TimeUnit.SECONDS);
            return result;
        }
    }

    private static ProofPlan holdPlan(ProofRuntimeHarness harness, SemanticHold hold) {
        return basePlan(harness, "hold-lock-order")
            .control("hold-control", hold, SemanticHoldState.FORWARDED)
            .evidence("hold-evidence", hold)
            .build();
    }

    private static ProofPlan guardPlan(
        ProofRuntimeHarness harness,
        SemanticPredecessorGuard guard
    ) {
        return basePlan(harness, "guard-lock-order")
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

    private static ProofPlan.Builder basePlan(
        ProofRuntimeHarness harness,
        String id
    ) {
        return ProofPlan.builder(id, "Semantic control lock order", harness.subject, DEADLINE)
            .prerequisite("prerequisite", harness.prerequisite())
            .observation("observation", harness.connectionId, ProofTestFixture.PROFILE);
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

    private enum GuardBoundary {
        PREDECESSOR,
        SUCCESSOR
    }

    private static final class OldOrderHooks implements ProofRuntimeHarness.BoundaryHooks {
        private final Invalidation invalidation;
        private final AtomicBoolean enabled = new AtomicBoolean();
        private final AtomicReference<Thread> controlThread = new AtomicReference<>();
        private final CountDownLatch publicationFactEntered = new CountDownLatch(1);
        private final CountDownLatch controlBoundaryAttempted = new CountDownLatch(1);
        private final CountDownLatch allowPublication = new CountDownLatch(1);

        private OldOrderHooks(Invalidation invalidation) {
            this.invalidation = invalidation;
        }

        @Override
        public void beforeProofFact(ScenarioEvent event) {
            if (!enabled.get() || !matches(event)) {
                return;
            }
            publicationFactEntered.countDown();
            await(allowPublication, "release of paused subject publication");
        }

        @Override
        public void beforeAuthoritativeOperationBoundary() {
            if (enabled.get() && Thread.currentThread() == controlThread.get()) {
                controlBoundaryAttempted.countDown();
            }
        }

        private boolean matches(ScenarioEvent event) {
            return invalidation == Invalidation.ARM
                ? event instanceof ProofSubjectArmedEvent
                : event instanceof CorrelationCandidateEvent;
        }
    }
}
