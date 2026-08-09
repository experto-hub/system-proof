package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import io.github.jacekkardys.systemproof.control.SemanticInteractionSelector;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardFailure;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardSpec;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorRequirement;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorViolation;
import io.github.jacekkardys.systemproof.diagnostics.JournalRenderer;
import io.github.jacekkardys.systemproof.journal.SemanticPredecessorGuardEvent;
import io.github.jacekkardys.systemproof.observation.EvidenceCodec;
import io.github.jacekkardys.systemproof.observation.EvidenceSchemaId;
import io.github.jacekkardys.systemproof.observation.EvidenceSnapshot;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.observation.RecordedInteraction;
import io.github.jacekkardys.systemproof.observation.RequiredObservationProfile;
import io.github.jacekkardys.systemproof.observation.RequiredObservationProfile.Capability;
import io.github.jacekkardys.systemproof.observation.SessionId;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.CorrelationKeySchema;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.testsupport.OpaqueReferenceDiagnosticsFixture;
import io.github.jacekkardys.systemproof.testsupport.OpaqueReferenceDiagnosticsFixture.Behavior;
import io.github.jacekkardys.systemproof.testsupport.OpaqueReferenceDiagnosticsFixture.Probe;
import io.github.jacekkardys.systemproof.topology.ConnectionId;
import org.junit.jupiter.api.Test;

class SemanticPredecessorGuardTest {
    private static final ConnectionId PREDECESSOR_CONNECTION =
        ConnectionId.of("source[].out->predecessor[].in");
    private static final ConnectionId SUCCESSOR_CONNECTION =
        ConnectionId.of("source[].out->successor[].in");
    private static final Duration MAXIMUM_DURATION = Duration.ofSeconds(30);
    private static final EvidenceCodec<String> PREDECESSOR_CODEC = codec("predecessor");
    private static final EvidenceCodec<String> SUCCESSOR_CODEC = codec("successor");
    private static final EvidenceCodec<String> PREDECESSOR_REF_CODEC =
        codec("predecessor-ref");
    private static final EvidenceCodec<String> SUCCESSOR_REF_CODEC =
        codec("successor-ref");

    @Test
    void shouldNotInspectOpaqueSubjectWhenRenderingGuardSpecificationMetadata() {
        String[] canaries = OpaqueReferenceDiagnosticsFixture.allCanaries()
            .toArray(String[]::new);
        for (Behavior behavior : Behavior.values()) {
            Probe probe = new Probe(behavior);
            SemanticInteractionSelector<String> predecessor = SemanticInteractionSelector
                .matching(
                    PREDECESSOR_CONNECTION,
                    FlowDirection.CONSUMER_TO_PROVIDER,
                    PREDECESSOR_CODEC,
                    ignored -> true
                )
                .forSubject(probe.proofSubject());
            SemanticInteractionSelector<String> successor = SemanticInteractionSelector
                .matching(
                    SUCCESSOR_CONNECTION,
                    FlowDirection.CONSUMER_TO_PROVIDER,
                    SUCCESSOR_CODEC,
                    ignored -> true
                )
                .forSubject(probe.proofSubject());
            SemanticPredecessorGuardSpec spec = SemanticPredecessorGuardSpec.requiring(
                probe.proofSubject(),
                SemanticPredecessorRequirement.confirmed(predecessor),
                successor,
                MAXIMUM_DURATION
            );

            assertThat(spec.toString())
                .contains("subject=opaque", "subjectConstrained=true")
                .doesNotContain(canaries);
            assertThat(probe.toStringCalls()).isZero();
        }
    }

    @Test
    void shouldAuthorizeAndRecordConfirmedPredecessorRelationBeforeForwardingOnce()
        throws Exception {
        Fixture fixture = fixture();
        GuardScenario scenario = confirmedScenario(fixture, 1);
        RecordedInteraction predecessor = predecessor("confirmed:p-1", 1, 1);
        correlate(fixture, scenario.key, predecessor, PREDECESSOR_REF_CODEC, "p-1");

        assertForwarded(fixture.coordinator.permit(predecessor));

        assertThat(scenario.guard.state()).isEqualTo(
            SemanticPredecessorGuardState.PREDECESSOR_SATISFIED
        );
        RecordedInteraction successor = successor("positive:s-1", 1, 1);
        correlate(fixture, scenario.key, successor, SUCCESSOR_REF_CODEC, "s-1");
        ForwardingPermit permit = fixture.coordinator.permit(successor);

        assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
        assertThat(scenario.guard.state()).isEqualTo(
            SemanticPredecessorGuardState.SUCCESSOR_AUTHORIZED
        );
        permit.forwarded();
        permit.forwarded();

        assertThat(await(scenario.guard.completion())).isEqualTo(
            SemanticPredecessorGuardState.SATISFIED
        );
        assertThat(facts(fixture).stream()
            .filter(event -> event.kind() == SemanticPredecessorGuardEvent.Kind.TERMINAL
                && event.state() == SemanticPredecessorGuardState.SATISFIED))
            .singleElement()
            .satisfies(event -> {
                assertThat(event.predecessor()).contains(predecessor.interactionRef());
                assertThat(event.successor()).contains(successor.interactionRef());
            });
    }

    @Test
    void shouldViolateImmediatelyWhenSuccessorLinearizesBeforePredecessor()
        throws Exception {
        Fixture fixture = fixture();
        GuardScenario scenario = confirmedScenario(fixture, 2);
        RecordedInteraction successor = successor("positive:s-2", 1, 1);
        correlate(fixture, scenario.key, successor, SUCCESSOR_REF_CODEC, "s-2");

        ForwardingPermit rejected = fixture.coordinator.permit(successor);

        assertThat(rejected.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
        assertThat(await(scenario.guard.completion())).isEqualTo(
            SemanticPredecessorGuardState.VIOLATED
        );
        RecordedInteraction later = predecessor("confirmed:p-2", 1, 1);
        correlate(fixture, scenario.key, later, PREDECESSOR_REF_CODEC, "p-2");
        assertForwarded(fixture.coordinator.permit(later));
        assertThat(scenario.guard.state()).isEqualTo(
            SemanticPredecessorGuardState.VIOLATED
        );
        assertThat(facts(fixture).stream()
            .filter(event -> event.kind() == SemanticPredecessorGuardEvent.Kind.TERMINAL
                && event.state() == SemanticPredecessorGuardState.VIOLATED))
            .singleElement()
            .satisfies(event -> {
                assertThat(event.successor()).contains(successor.interactionRef());
                assertThat(event.violation()).contains(
                    SemanticPredecessorViolation.PREDECESSOR_NOT_ESTABLISHED
                );
            });

        fixture.coordinator.observationFailed(SUCCESSOR_CONNECTION);
        assertThat(scenario.guard.state()).isEqualTo(
            SemanticPredecessorGuardState.VIOLATED
        );
        assertThat(facts(fixture).stream()
            .filter(event -> event.kind()
                == SemanticPredecessorGuardEvent.Kind.SUPPRESSED_FAILURE))
            .singleElement()
            .satisfies(event -> assertThat(event.failure()).contains(
                SemanticPredecessorGuardFailure.REQUIRED_OBSERVATION_FAILURE
            ));
    }

    @Test
    void shouldNotTreatCommitAttemptAsConfirmed() throws Exception {
        Fixture fixture = fixture();
        GuardScenario scenario = confirmedScenario(fixture, 3);
        RecordedInteraction attempt = predecessor("attempt:p-3", 1, 1);
        correlate(fixture, scenario.key, attempt, PREDECESSOR_REF_CODEC, "p-3");
        assertForwarded(fixture.coordinator.permit(attempt));

        RecordedInteraction successor = successor("positive:s-3", 1, 1);
        correlate(fixture, scenario.key, successor, SUCCESSOR_REF_CODEC, "s-3");

        assertThat(fixture.coordinator.permit(successor).awaitDecision())
            .isEqualTo(ForwardingDecision.CLOSE_SESSION);
        assertThat(scenario.guard.state()).isEqualTo(
            SemanticPredecessorGuardState.VIOLATED
        );
    }

    @Test
    void shouldRequireForwardedCallbackRatherThanForwardDecision() throws Exception {
        Fixture earlyFixture = fixture();
        GuardScenario early = forwardedScenario(earlyFixture, 4);
        RecordedInteraction predecessor = predecessor("positive:p-4", 1, 1);
        correlate(earlyFixture, early.key, predecessor, PREDECESSOR_REF_CODEC, "p-4");
        ForwardingPermit predecessorPermit = earlyFixture.coordinator.permit(predecessor);
        assertThat(predecessorPermit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
        assertThat(early.guard.state()).isEqualTo(
            SemanticPredecessorGuardState.PREDECESSOR_OBSERVED
        );

        RecordedInteraction successor = successor("positive:s-4", 1, 1);
        correlate(earlyFixture, early.key, successor, SUCCESSOR_REF_CODEC, "s-4");
        assertThat(earlyFixture.coordinator.permit(successor).awaitDecision())
            .isEqualTo(ForwardingDecision.CLOSE_SESSION);
        predecessorPermit.forwarded();
        assertThat(early.guard.state()).isEqualTo(
            SemanticPredecessorGuardState.VIOLATED
        );

        Fixture validFixture = fixture();
        GuardScenario valid = forwardedScenario(validFixture, 5);
        RecordedInteraction validPredecessor = predecessor("positive:p-5", 1, 1);
        correlate(validFixture, valid.key, validPredecessor, PREDECESSOR_REF_CODEC, "p-5");
        assertForwarded(validFixture.coordinator.permit(validPredecessor));
        RecordedInteraction validSuccessor = successor("positive:s-5", 1, 1);
        correlate(validFixture, valid.key, validSuccessor, SUCCESSOR_REF_CODEC, "s-5");
        assertForwarded(validFixture.coordinator.permit(validSuccessor));
        assertThat(valid.guard.state()).isEqualTo(
            SemanticPredecessorGuardState.SATISFIED
        );
    }

    @Test
    void shouldFailForwardedBoundaryOnWriteFailureAndAbandonment() throws Exception {
        for (boolean abandoned : List.of(false, true)) {
            Fixture fixture = fixture();
            GuardScenario scenario = forwardedScenario(fixture, abandoned ? 7 : 6);
            String ref = abandoned ? "p-7" : "p-6";
            RecordedInteraction predecessor = predecessor("positive:" + ref, 1, 1);
            correlate(fixture, scenario.key, predecessor, PREDECESSOR_REF_CODEC, ref);
            ForwardingPermit permit = fixture.coordinator.permit(predecessor);
            assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);

            if (abandoned) {
                permit.abandoned();
            } else {
                permit.writeFailed();
            }

            assertThat(scenario.guard.state()).isEqualTo(
                SemanticPredecessorGuardState.FAILED
            );
            RecordedInteraction successor = successor(
                "positive:" + (abandoned ? "s-7" : "s-6"),
                1,
                1
            );
            correlate(
                fixture,
                scenario.key,
                successor,
                SUCCESSOR_REF_CODEC,
                abandoned ? "s-7" : "s-6"
            );
            assertThat(fixture.coordinator.permit(successor).awaitDecision())
                .isEqualTo(ForwardingDecision.CLOSE_SESSION);
        }
    }

    @Test
    void shouldLinearizeBothSimultaneousOrdersWithOneCoordinatorLock() throws Exception {
        assertConcurrentOrder(true);
        assertConcurrentOrder(false);
    }

    @Test
    void shouldIsolateSubjectsAndLeaveMissingOrAmbiguousTrafficUnresolved()
        throws Exception {
        Fixture fixture = fixture();
        GuardScenario target = confirmedScenario(fixture, 8);
        ProofSubjectRef otherSubject = fixture.proofSubjects.create();
        CorrelationKey otherKey = key(9);
        fixture.proofSubjects.arm(otherSubject, otherKey);

        RecordedInteraction otherPredecessor = predecessor("confirmed:p-9", 2, 1);
        correlate(fixture, otherKey, otherPredecessor, PREDECESSOR_REF_CODEC, "p-9");
        assertForwarded(fixture.coordinator.permit(otherPredecessor));
        assertThat(target.guard.state()).isEqualTo(SemanticPredecessorGuardState.ARMED);

        RecordedInteraction missing = successor("positive:s-8", 1, 1);
        assertForwarded(fixture.coordinator.permit(missing));
        assertThat(target.guard.state()).isEqualTo(SemanticPredecessorGuardState.ARMED);

        correlate(fixture, target.key, missing, SUCCESSOR_REF_CODEC, "s-8");
        RecordedInteraction duplicate = successor("positive:s-8", 2, 1);
        correlate(fixture, target.key, duplicate, SUCCESSOR_REF_CODEC, "s-8");
        assertForwarded(fixture.coordinator.permit(duplicate));
        assertThat(target.guard.state()).isEqualTo(SemanticPredecessorGuardState.ARMED);

        RecordedInteraction unrelatedSuccessor = successor("positive:s-9", 2, 2);
        correlate(fixture, otherKey, unrelatedSuccessor, SUCCESSOR_REF_CODEC, "s-9");
        assertForwarded(fixture.coordinator.permit(unrelatedSuccessor));
        assertThat(target.guard.state()).isEqualTo(SemanticPredecessorGuardState.ARMED);
        assertThat(target.guard.cancel()).isTrue();
    }

    @Test
    void shouldRejectOldNativeReferenceOnReconnectWithoutChoosingTheGuard()
        throws Exception {
        Fixture fixture = fixture();
        GuardScenario scenario = confirmedScenario(fixture, 10);
        RecordedInteraction old = successor("positive:s-10", 1, 1);
        correlate(fixture, scenario.key, old, SUCCESSOR_REF_CODEC, "s-10");
        RecordedInteraction reconnected = successor("positive:s-10", 2, 1);

        assertForwarded(fixture.coordinator.permit(reconnected));

        assertThat(scenario.guard.state()).isEqualTo(SemanticPredecessorGuardState.ARMED);
        assertThat(scenario.guard.cancel()).isTrue();
    }

    @Test
    void shouldKeepTimeoutAndRequiredObservationFailureFailClosed() throws Exception {
        Fixture timedFixture = fixture();
        GuardScenario timed = confirmedScenario(timedFixture, 11);
        assertThat(timedFixture.scheduler.runNext()).isTrue();
        assertThat(timed.guard.state()).isEqualTo(SemanticPredecessorGuardState.TIMED_OUT);
        RecordedInteraction timedSuccessor = successor("positive:s-11", 1, 1);
        correlate(timedFixture, timed.key, timedSuccessor, SUCCESSOR_REF_CODEC, "s-11");
        assertThat(timedFixture.coordinator.permit(timedSuccessor).awaitDecision())
            .isEqualTo(ForwardingDecision.CLOSE_SESSION);

        Fixture failedFixture = fixture();
        GuardScenario failed = confirmedScenario(failedFixture, 12);
        failedFixture.coordinator.observationFailed(PREDECESSOR_CONNECTION);
        assertThat(failed.guard.state()).isEqualTo(SemanticPredecessorGuardState.FAILED);
        assertThat(facts(failedFixture).getLast().failure()).contains(
            SemanticPredecessorGuardFailure.REQUIRED_OBSERVATION_FAILURE
        );
        RecordedInteraction failedSuccessor = successor("positive:s-12", 1, 1);
        correlate(failedFixture, failed.key, failedSuccessor, SUCCESSOR_REF_CODEC, "s-12");
        assertThat(failedFixture.coordinator.permit(failedSuccessor).awaitDecision())
            .isEqualTo(ForwardingDecision.CLOSE_SESSION);
    }

    @Test
    void shouldFailEveryAuthorizedGuardWhenAnyMatchingGuardRejectsInEitherArmOrder()
        throws Exception {
        assertAggregateCloseOrder(true);
        assertAggregateCloseOrder(false);
    }

    @Test
    void shouldLinearizeRequiredObservationFailureAgainstForwardedReport()
        throws Exception {
        Fixture failedFixture = fixture();
        AuthorizedScenario failed = authorizedScenario(failedFixture, 22);

        runInOrder(
            () -> failedFixture.coordinator.observationFailed(SUCCESSOR_CONNECTION),
            failed.permit::forwarded
        );

        assertThat(await(failed.guard.completion())).isEqualTo(
            SemanticPredecessorGuardState.FAILED
        );
        assertThat(relations(failedFixture, failed.guard)).isEmpty();
        assertThat(facts(failedFixture).stream()
            .filter(event -> event.guardRef().equals(failed.guard.ref()))
            .filter(event -> event.failure()
                .filter(SemanticPredecessorGuardFailure.REQUIRED_OBSERVATION_FAILURE::equals)
                .isPresent()))
            .hasSize(1);

        Fixture satisfiedFixture = fixture();
        AuthorizedScenario satisfied = authorizedScenario(satisfiedFixture, 23);

        runInOrder(
            satisfied.permit::forwarded,
            () -> satisfiedFixture.coordinator.observationFailed(SUCCESSOR_CONNECTION)
        );

        assertThat(await(satisfied.guard.completion())).isEqualTo(
            SemanticPredecessorGuardState.SATISFIED
        );
        assertThat(relations(satisfiedFixture, satisfied.guard)).hasSize(1);
    }

    @Test
    void shouldLinearizeTeardownAgainstForwardedReportAndCompleteWithoutCallback()
        throws Exception {
        Fixture cancelledFixture = fixture();
        AuthorizedScenario cancelled = authorizedScenario(cancelledFixture, 24);

        runInOrder(
            cancelledFixture.coordinator::completeExecution,
            cancelled.permit::forwarded
        );

        assertThat(await(cancelled.guard.completion())).isEqualTo(
            SemanticPredecessorGuardState.CANCELLED
        );
        assertThat(relations(cancelledFixture, cancelled.guard)).isEmpty();

        Fixture satisfiedFixture = fixture();
        AuthorizedScenario satisfied = authorizedScenario(satisfiedFixture, 25);

        runInOrder(
            satisfied.permit::forwarded,
            satisfiedFixture.coordinator::completeExecution
        );

        assertThat(await(satisfied.guard.completion())).isEqualTo(
            SemanticPredecessorGuardState.SATISFIED
        );
        assertThat(relations(satisfiedFixture, satisfied.guard)).hasSize(1);

        Fixture noCallbackFixture = fixture();
        AuthorizedScenario noCallback = authorizedScenario(noCallbackFixture, 26);

        noCallbackFixture.coordinator.completeExecution();

        assertThat(await(noCallback.guard.completion())).isEqualTo(
            SemanticPredecessorGuardState.CANCELLED
        );
        assertThat(relations(noCallbackFixture, noCallback.guard)).isEmpty();
    }

    @Test
    void shouldLinearizeCancelTimeoutTeardownAndOutcomeRaces() throws Exception {
        Fixture cancelledFixture = fixture();
        GuardScenario cancelled = confirmedScenario(cancelledFixture, 13);
        assertThat(cancelled.guard.cancel()).isTrue();
        assertThat(cancelled.guard.cancel()).isFalse();

        Fixture teardownFixture = fixture();
        GuardScenario teardown = confirmedScenario(teardownFixture, 14);
        RecordedInteraction teardownSuccessor = successor("positive:s-14", 1, 1);
        correlate(
            teardownFixture,
            teardown.key,
            teardownSuccessor,
            SUCCESSOR_REF_CODEC,
            "s-14"
        );
        teardownFixture.coordinator.completeExecution();
        assertThat(teardown.guard.state()).isEqualTo(
            SemanticPredecessorGuardState.CANCELLED
        );
        assertThat(teardownFixture.coordinator.permit(
            teardownSuccessor
        ).awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
        assertForwarded(teardownFixture.coordinator.permit(
            successor("ordinary-cleanup", 2, 1)
        ));

        assertOutcomeOrder(true);
        assertOutcomeOrder(false);
    }

    @Test
    void shouldRenderOnlyTypedSafeGuardMetadata() throws Exception {
        String secret = "guard-secret-value";
        Fixture fixture = fixture();
        ProofSubjectRef subject = fixture.proofSubjects.create();
        CorrelationKey key = key(15);
        fixture.proofSubjects.arm(subject, key);
        SemanticPredecessorGuard guard = fixture.coordinator.guard(
            SemanticPredecessorGuardSpec.requiring(
                subject,
                SemanticPredecessorRequirement.confirmed(selector(
                    PREDECESSOR_CONNECTION,
                    PREDECESSOR_CODEC,
                    PREDECESSOR_REF_CODEC,
                    subject,
                    key,
                    "confirmed:",
                    secret
                )),
                selector(
                    SUCCESSOR_CONNECTION,
                    SUCCESSOR_CODEC,
                    SUCCESSOR_REF_CODEC,
                    subject,
                    key,
                    "positive:",
                    "s-15"
                ),
                MAXIMUM_DURATION
            )
        );
        RecordedInteraction successor = successor("positive:s-15", 1, 1);
        correlate(fixture, key, successor, SUCCESSOR_REF_CODEC, "s-15");
        assertThat(fixture.coordinator.permit(successor).awaitDecision())
            .isEqualTo(ForwardingDecision.CLOSE_SESSION);

        String rendered = new JournalRenderer().render(fixture.journal.snapshot());
        assertThat(rendered)
            .contains("PREDECESSOR_NOT_ESTABLISHED", "[ref=opaque]")
            .doesNotContain(secret, "positive:s-15", "semantic-predecessor-guard-1");
        assertThat(guard.toString()).doesNotContain(secret);
    }

    private static void assertConcurrentOrder(boolean predecessorFirst) throws Exception {
        Fixture fixture = fixture();
        int seed = predecessorFirst ? 16 : 17;
        GuardScenario scenario = confirmedScenario(fixture, seed);
        RecordedInteraction predecessor = predecessor("confirmed:p-" + seed, 1, 1);
        RecordedInteraction successor = successor("positive:s-" + seed, 1, 1);
        correlate(fixture, scenario.key, predecessor, PREDECESSOR_REF_CODEC, "p-" + seed);
        correlate(fixture, scenario.key, successor, SUCCESSOR_REF_CODEC, "s-" + seed);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch firstLinearized = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<ForwardingDecision> predecessorDecision = executor.submit(() -> {
                awaitLatch(start);
                if (!predecessorFirst) {
                    awaitLatch(firstLinearized);
                }
                ForwardingPermit permit = fixture.coordinator.permit(predecessor);
                ForwardingDecision decision = permit.awaitDecision();
                permit.forwarded();
                if (predecessorFirst) {
                    firstLinearized.countDown();
                }
                return decision;
            });
            Future<ForwardingDecision> successorDecision = executor.submit(() -> {
                awaitLatch(start);
                if (predecessorFirst) {
                    awaitLatch(firstLinearized);
                }
                ForwardingPermit permit = fixture.coordinator.permit(successor);
                ForwardingDecision decision = permit.awaitDecision();
                if (decision == ForwardingDecision.FORWARD) {
                    permit.forwarded();
                }
                if (!predecessorFirst) {
                    firstLinearized.countDown();
                }
                return decision;
            });
            start.countDown();
            assertThat(predecessorDecision.get(10, TimeUnit.SECONDS))
                .isEqualTo(ForwardingDecision.FORWARD);
            assertThat(successorDecision.get(10, TimeUnit.SECONDS)).isEqualTo(
                predecessorFirst
                    ? ForwardingDecision.FORWARD
                    : ForwardingDecision.CLOSE_SESSION
            );
        }
        assertThat(scenario.guard.state()).isEqualTo(
            predecessorFirst
                ? SemanticPredecessorGuardState.SATISFIED
                : SemanticPredecessorGuardState.VIOLATED
        );
    }

    private static void assertAggregateCloseOrder(boolean satisfiedGuardArmedFirst)
        throws Exception {
        Fixture fixture = fixture();
        int seed = satisfiedGuardArmedFirst ? 20 : 21;
        ProofSubjectRef subject = fixture.proofSubjects.create();
        CorrelationKey key = key(seed);
        fixture.proofSubjects.arm(subject, key);
        String predecessorRef = "p-" + seed;
        String successorRef = "s-" + seed;
        SemanticPredecessorGuard satisfied;
        SemanticPredecessorGuard violating;
        if (satisfiedGuardArmedFirst) {
            satisfied = armConfirmedGuard(
                fixture,
                subject,
                key,
                predecessorRef,
                successorRef
            );
            violating = armConfirmedGuard(
                fixture,
                subject,
                key,
                "absent-" + seed,
                successorRef
            );
        } else {
            violating = armConfirmedGuard(
                fixture,
                subject,
                key,
                "absent-" + seed,
                successorRef
            );
            satisfied = armConfirmedGuard(
                fixture,
                subject,
                key,
                predecessorRef,
                successorRef
            );
        }
        RecordedInteraction predecessor = predecessor(
            "confirmed:" + predecessorRef,
            1,
            1
        );
        correlate(fixture, key, predecessor, PREDECESSOR_REF_CODEC, predecessorRef);
        assertForwarded(fixture.coordinator.permit(predecessor));
        RecordedInteraction successor = successor("positive:" + successorRef, 1, 1);
        correlate(fixture, key, successor, SUCCESSOR_REF_CODEC, successorRef);

        ForwardingPermit rejected = fixture.coordinator.permit(successor);

        assertThat(rejected.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
        rejected.forwarded();
        assertThat(await(satisfied.completion())).isEqualTo(
            SemanticPredecessorGuardState.FAILED
        );
        assertThat(await(violating.completion())).isEqualTo(
            SemanticPredecessorGuardState.VIOLATED
        );
        assertThat(relations(fixture, satisfied)).isEmpty();
        assertThat(relations(fixture, violating)).isEmpty();
    }

    private static AuthorizedScenario authorizedScenario(Fixture fixture, int seed)
        throws Exception {
        GuardScenario scenario = confirmedScenario(fixture, seed);
        RecordedInteraction predecessor = predecessor("confirmed:p-" + seed, 1, 1);
        correlate(fixture, scenario.key, predecessor, PREDECESSOR_REF_CODEC, "p-" + seed);
        assertForwarded(fixture.coordinator.permit(predecessor));
        RecordedInteraction successor = successor("positive:s-" + seed, 1, 1);
        correlate(fixture, scenario.key, successor, SUCCESSOR_REF_CODEC, "s-" + seed);
        ForwardingPermit permit = fixture.coordinator.permit(successor);
        assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
        assertThat(scenario.guard.state()).isEqualTo(
            SemanticPredecessorGuardState.SUCCESSOR_AUTHORIZED
        );
        return new AuthorizedScenario(scenario.guard, permit);
    }

    private static void runInOrder(Runnable first, Runnable second) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch firstCompleted = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> firstResult = executor.submit(() -> {
                awaitLatch(start);
                try {
                    first.run();
                } finally {
                    firstCompleted.countDown();
                }
            });
            Future<?> secondResult = executor.submit(() -> {
                awaitLatch(start);
                awaitLatch(firstCompleted);
                second.run();
            });
            start.countDown();
            firstResult.get(10, TimeUnit.SECONDS);
            secondResult.get(10, TimeUnit.SECONDS);
        }
    }

    private static void assertOutcomeOrder(boolean forwardedFirst) throws Exception {
        Fixture fixture = fixture();
        int seed = forwardedFirst ? 18 : 19;
        GuardScenario scenario = confirmedScenario(fixture, seed);
        RecordedInteraction predecessor = predecessor("confirmed:p-" + seed, 1, 1);
        correlate(fixture, scenario.key, predecessor, PREDECESSOR_REF_CODEC, "p-" + seed);
        assertForwarded(fixture.coordinator.permit(predecessor));
        RecordedInteraction successor = successor("positive:s-" + seed, 1, 1);
        correlate(fixture, scenario.key, successor, SUCCESSOR_REF_CODEC, "s-" + seed);
        ForwardingPermit permit = fixture.coordinator.permit(successor);
        assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch first = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> forwarded = executor.submit(() -> {
                awaitLatch(start);
                if (!forwardedFirst) {
                    awaitLatch(first);
                }
                permit.forwarded();
                if (forwardedFirst) {
                    first.countDown();
                }
                return null;
            });
            Future<?> failed = executor.submit(() -> {
                awaitLatch(start);
                if (forwardedFirst) {
                    awaitLatch(first);
                }
                permit.writeFailed();
                if (!forwardedFirst) {
                    first.countDown();
                }
                return null;
            });
            start.countDown();
            forwarded.get(10, TimeUnit.SECONDS);
            failed.get(10, TimeUnit.SECONDS);
        }
        assertThat(scenario.guard.state()).isEqualTo(
            forwardedFirst
                ? SemanticPredecessorGuardState.SATISFIED
                : SemanticPredecessorGuardState.FAILED
        );
    }

    private static GuardScenario confirmedScenario(Fixture fixture, int seed) {
        return scenario(fixture, seed, true);
    }

    private static GuardScenario forwardedScenario(Fixture fixture, int seed) {
        return scenario(fixture, seed, false);
    }

    private static GuardScenario scenario(Fixture fixture, int seed, boolean confirmed) {
        ProofSubjectRef subject = fixture.proofSubjects.create();
        CorrelationKey key = key(seed);
        fixture.proofSubjects.arm(subject, key);
        String predecessorRef = "p-" + seed;
        SemanticInteractionSelector<String> predecessor = selector(
            PREDECESSOR_CONNECTION,
            PREDECESSOR_CODEC,
            PREDECESSOR_REF_CODEC,
            subject,
            key,
            confirmed ? "confirmed:" : "positive:",
            predecessorRef
        );
        SemanticInteractionSelector<String> successor = selector(
            SUCCESSOR_CONNECTION,
            SUCCESSOR_CODEC,
            SUCCESSOR_REF_CODEC,
            subject,
            key,
            "positive:",
            "s-" + seed
        );
        SemanticPredecessorRequirement<String> requirement = confirmed
            ? SemanticPredecessorRequirement.confirmed(predecessor)
            : SemanticPredecessorRequirement.forwarded(predecessor);
        SemanticPredecessorGuard guard = fixture.coordinator.guard(
            SemanticPredecessorGuardSpec.requiring(
                subject,
                requirement,
                successor,
                MAXIMUM_DURATION
            )
        );
        return new GuardScenario(guard, key);
    }

    private static SemanticPredecessorGuard armConfirmedGuard(
        Fixture fixture,
        ProofSubjectRef subject,
        CorrelationKey key,
        String predecessorRef,
        String successorRef
    ) {
        return fixture.coordinator.guard(SemanticPredecessorGuardSpec.requiring(
            subject,
            SemanticPredecessorRequirement.confirmed(selector(
                PREDECESSOR_CONNECTION,
                PREDECESSOR_CODEC,
                PREDECESSOR_REF_CODEC,
                subject,
                key,
                "confirmed:",
                predecessorRef
            )),
            selector(
                SUCCESSOR_CONNECTION,
                SUCCESSOR_CODEC,
                SUCCESSOR_REF_CODEC,
                subject,
                key,
                "positive:",
                successorRef
            ),
            MAXIMUM_DURATION
        ));
    }

    private static SemanticInteractionSelector<String> selector(
        ConnectionId connection,
        EvidenceCodec<String> evidenceCodec,
        EvidenceCodec<String> nativeReferenceCodec,
        ProofSubjectRef subject,
        CorrelationKey key,
        String prefix,
        String expectedReference
    ) {
        return SemanticInteractionSelector.matching(
            connection,
            FlowDirection.CONSUMER_TO_PROVIDER,
            evidenceCodec,
            (prefix + expectedReference)::equals
        ).forSubject(subject).through(
            key,
            nativeReferenceCodec,
            evidence -> evidence.substring(prefix.length())
        );
    }

    private static Fixture fixture() {
        ScenarioJournal journal = ScenarioJournal.withoutDiagnosticTime();
        EnvironmentEventPublisher events = new EnvironmentEventPublisher(
            journal,
            EnvironmentLogging.defaults()
        );
        ProofSubjectRegistry proofSubjects = new ProofSubjectRegistry(events);
        ManualTimeoutScheduler scheduler = new ManualTimeoutScheduler();
        SemanticControlCapabilityRegistry capabilities =
            new SemanticControlCapabilityRegistry();
        capabilities.register(
            PREDECESSOR_CONNECTION,
            () -> SemanticControlCapabilityRegistry.Availability.DECLARED,
            Optional.of(profile(PREDECESSOR_CODEC, PREDECESSOR_REF_CODEC))
        );
        capabilities.register(
            SUCCESSOR_CONNECTION,
            () -> SemanticControlCapabilityRegistry.Availability.DECLARED,
            Optional.of(profile(SUCCESSOR_CODEC, SUCCESSOR_REF_CODEC))
        );
        return new Fixture(
            new SemanticControlCoordinator(events, proofSubjects, capabilities, scheduler),
            proofSubjects,
            journal,
            scheduler
        );
    }

    private static RequiredObservationProfile profile(
        EvidenceCodec<String> evidenceCodec,
        EvidenceCodec<String> nativeReferenceCodec
    ) {
        return new RequiredObservationProfile(
            evidenceCodec.schemaId(),
            Optional.of(nativeReferenceCodec.schemaId()),
            Set.of(Capability.CORRELATION_CONTRIBUTIONS, Capability.SEMANTIC_CONTROL),
            Set.of()
        );
    }

    private static RecordedInteraction predecessor(String value, long session, long ordinal) {
        return interaction(PREDECESSOR_CONNECTION, PREDECESSOR_CODEC, value, session, ordinal);
    }

    private static RecordedInteraction successor(String value, long session, long ordinal) {
        return interaction(SUCCESSOR_CONNECTION, SUCCESSOR_CODEC, value, session, ordinal);
    }

    private static RecordedInteraction interaction(
        ConnectionId connection,
        EvidenceCodec<String> codec,
        String value,
        long session,
        long ordinal
    ) {
        return new RecordedInteraction(
            new InteractionRef(
                new SessionId(connection, session),
                FlowDirection.CONSUMER_TO_PROVIDER,
                ordinal
            ),
            EvidenceSnapshot.capture(codec, value)
        );
    }

    private static void correlate(
        Fixture fixture,
        CorrelationKey key,
        RecordedInteraction interaction,
        EvidenceCodec<String> nativeReferenceCodec,
        String nativeReference
    ) {
        fixture.proofSubjects.publish(
            interaction.interactionRef(),
            CorrelationContribution.capture(key, nativeReferenceCodec, nativeReference)
        );
    }

    private static EvidenceCodec<String> codec(String name) {
        return new EvidenceCodec<>() {
            @Override
            public EvidenceSchemaId schemaId() {
                return new EvidenceSchemaId("test", name, 1);
            }

            @Override
            public byte[] encode(String evidence) {
                return evidence.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public String decode(byte[] encodedEvidence) {
                return new String(encodedEvidence, StandardCharsets.UTF_8);
            }
        };
    }

    private static CorrelationKey key(int seed) {
        byte[] digest = new byte[16];
        Arrays.fill(digest, (byte) seed);
        return CorrelationKey.ofDigest(
            new CorrelationKeySchema("test", "semantic-predecessor", 1),
            digest
        );
    }

    private static List<SemanticPredecessorGuardEvent> facts(Fixture fixture) {
        return fixture.journal.snapshot().entries().stream()
            .map(entry -> entry.event())
            .filter(SemanticPredecessorGuardEvent.class::isInstance)
            .map(SemanticPredecessorGuardEvent.class::cast)
            .toList();
    }

    private static List<SemanticPredecessorGuardEvent> relations(
        Fixture fixture,
        SemanticPredecessorGuard guard
    ) {
        return facts(fixture).stream()
            .filter(event -> event.guardRef().equals(guard.ref()))
            .filter(event -> event.kind() == SemanticPredecessorGuardEvent.Kind.TERMINAL
                && event.state() == SemanticPredecessorGuardState.SATISFIED)
            .toList();
    }

    private static void assertForwarded(ForwardingPermit permit) throws Exception {
        assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
        permit.forwarded();
    }

    private static <T> T await(java.util.concurrent.CompletionStage<T> stage)
        throws Exception {
        return stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Race did not reach its control point");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Race was interrupted", interrupted);
        }
    }

    private record Fixture(
        SemanticControlCoordinator coordinator,
        ProofSubjectRegistry proofSubjects,
        ScenarioJournal journal,
        ManualTimeoutScheduler scheduler
    ) {}

    private record GuardScenario(SemanticPredecessorGuard guard, CorrelationKey key) {}

    private record AuthorizedScenario(
        SemanticPredecessorGuard guard,
        ForwardingPermit permit
    ) {}

    private static final class ManualTimeoutScheduler
        implements SemanticControlCoordinator.TimeoutScheduler {
        private final List<ScheduledAction> actions = new ArrayList<>();

        @Override
        public synchronized SemanticControlCoordinator.TimeoutTask schedule(
            Duration delay,
            Runnable action
        ) {
            ScheduledAction scheduled = new ScheduledAction(action);
            actions.add(scheduled);
            return () -> scheduled.cancelled = true;
        }

        private boolean runNext() {
            ScheduledAction scheduled;
            synchronized (this) {
                scheduled = actions.stream()
                    .filter(action -> !action.cancelled && !action.ran)
                    .findFirst()
                    .orElse(null);
                if (scheduled == null) {
                    return false;
                }
                scheduled.ran = true;
            }
            scheduled.action.run();
            return true;
        }

        @Override
        public synchronized void close() {
            actions.forEach(action -> action.cancelled = true);
        }

        private static final class ScheduledAction {
            private final Runnable action;
            private boolean cancelled;
            private boolean ran;

            private ScheduledAction(Runnable action) {
                this.action = action;
            }
        }
    }
}
