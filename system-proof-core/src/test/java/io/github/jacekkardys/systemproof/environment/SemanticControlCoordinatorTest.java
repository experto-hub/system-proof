package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldFailure;
import io.github.jacekkardys.systemproof.control.SemanticInteractionSelector;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.journal.SemanticHoldEvent;
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
import io.github.jacekkardys.systemproof.proof.CorrelationResult;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.topology.ConnectionId;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;

class SemanticControlCoordinatorTest {
    private static final ConnectionId CONNECTION =
        ConnectionId.of("client[].out->server[].in");
    private static final ConnectionId OTHER_CONNECTION =
        ConnectionId.of("other[].out->server[].in");
    private static final Duration MAXIMUM_HOLD = Duration.ofSeconds(30);
    private static final EvidenceCodec<String> CODEC = codec("message");
    private static final EvidenceCodec<String> OTHER_CODEC = codec("other-message");

    @Test
    void shouldRejectArmUnlessConnectionBelongsToEnvironmentAndDeclaresAvailableCapability() {
        Fixture outside = fixture();
        assertThatThrownBy(() -> outside.coordinator.arm(
            selector(OTHER_CONNECTION, "target"),
            MAXIMUM_HOLD
        )).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Connection 'other[].out->server[].in' is outside the environment");

        Fixture unsupported = fixture(
            SemanticControlCapabilityRegistry.Availability.UNSUPPORTED
        );
        assertThatThrownBy(() -> unsupported.coordinator.arm(
            selector("target"),
            MAXIMUM_HOLD
        )).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("does not declare semantic-control capability");

        Fixture unavailable = fixture(
            SemanticControlCapabilityRegistry.Availability.UNAVAILABLE
        );
        assertThatThrownBy(() -> unavailable.coordinator.arm(
            selector("target"),
            MAXIMUM_HOLD
        )).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("does not currently have active semantic-control capability");

        Fixture available = fixture(
            SemanticControlCapabilityRegistry.Availability.AVAILABLE
        );
        SemanticHold hold = available.coordinator.arm(selector("target"), MAXIMUM_HOLD);
        assertThat(hold.cancel()).isTrue();
    }

    @Test
    void shouldRejectSelectorSchemasOutsideRequiredConnectionProfile() {
        Fixture fixture = fixture();
        assertThatThrownBy(() -> fixture.coordinator.arm(
            SemanticInteractionSelector.matching(
                CONNECTION,
                FlowDirection.CONSUMER_TO_PROVIDER,
                OTHER_CODEC,
                evidence -> true
            ),
            MAXIMUM_HOLD
        )).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("evidence schema does not match");

        ProofSubjectRef subject = fixture.proofSubjects.create();
        CorrelationKey key = correlationKey(1);
        fixture.proofSubjects.arm(subject, key);
        assertThatThrownBy(() -> fixture.coordinator.arm(
            selector("target").forSubject(subject).through(
                key,
                OTHER_CODEC,
                evidence -> evidence
            ),
            MAXIMUM_HOLD
        )).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("native-flow schema does not match");
    }

    @Test
    void shouldArmMatchJournalReachReleaseAndCompleteOnlyAfterForwardedCallback()
        throws Exception {
        Fixture fixture = fixture();
        SemanticHold hold = fixture.coordinator.arm(selector("target"), MAXIMUM_HOLD);
        assertThat(hold.state()).isEqualTo(SemanticHoldState.ARMED);
        assertThat(hold.release().toCompletableFuture()).isCompletedExceptionally();
        var journalContainedReachedAtSignal = hold.reached().thenApply(ignored ->
            events(fixture).stream()
                .anyMatch(event -> event.state() == SemanticHoldState.REACHED_HELD)
        );

        ForwardingPermit permit = fixture.coordinator.permit(interaction("target", 1));

        assertThat(hold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertThat(await(hold.reached())).isEqualTo(interactionRef(1));
        assertThat(await(journalContainedReachedAtSignal)).isTrue();
        assertThat(fixture.scheduler.activeTasks()).isEqualTo(1);

        var release = hold.release();

        assertThat(hold.state()).isEqualTo(SemanticHoldState.RELEASING);
        assertThat(release.toCompletableFuture()).isNotDone();
        assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
        assertThat(hold.release().toCompletableFuture()).isCompletedExceptionally();

        permit.forwarded();

        assertThat(await(release)).isNull();
        assertThat(await(hold.completion())).isEqualTo(SemanticHoldState.FORWARDED);
        assertThat(hold.cancel()).isFalse();
        assertThat(states(fixture)).containsExactly(
            SemanticHoldState.ARMED,
            SemanticHoldState.REACHED_HELD,
            SemanticHoldState.RELEASING,
            SemanticHoldState.FORWARDED
        );
    }

    @Test
    void shouldIgnoreNonMatchingConnectionDirectionSchemaAndPredicate() throws Exception {
        Fixture fixture = fixture();
        SemanticHold hold = fixture.coordinator.arm(selector("target"), MAXIMUM_HOLD);

        assertImmediate(fixture.coordinator.permit(interaction(
            OTHER_CONNECTION,
            FlowDirection.CONSUMER_TO_PROVIDER,
            CODEC,
            "target",
            1
        )));
        assertImmediate(fixture.coordinator.permit(interaction(
            CONNECTION,
            FlowDirection.PROVIDER_TO_CONSUMER,
            CODEC,
            "target",
            2
        )));
        assertImmediate(fixture.coordinator.permit(interaction(
            CONNECTION,
            FlowDirection.CONSUMER_TO_PROVIDER,
            OTHER_CODEC,
            "target",
            3
        )));
        assertImmediate(fixture.coordinator.permit(interaction("other", 4)));

        assertThat(hold.state()).isEqualTo(SemanticHoldState.ARMED);
        assertThat(states(fixture)).containsExactly(SemanticHoldState.ARMED);
    }

    @Test
    void shouldFailClosedOnSelectorFailureAndOverlappingMatches() throws Exception {
        Fixture selectorFixture = fixture();
        SemanticHold broken = selectorFixture.coordinator.arm(
            SemanticInteractionSelector.matching(
                CONNECTION,
                FlowDirection.CONSUMER_TO_PROVIDER,
                CODEC,
                ignored -> {
                    throw new IllegalStateException("secret matcher failure");
                }
            ),
            MAXIMUM_HOLD
        );

        ForwardingPermit brokenPermit = selectorFixture.coordinator.permit(
            interaction("target-secret", 1)
        );

        assertThat(brokenPermit.awaitDecision()).isEqualTo(
            ForwardingDecision.CLOSE_SESSION
        );
        assertThat(broken.state()).isEqualTo(SemanticHoldState.FAILED);
        assertThat(events(selectorFixture).getLast().failure())
            .contains(SemanticHoldFailure.SELECTOR_EVALUATION);
        assertThat(rendered(selectorFixture)).doesNotContain("target-secret", "matcher failure");

        Fixture overlapFixture = fixture();
        SemanticHold first = overlapFixture.coordinator.arm(
            SemanticInteractionSelector.matching(
                CONNECTION,
                FlowDirection.CONSUMER_TO_PROVIDER,
                CODEC,
                ignored -> true
            ),
            MAXIMUM_HOLD
        );
        SemanticHold second = overlapFixture.coordinator.arm(
            SemanticInteractionSelector.matching(
                CONNECTION,
                FlowDirection.CONSUMER_TO_PROVIDER,
                CODEC,
                value -> value.startsWith("target")
            ),
            MAXIMUM_HOLD
        );

        ForwardingPermit overlapPermit = overlapFixture.coordinator.permit(
            interaction("target-secret", 1)
        );

        assertThat(overlapPermit.awaitDecision()).isEqualTo(
            ForwardingDecision.CLOSE_SESSION
        );
        assertThat(first.state()).isEqualTo(SemanticHoldState.FAILED);
        assertThat(second.state()).isEqualTo(SemanticHoldState.FAILED);
        assertThat(events(overlapFixture).stream()
            .filter(event -> event.failure().equals(
                java.util.Optional.of(SemanticHoldFailure.AMBIGUOUS_MATCH)
            )))
            .hasSize(2);
    }

    @Test
    void shouldCancelBeforeOrAfterReachWithoutForwarding() throws Exception {
        Fixture beforeFixture = fixture();
        SemanticHold before = beforeFixture.coordinator.arm(selector("target"), MAXIMUM_HOLD);

        assertThat(before.cancel()).isTrue();
        assertThat(before.cancel()).isFalse();
        assertThat(before.state()).isEqualTo(SemanticHoldState.CANCELLED);
        assertThatThrownBy(before.reached().toCompletableFuture()::join)
            .isInstanceOf(CompletionException.class);
        assertImmediate(beforeFixture.coordinator.permit(interaction("target", 1)));

        Fixture afterFixture = fixture();
        SemanticHold after = afterFixture.coordinator.arm(selector("target"), MAXIMUM_HOLD);
        ForwardingPermit permit = afterFixture.coordinator.permit(interaction("target", 1));

        assertThat(after.cancel()).isTrue();
        assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
        assertThat(after.state()).isEqualTo(SemanticHoldState.CANCELLED);
        assertThat(after.release().toCompletableFuture()).isCompletedExceptionally();
    }

    @Test
    void shouldTimeoutFromReachedAndFailReleaseOnWriteFailure() throws Exception {
        Fixture timeoutFixture = fixture();
        SemanticHold timed = timeoutFixture.coordinator.arm(selector("target"), MAXIMUM_HOLD);
        ForwardingPermit timedPermit = timeoutFixture.coordinator.permit(
            interaction("target", 1)
        );

        timeoutFixture.scheduler.runNext();

        assertThat(timedPermit.awaitDecision()).isEqualTo(
            ForwardingDecision.CLOSE_SESSION
        );
        assertThat(timed.state()).isEqualTo(SemanticHoldState.TIMED_OUT);

        Fixture failureFixture = fixture();
        SemanticHold failed = failureFixture.coordinator.arm(selector("target"), MAXIMUM_HOLD);
        ForwardingPermit failedPermit = failureFixture.coordinator.permit(
            interaction("target", 1)
        );
        var release = failed.release();
        assertThat(failedPermit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);

        failedPermit.writeFailed();

        assertThat(failed.state()).isEqualTo(SemanticHoldState.FAILED);
        assertThatThrownBy(release.toCompletableFuture()::join)
            .isInstanceOf(CompletionException.class);
        assertThat(events(failureFixture).getLast().failure())
            .contains(SemanticHoldFailure.WRITE_FAILURE);
        assertThat(states(failureFixture).stream()
            .filter(SemanticControlCoordinatorTest::terminal))
            .containsExactly(SemanticHoldState.FAILED);
    }

    @Test
    void shouldKeepReachedHistoricalWhenTimeoutRunsBeforeReachedFuturePublication()
        throws Exception {
        ScenarioJournal journal = ScenarioJournal.withoutDiagnosticTime();
        EnvironmentEventPublisher events = new EnvironmentEventPublisher(
            journal,
            EnvironmentLogging.defaults()
        );
        ProofSubjectRegistry proofSubjects = new ProofSubjectRegistry(events);
        SemanticControlCapabilityRegistry capabilities =
            new SemanticControlCapabilityRegistry();
        capabilities.register(
            CONNECTION,
            () -> SemanticControlCapabilityRegistry.Availability.DECLARED,
            Optional.of(requiredObservationProfile())
        );
        ImmediateTimeoutScheduler scheduler = new ImmediateTimeoutScheduler();
        SemanticControlCoordinator coordinator = new SemanticControlCoordinator(
            events,
            proofSubjects,
            capabilities,
            scheduler
        );
        SemanticHold hold = coordinator.arm(selector("target"), MAXIMUM_HOLD);

        ForwardingPermit permit = coordinator.permit(interaction("target", 1));

        assertThat(await(hold.reached())).isEqualTo(interactionRef(1));
        assertThat(await(hold.completion())).isEqualTo(SemanticHoldState.TIMED_OUT);
        assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
        assertThat(journal.snapshot().entries().stream()
            .map(entry -> entry.event())
            .filter(SemanticHoldEvent.class::isInstance)
            .map(SemanticHoldEvent.class::cast)
            .map(SemanticHoldEvent::state))
            .containsExactly(
                SemanticHoldState.ARMED,
                SemanticHoldState.REACHED_HELD,
                SemanticHoldState.TIMED_OUT
            );
        assertThat(scheduler.cancelled).isTrue();
    }

    @Test
    void shouldLinearizeReleaseAgainstCancelWithOneTerminalFact() throws Exception {
        Fixture fixture = fixture();
        SemanticHold hold = fixture.coordinator.arm(selector("target"), MAXIMUM_HOLD);
        ForwardingPermit permit = fixture.coordinator.permit(interaction("target", 1));
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> release = executor.submit(() -> {
                awaitLatch(start);
                return hold.release();
            });
            Future<Boolean> cancel = executor.submit(() -> {
                awaitLatch(start);
                return hold.cancel();
            });
            start.countDown();
            release.get(10, TimeUnit.SECONDS);
            boolean cancelWon = cancel.get(10, TimeUnit.SECONDS);

            ForwardingDecision decision = permit.awaitDecision();
            if (cancelWon) {
                assertThat(decision).isEqualTo(ForwardingDecision.CLOSE_SESSION);
                assertThat(hold.state()).isEqualTo(SemanticHoldState.CANCELLED);
            } else {
                assertThat(decision).isEqualTo(ForwardingDecision.FORWARD);
                permit.forwarded();
                assertThat(hold.state()).isEqualTo(SemanticHoldState.FORWARDED);
            }
        }
        assertThat(states(fixture).stream().filter(SemanticControlCoordinatorTest::terminal))
            .hasSize(1);
    }

    @Test
    void shouldLinearizeReleaseAgainstTimeoutWithOneTerminalFact() throws Exception {
        Fixture fixture = fixture();
        SemanticHold hold = fixture.coordinator.arm(selector("target"), MAXIMUM_HOLD);
        ForwardingPermit permit = fixture.coordinator.permit(interaction("target", 1));
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> release = executor.submit(() -> {
                awaitLatch(start);
                return hold.release();
            });
            Future<Boolean> timeout = executor.submit(() -> {
                awaitLatch(start);
                return fixture.scheduler.runNext();
            });
            start.countDown();
            release.get(10, TimeUnit.SECONDS);
            timeout.get(10, TimeUnit.SECONDS);

            ForwardingDecision decision = permit.awaitDecision();
            if (hold.state() == SemanticHoldState.TIMED_OUT) {
                assertThat(decision).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            } else {
                assertThat(decision).isEqualTo(ForwardingDecision.FORWARD);
                permit.forwarded();
                assertThat(hold.state()).isEqualTo(SemanticHoldState.FORWARDED);
            }
        }
        assertThat(states(fixture).stream().filter(SemanticControlCoordinatorTest::terminal))
            .hasSize(1);
    }

    @Test
    void shouldLinearizeReleaseAgainstTeardownWithOneTerminalFact() throws Exception {
        Fixture fixture = fixture();
        SemanticHold hold = fixture.coordinator.arm(selector("target"), MAXIMUM_HOLD);
        ForwardingPermit permit = fixture.coordinator.permit(interaction("target", 1));
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> release = executor.submit(() -> {
                awaitLatch(start);
                return hold.release();
            });
            Future<?> teardown = executor.submit(() -> {
                awaitLatch(start);
                fixture.coordinator.completeExecution();
                return null;
            });
            start.countDown();
            release.get(10, TimeUnit.SECONDS);
            teardown.get(10, TimeUnit.SECONDS);

            ForwardingDecision decision = permit.awaitDecision();
            if (hold.state() == SemanticHoldState.CANCELLED) {
                assertThat(decision).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            } else {
                assertThat(decision).isEqualTo(ForwardingDecision.FORWARD);
                permit.forwarded();
                assertThat(hold.state()).isEqualTo(SemanticHoldState.FORWARDED);
            }
        }
        assertThat(states(fixture).stream().filter(SemanticControlCoordinatorTest::terminal))
            .hasSize(1);
    }

    @Test
    void shouldValidateSubjectOwnershipAndRequireUniquePublishedCorrelation()
        throws Exception {
        Fixture firstExecution = fixture();
        Fixture secondExecution = fixture();
        ProofSubjectRef foreign = secondExecution.proofSubjects.create();

        assertThatThrownBy(() -> firstExecution.coordinator.arm(
            selector("target").forSubject(foreign),
            MAXIMUM_HOLD
        )).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Proof subject belongs to a different environment execution");

        ProofSubjectRef subject = firstExecution.proofSubjects.create();
        CorrelationKey key = CorrelationKey.ofDigest(
            new CorrelationKeySchema("test", "request", 1),
            new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
        );
        firstExecution.proofSubjects.arm(subject, key);
        SemanticHold hold = firstExecution.coordinator.arm(
            selector("target").forSubject(subject),
            MAXIMUM_HOLD
        );

        assertImmediate(firstExecution.coordinator.permit(interaction("target", 1)));
        RecordedInteraction correlated = interaction("target", 2);
        firstExecution.proofSubjects.publish(
            correlated.interactionRef(),
            CorrelationContribution.capture(key, CODEC, "native-reference")
        );

        ForwardingPermit permit = firstExecution.coordinator.permit(correlated);

        assertThat(hold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertThat(hold.cancel()).isTrue();
        assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);

        Fixture ambiguousFixture = fixture();
        ProofSubjectRef left = ambiguousFixture.proofSubjects.create();
        ProofSubjectRef right = ambiguousFixture.proofSubjects.create();
        ambiguousFixture.proofSubjects.arm(left, key);
        ambiguousFixture.proofSubjects.arm(right, key);
        SemanticHold isolated = ambiguousFixture.coordinator.arm(
            selector("target").forSubject(left),
            MAXIMUM_HOLD
        );
        RecordedInteraction ambiguous = interaction("target", 3);
        ambiguousFixture.proofSubjects.publish(
            ambiguous.interactionRef(),
            CorrelationContribution.capture(key, CODEC, "native-reference")
        );

        assertImmediate(ambiguousFixture.coordinator.permit(ambiguous));
        assertThat(isolated.state()).isEqualTo(SemanticHoldState.ARMED);
    }

    @Test
    void shouldNotReachSubjectOnlyHoldWhenKeyBecomesSharedAfterPublication()
        throws Exception {
        Fixture fixture = fixture();
        ProofSubjectRef first = fixture.proofSubjects.create();
        ProofSubjectRef second = fixture.proofSubjects.create();
        CorrelationKey key = correlationKey(20);
        fixture.proofSubjects.arm(first, key);
        SemanticHold hold = fixture.coordinator.arm(
            selector("target").forSubject(first),
            MAXIMUM_HOLD
        );
        RecordedInteraction candidate = interaction("target", 1);
        fixture.proofSubjects.publish(
            candidate.interactionRef(),
            CorrelationContribution.capture(key, CODEC, "native-reference")
        );

        fixture.proofSubjects.arm(second, key);

        assertThat(fixture.proofSubjects.correlation(first, key, CODEC))
            .isInstanceOf(CorrelationResult.Ambiguous.class);
        assertThat(fixture.proofSubjects.correlation(second, key, CODEC))
            .isInstanceOf(CorrelationResult.Ambiguous.class);
        assertImmediate(fixture.coordinator.permit(candidate));
        assertThat(hold.state()).isEqualTo(SemanticHoldState.ARMED);
        assertThat(hold.cancel()).isTrue();
    }

    @Test
    void shouldNotReachNativeFlowHoldWhenKeyBecomesSharedAfterPublication()
        throws Exception {
        Fixture fixture = fixture();
        ProofSubjectRef first = fixture.proofSubjects.create();
        ProofSubjectRef second = fixture.proofSubjects.create();
        CorrelationKey key = correlationKey(21);
        fixture.proofSubjects.arm(first, key);
        SemanticHold hold = fixture.coordinator.arm(
            selector("commit:native-reference").forSubject(first).through(
                key,
                CODEC,
                evidence -> evidence.substring("commit:".length())
            ),
            MAXIMUM_HOLD
        );
        RecordedInteraction candidate = interaction("commit:native-reference", 1);
        fixture.proofSubjects.publish(
            candidate.interactionRef(),
            CorrelationContribution.capture(key, CODEC, "native-reference")
        );

        fixture.proofSubjects.arm(second, key);

        assertThat(fixture.proofSubjects.correlation(first, key, CODEC))
            .isInstanceOf(CorrelationResult.Ambiguous.class);
        assertThat(fixture.proofSubjects.correlation(second, key, CODEC))
            .isInstanceOf(CorrelationResult.Ambiguous.class);
        assertImmediate(fixture.coordinator.permit(candidate));
        assertThat(hold.state()).isEqualTo(SemanticHoldState.ARMED);
        assertThat(hold.cancel()).isTrue();
    }

    @Test
    void shouldKeepTwoSubjectBoundHoldsCorrelationIsolated() throws Exception {
        Fixture fixture = fixture();
        ProofSubjectRef leftSubject = fixture.proofSubjects.create();
        ProofSubjectRef rightSubject = fixture.proofSubjects.create();
        CorrelationKey leftKey = correlationKey(1);
        CorrelationKey rightKey = correlationKey(2);
        fixture.proofSubjects.arm(leftSubject, leftKey);
        fixture.proofSubjects.arm(rightSubject, rightKey);
        SemanticHold left = fixture.coordinator.arm(
            SemanticInteractionSelector.matching(
                CONNECTION,
                FlowDirection.CONSUMER_TO_PROVIDER,
                CODEC,
                ignored -> true
            ).forSubject(leftSubject),
            MAXIMUM_HOLD
        );
        SemanticHold right = fixture.coordinator.arm(
            SemanticInteractionSelector.matching(
                CONNECTION,
                FlowDirection.CONSUMER_TO_PROVIDER,
                CODEC,
                ignored -> true
            ).forSubject(rightSubject),
            MAXIMUM_HOLD
        );
        RecordedInteraction leftInteraction = interaction("same-evidence", 1);
        fixture.proofSubjects.publish(
            leftInteraction.interactionRef(),
            CorrelationContribution.capture(leftKey, CODEC, "left-native")
        );

        ForwardingPermit leftPermit = fixture.coordinator.permit(leftInteraction);

        assertThat(left.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertThat(right.state()).isEqualTo(SemanticHoldState.ARMED);
        assertThat(left.cancel()).isTrue();
        assertThat(leftPermit.awaitDecision()).isEqualTo(
            ForwardingDecision.CLOSE_SESSION
        );

        RecordedInteraction rightInteraction = interaction("same-evidence", 2);
        fixture.proofSubjects.publish(
            rightInteraction.interactionRef(),
            CorrelationContribution.capture(rightKey, CODEC, "right-native")
        );
        ForwardingPermit rightPermit = fixture.coordinator.permit(rightInteraction);

        assertThat(right.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertThat(right.cancel()).isTrue();
        assertThat(rightPermit.awaitDecision()).isEqualTo(
            ForwardingDecision.CLOSE_SESSION
        );
    }

    @Test
    void shouldReachPreArmedSubjectHoldThroughNativeFlowReference() throws Exception {
        Fixture fixture = fixture();
        ProofSubjectRef subject = fixture.proofSubjects.create();
        CorrelationKey key = correlationKey(1);
        fixture.proofSubjects.arm(subject, key);
        SemanticHold hold = fixture.coordinator.arm(
            SemanticInteractionSelector.matching(
                CONNECTION,
                FlowDirection.CONSUMER_TO_PROVIDER,
                CODEC,
                evidence -> evidence.startsWith("commit:")
            ).forSubject(subject).through(
                key,
                CODEC,
                evidence -> evidence.substring("commit:".length())
            ),
            MAXIMUM_HOLD
        );

        assertImmediate(fixture.coordinator.permit(interaction("commit:other", 1)));
        fixture.proofSubjects.publish(
            interactionRef(2),
            CorrelationContribution.capture(key, CODEC, "selected")
        );
        assertImmediate(fixture.coordinator.permit(interaction("commit:other", 3)));

        ForwardingPermit permit = fixture.coordinator.permit(
            interaction("commit:selected", 4)
        );

        assertThat(hold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertThat(hold.cancel()).isTrue();
        assertThat(permit.awaitDecision()).isEqualTo(
            ForwardingDecision.CLOSE_SESSION
        );
    }

    @Test
    void shouldRejectEqualNativeReferenceFromAnotherConnection() throws Exception {
        Fixture fixture = fixture(
            SemanticControlCapabilityRegistry.Availability.DECLARED,
            OTHER_CONNECTION
        );
        ProofSubjectRef subject = fixture.proofSubjects.create();
        CorrelationKey key = correlationKey(3);
        fixture.proofSubjects.arm(subject, key);
        fixture.proofSubjects.publish(
            interactionRef(CONNECTION, FlowDirection.CONSUMER_TO_PROVIDER, 1, 1),
            CorrelationContribution.capture(key, CODEC, "same-native-reference")
        );
        SemanticHold hold = fixture.coordinator.arm(
            selector(OTHER_CONNECTION, "same-native-reference")
                .forSubject(subject)
                .through(key, CODEC, evidence -> evidence),
            MAXIMUM_HOLD
        );

        assertImmediate(fixture.coordinator.permit(interaction(
            OTHER_CONNECTION,
            FlowDirection.CONSUMER_TO_PROVIDER,
            CODEC,
            "same-native-reference",
            1,
            1
        )));

        assertThat(hold.state()).isEqualTo(SemanticHoldState.ARMED);
        assertThat(hold.cancel()).isTrue();
    }

    @Test
    void shouldRejectEqualNativeReferenceFromAnotherSession() throws Exception {
        Fixture fixture = fixture();
        ProofSubjectRef subject = fixture.proofSubjects.create();
        CorrelationKey key = correlationKey(4);
        fixture.proofSubjects.arm(subject, key);
        fixture.proofSubjects.publish(
            interactionRef(CONNECTION, FlowDirection.CONSUMER_TO_PROVIDER, 1, 1),
            CorrelationContribution.capture(key, CODEC, "same-native-reference")
        );
        SemanticHold hold = fixture.coordinator.arm(
            selector("same-native-reference").forSubject(subject).through(
                key,
                CODEC,
                evidence -> evidence
            ),
            MAXIMUM_HOLD
        );

        assertImmediate(fixture.coordinator.permit(interaction(
            CONNECTION,
            FlowDirection.CONSUMER_TO_PROVIDER,
            CODEC,
            "same-native-reference",
            2,
            1
        )));

        assertThat(hold.state()).isEqualTo(SemanticHoldState.ARMED);
        assertThat(hold.cancel()).isTrue();
    }

    @Test
    void shouldIgnoreEqualReferencesOwnedByOtherSubjectsOnDifferentProvenance()
        throws Exception {
        Fixture fixture = fixture(
            SemanticControlCapabilityRegistry.Availability.DECLARED,
            OTHER_CONNECTION
        );
        ProofSubjectRef selected = fixture.proofSubjects.create();
        ProofSubjectRef otherConnection = fixture.proofSubjects.create();
        ProofSubjectRef otherSession = fixture.proofSubjects.create();
        CorrelationKey selectedKey = correlationKey(5);
        CorrelationKey otherConnectionKey = correlationKey(6);
        CorrelationKey otherSessionKey = correlationKey(7);
        fixture.proofSubjects.arm(selected, selectedKey);
        fixture.proofSubjects.arm(otherConnection, otherConnectionKey);
        fixture.proofSubjects.arm(otherSession, otherSessionKey);
        fixture.proofSubjects.publish(
            interactionRef(CONNECTION, FlowDirection.CONSUMER_TO_PROVIDER, 1, 1),
            CorrelationContribution.capture(selectedKey, CODEC, "colliding-bytes")
        );
        fixture.proofSubjects.publish(
            interactionRef(OTHER_CONNECTION, FlowDirection.CONSUMER_TO_PROVIDER, 1, 1),
            CorrelationContribution.capture(
                otherConnectionKey,
                CODEC,
                "colliding-bytes"
            )
        );
        fixture.proofSubjects.publish(
            interactionRef(CONNECTION, FlowDirection.CONSUMER_TO_PROVIDER, 2, 1),
            CorrelationContribution.capture(otherSessionKey, CODEC, "colliding-bytes")
        );
        SemanticHold hold = fixture.coordinator.arm(
            selector("colliding-bytes").forSubject(selected).through(
                selectedKey,
                CODEC,
                evidence -> evidence
            ),
            MAXIMUM_HOLD
        );

        ForwardingPermit permit = fixture.coordinator.permit(interaction(
            CONNECTION,
            FlowDirection.CONSUMER_TO_PROVIDER,
            CODEC,
            "colliding-bytes",
            1,
            2
        ));

        assertThat(hold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertThat(hold.cancel()).isTrue();
        assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
    }

    @Test
    void shouldComposeOppositeDirectionsOnTheSamePhysicalSession() throws Exception {
        Fixture fixture = fixture();
        ProofSubjectRef subject = fixture.proofSubjects.create();
        CorrelationKey key = correlationKey(8);
        fixture.proofSubjects.arm(subject, key);
        fixture.proofSubjects.publish(
            interactionRef(CONNECTION, FlowDirection.CONSUMER_TO_PROVIDER, 1, 1),
            CorrelationContribution.capture(key, CODEC, "bidirectional-native")
        );
        SemanticHold hold = fixture.coordinator.arm(
            SemanticInteractionSelector.matching(
                CONNECTION,
                FlowDirection.PROVIDER_TO_CONSUMER,
                CODEC,
                "commit:bidirectional-native"::equals
            ).forSubject(subject).through(
                key,
                CODEC,
                evidence -> evidence.substring("commit:".length())
            ),
            MAXIMUM_HOLD
        );

        ForwardingPermit permit = fixture.coordinator.permit(interaction(
            CONNECTION,
            FlowDirection.PROVIDER_TO_CONSUMER,
            CODEC,
            "commit:bidirectional-native",
            1,
            1
        ));

        assertThat(hold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertThat(hold.cancel()).isTrue();
        assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
    }

    @Test
    void shouldKeepNativeFlowReferenceIsolatedAcrossSubjects() throws Exception {
        Fixture fixture = fixture();
        ProofSubjectRef selected = fixture.proofSubjects.create();
        ProofSubjectRef unrelated = fixture.proofSubjects.create();
        CorrelationKey selectedKey = correlationKey(1);
        CorrelationKey unrelatedKey = correlationKey(2);
        fixture.proofSubjects.arm(selected, selectedKey);
        fixture.proofSubjects.arm(unrelated, unrelatedKey);
        SemanticHold hold = fixture.coordinator.arm(
            selector("shared").forSubject(selected).through(
                selectedKey,
                CODEC,
                evidence -> evidence
            ),
            MAXIMUM_HOLD
        );
        fixture.proofSubjects.publish(
            interactionRef(1),
            CorrelationContribution.capture(selectedKey, CODEC, "shared")
        );
        fixture.proofSubjects.publish(
            interactionRef(2),
            CorrelationContribution.capture(unrelatedKey, CODEC, "shared")
        );

        assertImmediate(fixture.coordinator.permit(interaction("shared", 3)));

        assertThat(hold.state()).isEqualTo(SemanticHoldState.ARMED);
        assertThat(hold.cancel()).isTrue();
    }

    @Test
    void shouldIgnoreSharedKeyStaleFlowWhenIndependentKeyOwnsSameNativeFlow()
        throws Exception {
        Fixture fixture = fixture();
        ProofSubjectRef selected = fixture.proofSubjects.create();
        ProofSubjectRef stale = fixture.proofSubjects.create();
        ProofSubjectRef duplicate = fixture.proofSubjects.create();
        CorrelationKey selectedKey = correlationKey(22);
        CorrelationKey sharedKey = correlationKey(23);
        fixture.proofSubjects.arm(selected, selectedKey);
        fixture.proofSubjects.arm(stale, sharedKey);
        fixture.proofSubjects.publish(
            interactionRef(1),
            CorrelationContribution.capture(sharedKey, CODEC, "same-native-flow")
        );
        fixture.proofSubjects.arm(duplicate, sharedKey);
        fixture.proofSubjects.publish(
            interactionRef(2),
            CorrelationContribution.capture(selectedKey, CODEC, "same-native-flow")
        );
        SemanticHold hold = fixture.coordinator.arm(
            selector("commit:same-native-flow").forSubject(selected).through(
                selectedKey,
                CODEC,
                evidence -> evidence.substring("commit:".length())
            ),
            MAXIMUM_HOLD
        );

        ForwardingPermit permit = fixture.coordinator.permit(
            interaction("commit:same-native-flow", 3)
        );

        assertThat(fixture.proofSubjects.correlation(
            selected,
            selectedKey,
            CODEC
        )).isInstanceOf(CorrelationResult.Unique.class);
        assertThat(fixture.proofSubjects.correlation(
            stale,
            sharedKey,
            CODEC
        )).isInstanceOf(CorrelationResult.Ambiguous.class);
        assertThat(fixture.proofSubjects.correlation(
            duplicate,
            sharedKey,
            CODEC
        )).isInstanceOf(CorrelationResult.Ambiguous.class);
        assertThat(hold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertThat(hold.cancel()).isTrue();
        assertThat(permit.awaitDecision()).isEqualTo(
            ForwardingDecision.CLOSE_SESSION
        );
    }

    @Test
    void shouldSelectDifferentNativeFlowsForDifferentSubjects() throws Exception {
        Fixture fixture = fixture();
        ProofSubjectRef leftSubject = fixture.proofSubjects.create();
        ProofSubjectRef rightSubject = fixture.proofSubjects.create();
        CorrelationKey leftKey = correlationKey(1);
        CorrelationKey rightKey = correlationKey(2);
        fixture.proofSubjects.arm(leftSubject, leftKey);
        fixture.proofSubjects.arm(rightSubject, rightKey);
        SemanticHold left = fixture.coordinator.arm(
            selector("left").forSubject(leftSubject).through(
                leftKey,
                CODEC,
                evidence -> evidence
            ),
            MAXIMUM_HOLD
        );
        SemanticHold right = fixture.coordinator.arm(
            selector("right").forSubject(rightSubject).through(
                rightKey,
                CODEC,
                evidence -> evidence
            ),
            MAXIMUM_HOLD
        );
        fixture.proofSubjects.publish(
            interactionRef(1),
            CorrelationContribution.capture(leftKey, CODEC, "left")
        );
        fixture.proofSubjects.publish(
            interactionRef(2),
            CorrelationContribution.capture(rightKey, CODEC, "right")
        );

        ForwardingPermit leftPermit = fixture.coordinator.permit(
            interaction("left", 3)
        );
        assertThat(left.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertThat(right.state()).isEqualTo(SemanticHoldState.ARMED);
        assertThat(left.cancel()).isTrue();
        assertThat(leftPermit.awaitDecision()).isEqualTo(
            ForwardingDecision.CLOSE_SESSION
        );

        ForwardingPermit rightPermit = fixture.coordinator.permit(
            interaction("right", 4)
        );
        assertThat(right.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        assertThat(right.cancel()).isTrue();
        assertThat(rightPermit.awaitDecision()).isEqualTo(
            ForwardingDecision.CLOSE_SESSION
        );
    }

    @Test
    void shouldFailReleaseWhenReachedNativeFlowBecomesAmbiguous() throws Exception {
        ReachedNativeFlow reached = reachedNativeFlow(9);

        reached.fixture.proofSubjects.publish(
            interactionRef(3),
            reached.contribution
        );
        var release = reached.hold.release();

        assertThat(reached.permit.awaitDecision())
            .isEqualTo(ForwardingDecision.CLOSE_SESSION);
        assertThat(reached.hold.state()).isEqualTo(SemanticHoldState.FAILED);
        assertThat(release.toCompletableFuture()).isCompletedExceptionally();
        assertThat(reached.fixture.proofSubjects.correlation(
            reached.subject,
            reached.key,
            CODEC
        )).isInstanceOf(CorrelationResult.Ambiguous.class);
        assertThat(events(reached.fixture).getLast().failure())
            .contains(SemanticHoldFailure.CORRELATION_INVALIDATED);
        assertThat(rendered(reached.fixture))
            .doesNotContain("native-flow-secret-9");
    }

    @Test
    void shouldAllowReleaseAfterAnIdempotentRepeatOfTheOriginatingContribution()
        throws Exception {
        ReachedNativeFlow reached = reachedNativeFlow(10);

        reached.fixture.proofSubjects.publish(
            reached.originatingInteraction,
            reached.contribution
        );
        var release = reached.hold.release();

        assertThat(reached.permit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
        reached.permit.forwarded();
        assertThat(await(release)).isNull();
        assertThat(reached.fixture.proofSubjects.correlation(
            reached.subject,
            reached.key,
            CODEC
        )).isInstanceOf(CorrelationResult.Unique.class);
    }

    @RepeatedTest(20)
    void shouldLinearizeReleaseAgainstCorrelationInvalidationInBothOrders(
        RepetitionInfo repetition
    ) throws Exception {
        ReachedNativeFlow reached = reachedNativeFlow(20 + repetition.getCurrentRepetition());
        boolean invalidationFirst = repetition.getCurrentRepetition() % 2 == 1;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch firstLinearized = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<java.util.concurrent.CompletionStage<Void>> releasing = executor.submit(() -> {
                awaitLatch(start);
                if (invalidationFirst) {
                    awaitLatch(firstLinearized);
                }
                java.util.concurrent.CompletionStage<Void> release = reached.hold.release();
                if (!invalidationFirst) {
                    firstLinearized.countDown();
                }
                return release;
            });
            Future<?> invalidating = executor.submit(() -> {
                awaitLatch(start);
                if (!invalidationFirst) {
                    awaitLatch(firstLinearized);
                }
                reached.fixture.proofSubjects.publish(
                    interactionRef(3),
                    reached.contribution
                );
                if (invalidationFirst) {
                    firstLinearized.countDown();
                }
                return null;
            });

            start.countDown();
            java.util.concurrent.CompletionStage<Void> release = releasing.get(
                10,
                TimeUnit.SECONDS
            );
            invalidating.get(10, TimeUnit.SECONDS);

            if (invalidationFirst) {
                assertThat(reached.permit.awaitDecision())
                    .isEqualTo(ForwardingDecision.CLOSE_SESSION);
                assertThat(reached.hold.state()).isEqualTo(SemanticHoldState.FAILED);
                assertThatThrownBy(release.toCompletableFuture()::join)
                    .isInstanceOf(CompletionException.class);
            } else {
                assertThat(reached.permit.awaitDecision())
                    .isEqualTo(ForwardingDecision.FORWARD);
                reached.permit.forwarded();
                assertThat(await(release)).isNull();
            }
            assertThat(reached.fixture.proofSubjects.correlation(
                reached.subject,
                reached.key,
                CODEC
            )).isInstanceOf(CorrelationResult.Ambiguous.class);
        }
    }

    @Test
    void shouldRequireNativeFlowKeyToBeArmedForSelectedSubject() {
        Fixture fixture = fixture();
        ProofSubjectRef subject = fixture.proofSubjects.create();

        assertThatThrownBy(() -> fixture.coordinator.arm(
            selector("target").forSubject(subject).through(
                correlationKey(1),
                CODEC,
                evidence -> evidence
            ),
            MAXIMUM_HOLD
        )).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("is not armed for the selected proof subject");
    }

    @Test
    void shouldNotReachSubjectBoundHoldForInteractionUniqueToTwoSubjects()
        throws Exception {
        Fixture fixture = fixture();
        ProofSubjectRef leftSubject = fixture.proofSubjects.create();
        ProofSubjectRef rightSubject = fixture.proofSubjects.create();
        CorrelationKey leftKey = correlationKey(1);
        CorrelationKey rightKey = correlationKey(2);
        fixture.proofSubjects.arm(leftSubject, leftKey);
        fixture.proofSubjects.arm(rightSubject, rightKey);
        SemanticHold left = fixture.coordinator.arm(
            selector("shared-evidence").forSubject(leftSubject),
            MAXIMUM_HOLD
        );
        RecordedInteraction shared = interaction("shared-evidence", 1);
        fixture.proofSubjects.publish(
            shared.interactionRef(),
            CorrelationContribution.capture(leftKey, CODEC, "left-native")
        );
        fixture.proofSubjects.publish(
            shared.interactionRef(),
            CorrelationContribution.capture(rightKey, CODEC, "right-native")
        );

        assertImmediate(fixture.coordinator.permit(shared));

        assertThat(left.state()).isEqualTo(SemanticHoldState.ARMED);
        assertThat(left.cancel()).isTrue();
    }

    @Test
    void shouldCancelArmedAndHeldControlsOnTeardownButNotRevokeWonRelease()
        throws Exception {
        Fixture fixture = fixture();
        SemanticHold armed = fixture.coordinator.arm(selector("never"), MAXIMUM_HOLD);
        SemanticHold held = fixture.coordinator.arm(selector("held"), MAXIMUM_HOLD);
        SemanticHold releasing = fixture.coordinator.arm(selector("release"), MAXIMUM_HOLD);
        ForwardingPermit heldPermit = fixture.coordinator.permit(interaction("held", 1));
        ForwardingPermit releasingPermit = fixture.coordinator.permit(
            interaction("release", 2)
        );
        var release = releasing.release();

        fixture.coordinator.completeExecution();

        assertThat(armed.state()).isEqualTo(SemanticHoldState.CANCELLED);
        assertThat(held.state()).isEqualTo(SemanticHoldState.CANCELLED);
        assertThat(heldPermit.awaitDecision()).isEqualTo(
            ForwardingDecision.CLOSE_SESSION
        );
        assertThat(releasingPermit.awaitDecision()).isEqualTo(
            ForwardingDecision.FORWARD
        );
        releasingPermit.forwarded();
        assertThat(await(release)).isNull();
        assertThat(releasing.state()).isEqualTo(SemanticHoldState.FORWARDED);
        assertImmediate(fixture.coordinator.permit(interaction("cleanup", 3)));
        assertThatThrownBy(() -> fixture.coordinator.arm(
            selector("late"),
            MAXIMUM_HOLD
        )).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("cannot arm semantic controls");
        assertThat(fixture.scheduler.closed).isTrue();
    }

    private static Fixture fixture() {
        return fixture(SemanticControlCapabilityRegistry.Availability.DECLARED);
    }

    private static Fixture fixture(
        SemanticControlCapabilityRegistry.Availability availability
    ) {
        return fixture(availability, new ConnectionId[0]);
    }

    private static Fixture fixture(
        SemanticControlCapabilityRegistry.Availability availability,
        ConnectionId... additionalConnections
    ) {
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
            CONNECTION,
            () -> availability,
            Optional.of(requiredObservationProfile())
        );
        for (ConnectionId connection : additionalConnections) {
            capabilities.register(
                connection,
                () -> availability,
                Optional.of(requiredObservationProfile())
            );
        }
        return new Fixture(
            new SemanticControlCoordinator(
                events,
                proofSubjects,
                capabilities,
                scheduler
            ),
            proofSubjects,
            journal,
            scheduler
        );
    }

    private static SemanticInteractionSelector<String> selector(String expected) {
        return selector(CONNECTION, expected);
    }

    private static RequiredObservationProfile requiredObservationProfile() {
        return new RequiredObservationProfile(
            CODEC.schemaId(),
            Optional.of(CODEC.schemaId()),
            Set.of(
                Capability.CORRELATION_CONTRIBUTIONS,
                Capability.SEMANTIC_CONTROL
            ),
            Set.of()
        );
    }

    private static SemanticInteractionSelector<String> selector(
        ConnectionId connectionId,
        String expected
    ) {
        return SemanticInteractionSelector.matching(
            connectionId,
            FlowDirection.CONSUMER_TO_PROVIDER,
            CODEC,
            expected::equals
        );
    }

    private static RecordedInteraction interaction(String evidence, long ordinal) {
        return interaction(
            CONNECTION,
            FlowDirection.CONSUMER_TO_PROVIDER,
            CODEC,
            evidence,
            ordinal
        );
    }

    private static RecordedInteraction interaction(
        ConnectionId connectionId,
        FlowDirection direction,
        EvidenceCodec<String> codec,
        String evidence,
        long ordinal
    ) {
        return interaction(connectionId, direction, codec, evidence, 1, ordinal);
    }

    private static RecordedInteraction interaction(
        ConnectionId connectionId,
        FlowDirection direction,
        EvidenceCodec<String> codec,
        String evidence,
        long session,
        long ordinal
    ) {
        return new RecordedInteraction(
            new InteractionRef(new SessionId(connectionId, session), direction, ordinal),
            EvidenceSnapshot.capture(codec, evidence)
        );
    }

    private static InteractionRef interactionRef(long ordinal) {
        return interactionRef(
            CONNECTION,
            FlowDirection.CONSUMER_TO_PROVIDER,
            1,
            ordinal
        );
    }

    private static InteractionRef interactionRef(
        ConnectionId connection,
        FlowDirection direction,
        long session,
        long ordinal
    ) {
        return new InteractionRef(
            new SessionId(connection, session),
            direction,
            ordinal
        );
    }

    private static ReachedNativeFlow reachedNativeFlow(int seed) {
        Fixture fixture = fixture();
        ProofSubjectRef subject = fixture.proofSubjects.create();
        CorrelationKey key = correlationKey(seed);
        String nativeReference = "native-flow-secret-" + seed;
        CorrelationContribution<String> contribution = CorrelationContribution.capture(
            key,
            CODEC,
            nativeReference
        );
        InteractionRef originatingInteraction = interactionRef(1);
        fixture.proofSubjects.arm(subject, key);
        fixture.proofSubjects.publish(originatingInteraction, contribution);
        SemanticHold hold = fixture.coordinator.arm(
            SemanticInteractionSelector.matching(
                CONNECTION,
                FlowDirection.CONSUMER_TO_PROVIDER,
                CODEC,
                ("commit:" + nativeReference)::equals
            ).forSubject(subject).through(
                key,
                CODEC,
                evidence -> evidence.substring("commit:".length())
            ),
            MAXIMUM_HOLD
        );
        ForwardingPermit permit = fixture.coordinator.permit(
            interaction("commit:" + nativeReference, 2)
        );
        assertThat(hold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
        return new ReachedNativeFlow(
            fixture,
            subject,
            key,
            originatingInteraction,
            contribution,
            hold,
            permit
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

    private static CorrelationKey correlationKey(int seed) {
        byte[] digest = new byte[16];
        java.util.Arrays.fill(digest, (byte) seed);
        return CorrelationKey.ofDigest(
            new CorrelationKeySchema("test", "isolated", 1),
            digest
        );
    }

    private static void assertImmediate(ForwardingPermit permit) throws Exception {
        assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
        permit.forwarded();
    }

    private static List<SemanticHoldEvent> events(Fixture fixture) {
        return fixture.journal.snapshot().entries().stream()
            .map(entry -> entry.event())
            .filter(SemanticHoldEvent.class::isInstance)
            .map(SemanticHoldEvent.class::cast)
            .toList();
    }

    private static List<SemanticHoldState> states(Fixture fixture) {
        return events(fixture).stream().map(SemanticHoldEvent::state).toList();
    }

    private static String rendered(Fixture fixture) {
        return new io.github.jacekkardys.systemproof.diagnostics.JournalRenderer()
            .render(fixture.journal.snapshot());
    }

    private static boolean terminal(SemanticHoldState state) {
        return state == SemanticHoldState.FORWARDED
            || state == SemanticHoldState.CANCELLED
            || state == SemanticHoldState.TIMED_OUT
            || state == SemanticHoldState.FAILED;
    }

    private static <T> T await(java.util.concurrent.CompletionStage<T> stage)
        throws Exception {
        return stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Race did not start");
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

    private record ReachedNativeFlow(
        Fixture fixture,
        ProofSubjectRef subject,
        CorrelationKey key,
        InteractionRef originatingInteraction,
        CorrelationContribution<String> contribution,
        SemanticHold hold,
        ForwardingPermit permit
    ) {}

    private static final class ImmediateTimeoutScheduler
        implements SemanticControlCoordinator.TimeoutScheduler {
        private boolean cancelled;

        @Override
        public SemanticControlCoordinator.TimeoutTask schedule(
            Duration delay,
            Runnable action
        ) {
            assertThat(delay).isPositive();
            action.run();
            return () -> cancelled = true;
        }

        @Override
        public void close() {
        }
    }

    private static final class ManualTimeoutScheduler
        implements SemanticControlCoordinator.TimeoutScheduler {
        private final List<ScheduledAction> actions = new ArrayList<>();
        private boolean closed;

        @Override
        public synchronized SemanticControlCoordinator.TimeoutTask schedule(
            Duration delay,
            Runnable action
        ) {
            assertThat(delay).isPositive();
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

        private synchronized int activeTasks() {
            return Math.toIntExact(actions.stream()
                .filter(action -> !action.cancelled && !action.ran)
                .count());
        }

        @Override
        public synchronized void close() {
            closed = true;
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
