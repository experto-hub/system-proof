package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.journal.CorrelationCandidateEvent;
import io.github.jacekkardys.systemproof.journal.ProofSubjectArmedEvent;
import io.github.jacekkardys.systemproof.journal.ProofSubjectCreatedEvent;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;
import io.github.jacekkardys.systemproof.journal.SemanticHoldEvent;
import io.github.jacekkardys.systemproof.journal.SemanticPredecessorGuardEvent;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.proof.CorrelationResult;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;

class ProofCriticalPublicationCommitTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final String SECRET = "proof-emitter-canary-secret";

    @Test
    void shouldCommitSubjectArmingBeforeContainingDiagnosticEmissionFailure() {
        FailNextDiagnosticSink sink = new FailNextDiagnosticSink();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithDiagnosticSink(sink)) {
            sink.failNext();
            ProofSubjectRef second = harness.proofSubjects.create();
            ProofExecution execution = harness.activate(correlationPlan(harness));

            sink.failNext();
            harness.proofSubjects.arm(second, harness.key);

            assertThat(events(harness, ProofSubjectCreatedEvent.class))
                .filteredOn(event -> event.proofSubject().equals(second))
                .singleElement();
            assertThat(harness.proofSubjects.correlation(
                second,
                harness.key,
                ProofTestFixture.NativeCodec.INSTANCE
            )).isInstanceOf(CorrelationResult.Ambiguous.class);
            assertThat(events(harness, ProofSubjectArmedEvent.class))
                .filteredOn(event -> event.proofSubject().equals(second))
                .singleElement();
            execution.runStimulus(() -> {});
            ProofResult result = execution.evaluate();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertSecretAbsent(harness);
        }
    }

    @Test
    void shouldUseCommittedUniqueCandidateForProofAndPermitAfterEmissionFailure()
        throws Exception {
        FailNextDiagnosticSink sink = new FailNextDiagnosticSink();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithDiagnosticSink(sink)) {
            SemanticHold hold = harness.declareHold("unique");
            ProofExecution execution = harness.activate(correlationAndHoldPlan(harness, hold));
            ProofRuntimeHarness.Recorded recorded = harness.record("unique");

            sink.failNext();
            harness.correlate(recorded, "unique");

            assertThat(harness.proofSubjects.correlation(
                harness.subject,
                harness.key,
                ProofTestFixture.NativeCodec.INSTANCE
            )).isInstanceOf(CorrelationResult.Unique.class);
            ForwardingPermit permit = harness.route.coordinator().permit(recorded.interaction());
            assertThat(hold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
            var release = hold.release().toCompletableFuture();
            assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.FORWARD);
            permit.forwarded();
            release.get(5, TimeUnit.SECONDS);

            execution.runStimulus(() -> {});
            ProofResult result = execution.evaluate();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.PROVED);
            assertThat(events(harness, CorrelationCandidateEvent.class))
                .filteredOn(event -> event.interactionRef().equals(
                    recorded.interaction().interactionRef()
                ))
                .singleElement();
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertSecretAbsent(harness);
        }
    }

    @Test
    void shouldCommitGuardViolationAndCloseTransportAfterEmissionFailure()
        throws Exception {
        FailNextDiagnosticSink sink = new FailNextDiagnosticSink();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithDiagnosticSink(sink)) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));
            ProofRuntimeHarness.Recorded successor = harness.record("successor");
            harness.correlate(successor, "successor");

            sink.failNext();
            ForwardingPermit permit = harness.route.coordinator().permit(
                successor.interaction()
            );

            assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.VIOLATED);
            assertThat(events(harness, SemanticPredecessorGuardEvent.class))
                .filteredOn(event -> event.guardRef().equals(guard.ref()))
                .filteredOn(event -> event.kind()
                    == SemanticPredecessorGuardEvent.Kind.TERMINAL)
                .filteredOn(event -> event.state()
                    == SemanticPredecessorGuardState.VIOLATED)
                .singleElement();
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertSecretAbsent(harness);
        }
    }

    @Test
    void shouldCommitHoldAndGuardFailureTransitionsAfterEmissionFailure()
        throws Exception {
        assertCommittedHoldTransitionAfterEmissionFailure();
        assertCommittedGuardFailureAfterEmissionFailure();
    }

    @Test
    void shouldLeaveSubjectCorrelationAndControlStateUnchangedWhenAppendFails() {
        assertSubjectCreationUnchangedAfterAppendFailure();
        assertSubjectArmUnchangedAfterAppendFailure();
        assertCorrelationUnchangedAfterAppendFailure();
        assertHoldUnchangedAfterAppendFailure();
        assertGuardUnchangedAfterAppendFailure();
        assertGuardFailureUnchangedAfterAppendFailure();
    }

    @Test
    void shouldPropagateFatalEmitterFailureWithoutNormalizingCommittedProofState() {
        AtomicBoolean failNext = new AtomicBoolean();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithDiagnosticSink(
            (level, line) -> {
                if (failNext.compareAndSet(true, false)) {
                    throw new InjectedLinkageFailure();
                }
            }
        )) {
            ProofSubjectRef second = harness.proofSubjects.create();
            ProofExecution execution = harness.activate(correlationPlan(harness));

            failNext.set(true);
            assertThatThrownBy(() -> harness.proofSubjects.arm(second, harness.key))
                .isInstanceOf(InjectedLinkageFailure.class);

            assertThat(harness.proofSubjects.correlation(
                second,
                harness.key,
                ProofTestFixture.NativeCodec.INSTANCE
            )).isInstanceOf(CorrelationResult.Ambiguous.class);
            execution.runStimulus(() -> {});
            ProofResult result = execution.evaluate();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    private static void assertCommittedHoldTransitionAfterEmissionFailure()
        throws Exception {
        FailNextDiagnosticSink sink = new FailNextDiagnosticSink();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithDiagnosticSink(sink)) {
            SemanticHold hold = harness.declareHold("held");
            ProofExecution execution = harness.activate(holdPlan(harness, hold));
            ProofRuntimeHarness.Recorded recorded = harness.record("held");
            harness.correlate(recorded, "held");

            sink.failNext();
            ForwardingPermit permit = harness.route.coordinator().permit(recorded.interaction());

            assertThat(hold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
            assertThat(events(harness, SemanticHoldEvent.class))
                .filteredOn(event -> event.holdRef().equals(hold.ref()))
                .filteredOn(event -> event.state() == SemanticHoldState.REACHED_HELD)
                .singleElement();
            assertThat(hold.cancel()).isTrue();
            assertThat(permit.awaitDecision()).isEqualTo(ForwardingDecision.CLOSE_SESSION);
            execution.runStimulus(() -> {});
            ProofResult result = execution.evaluate();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertSecretAbsent(harness);
        }
    }

    private static void assertCommittedGuardFailureAfterEmissionFailure() {
        FailNextDiagnosticSink sink = new FailNextDiagnosticSink();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithDiagnosticSink(sink)) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));

            sink.failNext();
            harness.controls.observationFailed(harness.connectionId);

            assertThat(guard.state()).isEqualTo(SemanticPredecessorGuardState.FAILED);
            assertThat(events(harness, SemanticPredecessorGuardEvent.class))
                .filteredOn(event -> event.guardRef().equals(guard.ref()))
                .filteredOn(event -> event.state()
                    == SemanticPredecessorGuardState.FAILED)
                .singleElement();
            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
            assertSecretAbsent(harness);
        }
    }

    private static void assertSubjectArmUnchangedAfterAppendFailure() {
        FailNextNanoTime nanoTime = new FailNextNanoTime();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithJournal(
            new ScenarioJournal(nanoTime)
        )) {
            ProofSubjectRef second = harness.proofSubjects.create();
            ProofExecution execution = harness.activate(correlationPlan(harness));
            int armedBefore = events(harness, ProofSubjectArmedEvent.class).size();

            nanoTime.failNext();
            assertThatThrownBy(() -> harness.proofSubjects.arm(second, harness.key))
                .isInstanceOf(InjectedJournalFailure.class);

            assertThat(events(harness, ProofSubjectArmedEvent.class)).hasSize(armedBefore);
            assertThatThrownBy(() -> harness.proofSubjects.correlation(
                second,
                harness.key,
                ProofTestFixture.NativeCodec.INSTANCE
            )).isInstanceOf(IllegalArgumentException.class);
            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    private static void assertSubjectCreationUnchangedAfterAppendFailure() {
        FailNextNanoTime nanoTime = new FailNextNanoTime();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithJournal(
            new ScenarioJournal(nanoTime)
        )) {
            ProofExecution execution = harness.activate(observationPlan(harness));
            int createdBefore = events(harness, ProofSubjectCreatedEvent.class).size();

            nanoTime.failNext();
            assertThatThrownBy(harness.proofSubjects::create)
                .isInstanceOf(InjectedJournalFailure.class);

            assertThat(events(harness, ProofSubjectCreatedEvent.class))
                .hasSize(createdBefore);
            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    private static void assertCorrelationUnchangedAfterAppendFailure() {
        FailNextNanoTime nanoTime = new FailNextNanoTime();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithJournal(
            new ScenarioJournal(nanoTime)
        )) {
            ProofExecution execution = harness.activate(correlationPlan(harness));
            ProofRuntimeHarness.Recorded recorded = harness.record("candidate");
            int candidatesBefore = events(harness, CorrelationCandidateEvent.class).size();

            nanoTime.failNext();
            assertThatThrownBy(() -> harness.correlate(recorded, "candidate"))
                .isInstanceOf(InjectedJournalFailure.class);

            assertThat(events(harness, CorrelationCandidateEvent.class))
                .hasSize(candidatesBefore);
            assertThat(harness.proofSubjects.correlation(
                harness.subject,
                harness.key,
                ProofTestFixture.NativeCodec.INSTANCE
            )).isInstanceOf(CorrelationResult.Missing.class);
            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    private static void assertHoldUnchangedAfterAppendFailure() {
        FailNextNanoTime nanoTime = new FailNextNanoTime();
        java.util.concurrent.atomic.AtomicReference<SemanticHold> selectedHold =
            new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<SemanticHoldState> boundaryState =
            new java.util.concurrent.atomic.AtomicReference<>();
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void authoritativeOutcomeSelectedBeforeFinalization() {
                boundaryState.set(selectedHold.get().state());
            }
        };
        try (ProofRuntimeHarness harness =
            ProofRuntimeHarness.startWithJournalAndBoundaryHooks(
                new ScenarioJournal(nanoTime),
                hooks
            )) {
            SemanticHold hold = harness.declareHold("held");
            selectedHold.set(hold);
            ProofExecution execution = harness.activate(holdPlan(harness, hold));
            ProofRuntimeHarness.Recorded recorded = harness.record("held");
            harness.correlate(recorded, "held");
            int reachedBefore = holdEvents(harness, hold, SemanticHoldState.REACHED_HELD);

            nanoTime.failNext();
            assertThatThrownBy(() ->
                harness.route.coordinator().permit(recorded.interaction())
            ).isInstanceOf(InjectedJournalFailure.class);

            assertThat(boundaryState.get()).isEqualTo(SemanticHoldState.ARMED);
            assertThat(holdEvents(harness, hold, SemanticHoldState.REACHED_HELD))
                .isEqualTo(reachedBefore);
            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    private static void assertGuardUnchangedAfterAppendFailure() {
        FailNextNanoTime nanoTime = new FailNextNanoTime();
        java.util.concurrent.atomic.AtomicReference<SemanticPredecessorGuard> selectedGuard =
            new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<SemanticPredecessorGuardState>
            boundaryState = new java.util.concurrent.atomic.AtomicReference<>();
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void authoritativeOutcomeSelectedBeforeFinalization() {
                boundaryState.set(selectedGuard.get().state());
            }
        };
        try (ProofRuntimeHarness harness =
            ProofRuntimeHarness.startWithJournalAndBoundaryHooks(
                new ScenarioJournal(nanoTime),
                hooks
            )) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            selectedGuard.set(guard);
            ProofExecution execution = harness.activate(guardPlan(harness, guard));
            ProofRuntimeHarness.Recorded successor = harness.record("successor");
            harness.correlate(successor, "successor");
            int terminalBefore = guardEvents(
                harness,
                guard,
                SemanticPredecessorGuardEvent.Kind.TERMINAL
            );

            nanoTime.failNext();
            assertThatThrownBy(() ->
                harness.route.coordinator().permit(successor.interaction())
            ).isInstanceOf(InjectedJournalFailure.class);

            assertThat(boundaryState.get())
                .isEqualTo(SemanticPredecessorGuardState.ARMED);
            assertThat(guardEvents(
                harness,
                guard,
                SemanticPredecessorGuardEvent.Kind.TERMINAL
            )).isEqualTo(terminalBefore);
            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    private static void assertGuardFailureUnchangedAfterAppendFailure() {
        FailNextNanoTime nanoTime = new FailNextNanoTime();
        java.util.concurrent.atomic.AtomicReference<SemanticPredecessorGuard> selectedGuard =
            new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<SemanticPredecessorGuardState>
            boundaryState = new java.util.concurrent.atomic.AtomicReference<>();
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void authoritativeOutcomeSelectedBeforeFinalization() {
                boundaryState.set(selectedGuard.get().state());
            }
        };
        try (ProofRuntimeHarness harness =
            ProofRuntimeHarness.startWithJournalAndBoundaryHooks(
                new ScenarioJournal(nanoTime),
                hooks
            )) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            selectedGuard.set(guard);
            ProofExecution execution = harness.activate(guardPlan(harness, guard));
            int failedBefore = (int) events(
                harness,
                SemanticPredecessorGuardEvent.class
            ).stream().filter(event -> event.guardRef().equals(guard.ref()))
                .filter(event -> event.state() == SemanticPredecessorGuardState.FAILED)
                .count();

            nanoTime.failNext();
            assertThatThrownBy(() ->
                harness.controls.observationFailed(harness.connectionId)
            ).isInstanceOf(InjectedJournalFailure.class);

            assertThat(boundaryState.get())
                .isEqualTo(SemanticPredecessorGuardState.ARMED);
            assertThat(events(harness, SemanticPredecessorGuardEvent.class).stream()
                .filter(event -> event.guardRef().equals(guard.ref()))
                .filter(event -> event.state() == SemanticPredecessorGuardState.FAILED)
                .count()).isEqualTo(failedBefore);
            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        }
    }

    private static ProofPlan correlationPlan(ProofRuntimeHarness harness) {
        return basePlan(harness, "critical-correlation")
            .correlation(
                "correlation",
                harness.connectionId,
                harness.key,
                ProofTestFixture.NATIVE_SCHEMA
            ).build();
    }

    private static ProofPlan observationPlan(ProofRuntimeHarness harness) {
        return basePlan(harness, "critical-observation").build();
    }

    private static ProofPlan correlationAndHoldPlan(
        ProofRuntimeHarness harness,
        SemanticHold hold
    ) {
        return basePlan(harness, "critical-correlation-hold")
            .correlation(
                "correlation",
                harness.connectionId,
                harness.key,
                ProofTestFixture.NATIVE_SCHEMA
            ).control("hold", hold, SemanticHoldState.FORWARDED)
            .build();
    }

    private static ProofPlan holdPlan(ProofRuntimeHarness harness, SemanticHold hold) {
        return basePlan(harness, "critical-hold")
            .control("hold", hold, SemanticHoldState.FORWARDED)
            .build();
    }

    private static ProofPlan guardPlan(
        ProofRuntimeHarness harness,
        SemanticPredecessorGuard guard
    ) {
        return basePlan(harness, "critical-guard")
            .control("guard", guard, SemanticPredecessorGuardState.SATISFIED)
            .build();
    }

    private static ProofPlan.Builder basePlan(
        ProofRuntimeHarness harness,
        String id
    ) {
        return ProofPlan.builder(id, "Proof-critical publication commit", harness.subject, DEADLINE)
            .prerequisite("prerequisite", harness.prerequisite())
            .observation("observation", harness.connectionId, ProofTestFixture.PROFILE);
    }

    private static <T extends ScenarioEvent> List<T> events(
        ProofRuntimeHarness harness,
        Class<T> eventType
    ) {
        return harness.journal.snapshot().entries().stream()
            .map(entry -> entry.event())
            .filter(eventType::isInstance)
            .map(eventType::cast)
            .toList();
    }

    private static int holdEvents(
        ProofRuntimeHarness harness,
        SemanticHold hold,
        SemanticHoldState state
    ) {
        return (int) events(harness, SemanticHoldEvent.class).stream()
            .filter(event -> event.holdRef().equals(hold.ref()))
            .filter(event -> event.state() == state)
            .count();
    }

    private static int guardEvents(
        ProofRuntimeHarness harness,
        SemanticPredecessorGuard guard,
        SemanticPredecessorGuardEvent.Kind kind
    ) {
        return (int) events(harness, SemanticPredecessorGuardEvent.class).stream()
            .filter(event -> event.guardRef().equals(guard.ref()))
            .filter(event -> event.kind() == kind)
            .count();
    }

    private static void assertSecretAbsent(ProofRuntimeHarness harness) {
        assertThat(harness.journal.snapshot().toString()).doesNotContain(SECRET);
    }

    private static final class FailNextDiagnosticSink
        implements java.util.function.BiConsumer<
            io.github.jacekkardys.systemproof.journal.LogLevel,
            String
        > {
        private final AtomicBoolean failNext = new AtomicBoolean();

        @Override
        public void accept(
            io.github.jacekkardys.systemproof.journal.LogLevel level,
            String line
        ) {
            if (failNext.compareAndSet(true, false)) {
                throw new InjectedDiagnosticFailure(SECRET);
            }
        }

        private void failNext() {
            assertThat(failNext.compareAndSet(false, true)).isTrue();
        }
    }

    private static final class FailNextNanoTime implements LongSupplier {
        private final AtomicBoolean failNext = new AtomicBoolean();

        @Override
        public long getAsLong() {
            if (failNext.compareAndSet(true, false)) {
                throw new InjectedJournalFailure();
            }
            return 0L;
        }

        private void failNext() {
            assertThat(failNext.compareAndSet(false, true)).isTrue();
        }
    }

    private static final class InjectedDiagnosticFailure extends RuntimeException {
        private InjectedDiagnosticFailure(String message) {
            super(message);
        }
    }

    private static final class InjectedJournalFailure extends RuntimeException {}

    private static final class InjectedLinkageFailure extends LinkageError {}
}
