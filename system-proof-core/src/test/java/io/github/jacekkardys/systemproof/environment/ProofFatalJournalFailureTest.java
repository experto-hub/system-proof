package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.journal.ProofSubjectCreatedEvent;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;
import io.github.jacekkardys.systemproof.journal.SemanticHoldEvent;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofExecutionState;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;

class ProofFatalJournalFailureTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final String SECRET = "fatal-journal-append-canary-secret";

    @Test
    void shouldPropagateFatalSubjectAppendAndRetainEarlierCommitInTheOperation() {
        FatalNextNanoTime nanoTime = new FatalNextNanoTime();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithJournal(
            new ScenarioJournal(nanoTime)
        )) {
            ProofExecution execution = harness.activate(observationPlan(harness));
            int createdBefore = events(harness, ProofSubjectCreatedEvent.class).size();
            AtomicReference<ProofSubjectRef> committed = new AtomicReference<>();
            InjectedJournalLinkageError fatal = new InjectedJournalLinkageError(SECRET);

            Throwable thrown = catchThrowable(() -> harness.events.proofFactBatch(() -> {
                committed.set(harness.proofSubjects.create());
                nanoTime.failNext(fatal);
                harness.proofSubjects.create();
                return null;
            }));

            assertThat(thrown).isSameAs(fatal);
            assertThat(events(harness, ProofSubjectCreatedEvent.class))
                .hasSize(createdBefore + 1)
                .filteredOn(event -> event.proofSubject().equals(committed.get()))
                .singleElement();
            harness.proofSubjects.validateSubject(committed.get());
            assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);
            assertUnpublished(execution);
            assertSecretAbsent(harness);
        }
    }

    @Test
    void shouldPropagateFatalHoldAppendWithoutStateFactOrOutcomeMutation() {
        FatalNextNanoTime nanoTime = new FatalNextNanoTime();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithJournal(
            new ScenarioJournal(nanoTime)
        )) {
            SemanticHold hold = harness.declareHold("held");
            ProofExecution execution = harness.activate(holdPlan(harness, hold));
            ProofRuntimeHarness.Recorded recorded = harness.record("held");
            harness.correlate(recorded, "held");
            int reachedBefore = events(harness, SemanticHoldEvent.class).stream()
                .filter(event -> event.holdRef().equals(hold.ref()))
                .filter(event -> event.state() == SemanticHoldState.REACHED_HELD)
                .toList().size();
            InjectedJournalLinkageError fatal = new InjectedJournalLinkageError(SECRET);
            nanoTime.failNext(fatal);

            Throwable thrown = catchThrowable(() ->
                harness.route.coordinator().permit(recorded.interaction())
            );

            assertThat(thrown).isSameAs(fatal);
            assertThat(hold.state()).isEqualTo(SemanticHoldState.ARMED);
            assertThat(events(harness, SemanticHoldEvent.class).stream()
                .filter(event -> event.holdRef().equals(hold.ref()))
                .filter(event -> event.state() == SemanticHoldState.REACHED_HELD)
                .count()).isEqualTo(reachedBefore);
            assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);
            assertUnpublished(execution);
            assertSecretAbsent(harness);
        }
    }

    @Test
    void shouldRejectFatalJournalFailureObserverCallsBeforeCreatingAnIntent() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = harness.activate(observationPlan(harness));
            InjectedJournalLinkageError linkage =
                new InjectedJournalLinkageError(SECRET);
            InjectedVirtualMachineError virtualMachine =
                new InjectedVirtualMachineError(SECRET);

            Throwable nested = catchThrowable(() -> harness.events.proofFactBatch(() -> {
                harness.proofs.journalFailure(linkage);
                return null;
            }));
            Throwable direct = catchThrowable(() ->
                harness.proofs.journalFailure(virtualMachine)
            );

            assertThat(nested).isSameAs(linkage);
            assertThat(direct).isSameAs(virtualMachine);
            assertThat(execution.state()).isEqualTo(ProofExecutionState.ACTIVE);
            assertUnpublished(execution);
            assertSecretAbsent(harness);
        }
    }

    private static ProofPlan observationPlan(ProofRuntimeHarness harness) {
        return basePlan(harness, "fatal-journal-observer").build();
    }

    private static ProofPlan holdPlan(
        ProofRuntimeHarness harness,
        SemanticHold hold
    ) {
        return basePlan(harness, "fatal-journal-hold")
            .control("hold", hold, SemanticHoldState.FORWARDED)
            .build();
    }

    private static ProofPlan.Builder basePlan(
        ProofRuntimeHarness harness,
        String id
    ) {
        return ProofPlan.builder(id, "Fatal journal failure propagation", harness.subject, DEADLINE)
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

    private static void assertUnpublished(ProofExecution execution) {
        ProofExecutionCoordinator.PublicationInvariant invariant =
            ProofExecutionCoordinator.publicationInvariant(execution);
        assertThat(invariant.outcomeSelected()).isFalse();
        assertThat(invariant.resultReadyCompletedNormally()).isFalse();
        assertThat(invariant.finalizationReadyCompletedNormally()).isFalse();
        assertThat(invariant.finalizationComplete()).isFalse();
        assertThat(invariant.finalizing()).isFalse();
        assertThat(invariant.finalizationOwnerPresent()).isFalse();
        assertThat(invariant.authoritativeOutcomeBoundaryPending()).isFalse();
        assertThat(invariant.deadlineInstallationReadinessPresent()).isFalse();
        assertThat(invariant.authoritativeOperationOwnerPresent()).isFalse();
        assertThat(invariant.factBatchActive()).isFalse();
        assertThat(invariant.pendingCompletionPresent()).isFalse();
        assertThat(invariant.resultConstructionRecoveryCount()).isZero();
    }

    private static void assertSecretAbsent(ProofRuntimeHarness harness) {
        assertThat(harness.journal.snapshot().toString()).doesNotContain(SECRET);
    }

    private static final class FatalNextNanoTime implements LongSupplier {
        private final AtomicReference<Error> failure = new AtomicReference<>();

        @Override
        public long getAsLong() {
            Error fatal = failure.getAndSet(null);
            if (fatal != null) {
                throw fatal;
            }
            return 0L;
        }

        private void failNext(Error fatal) {
            assertThat(failure.compareAndSet(null, fatal)).isTrue();
        }
    }

    private static final class InjectedJournalLinkageError extends LinkageError {
        private InjectedJournalLinkageError(String message) {
            super(message);
        }
    }

    private static final class InjectedVirtualMachineError extends VirtualMachineError {
        private InjectedVirtualMachineError(String message) {
            super(message);
        }
    }
}
