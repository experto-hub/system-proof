package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.RepeatedTest;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResult;

class ProofFinalizationHandoffTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);

    @RepeatedTest(20)
    void shouldWakeConcurrentAccessorsAfterADirectAuthoritativeBoundary() throws Exception {
        PausingBoundary hooks = new PausingBoundary();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks);
             ExecutorService executor = Executors.newFixedThreadPool(3)) {
            ProofExecution execution = harness.activate(observationPlan(harness));

            Future<?> operation = executor.submit(() -> harness.events.proofFactBatch(() -> {
                harness.proofs.requiredObservationFailed(harness.connectionId);
                return null;
            }));
            hooks.awaitSelected();

            Future<ProofResult> resultAccess = executor.submit(execution::result);
            Future<ProofResult> evaluation = executor.submit(execution::evaluate);
            assertThat(resultAccess.isDone()).isFalse();
            assertThat(evaluation.isDone()).isFalse();

            hooks.release();
            operation.get(5, TimeUnit.SECONDS);
            ProofResult result = resultAccess.get(5, TimeUnit.SECONDS);
            assertThat(evaluation.get(5, TimeUnit.SECONDS)).isSameAs(result);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        } finally {
            hooks.release();
        }
    }

    @RepeatedTest(20)
    void shouldFinalizeAfterASubjectJournalFailureWithoutAControlHandoff()
        throws Exception {
        PausingBoundary hooks = new PausingBoundary();
        FailNextNanoTime nanoTime = new FailNextNanoTime();
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithJournalAndBoundaryHooks(
                new ScenarioJournal(nanoTime),
                hooks
            );
             ExecutorService executor = Executors.newFixedThreadPool(3)) {
            ProofExecution execution = harness.activate(observationPlan(harness));
            nanoTime.failNext();

            Future<?> operation = executor.submit(() -> {
                try {
                    harness.proofSubjects.create();
                } catch (InjectedJournalFailure expected) {
                    // The failed append is the test boundary.
                }
            });
            hooks.awaitSelected();
            Future<ProofResult> resultAccess = executor.submit(execution::result);
            Future<ProofResult> evaluation = executor.submit(execution::evaluate);

            hooks.release();
            operation.get(5, TimeUnit.SECONDS);
            ProofResult result = resultAccess.get(5, TimeUnit.SECONDS);
            assertThat(evaluation.get(5, TimeUnit.SECONDS)).isSameAs(result);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        } finally {
            hooks.release();
        }
    }

    @RepeatedTest(20)
    void shouldRunInternalPermitActionBeforePublishingAndGatePublicCompletion()
        throws Exception {
        PausingBoundary hooks = new PausingBoundary();
        AtomicReference<Future<ForwardingDecision>> decision = new AtomicReference<>();
        AtomicReference<Throwable> orderingFailure = new AtomicReference<>();
        hooks.beforeResult = () -> {
            try {
                assertThat(decision.get().get(5, TimeUnit.SECONDS))
                    .isEqualTo(ForwardingDecision.CLOSE_SESSION);
            } catch (Throwable failure) {
                orderingFailure.set(failure);
            }
        };
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks);
             ExecutorService executor = Executors.newFixedThreadPool(5)) {
            SemanticHold hold = harness.declareHold("held");
            ProofExecution execution = harness.activate(holdPlan(harness, hold));
            ProofRuntimeHarness.Recorded recorded = harness.record("held");
            harness.correlate(recorded, "held");
            ForwardingPermit permit = harness.route.coordinator().permit(recorded.interaction());
            assertThat(hold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
            decision.set(executor.submit(permit::awaitDecision));
            AtomicReference<ProofResult> callbackResult = new AtomicReference<>();
            Future<?> callback = hold.completion().thenRun(() ->
                callbackResult.set(execution.result())
            ).toCompletableFuture();

            Future<?> operation = executor.submit(() ->
                harness.controls.observationFailed(harness.connectionId)
            );
            hooks.awaitSelected();
            Future<ProofResult> resultAccess = executor.submit(execution::result);
            Future<ProofResult> evaluation = executor.submit(execution::evaluate);

            hooks.release();
            operation.get(5, TimeUnit.SECONDS);
            ProofResult result = resultAccess.get(5, TimeUnit.SECONDS);
            assertThat(evaluation.get(5, TimeUnit.SECONDS)).isSameAs(result);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(orderingFailure.get()).isNull();
            callback.get(5, TimeUnit.SECONDS);
            assertThat(callbackResult.get()).isSameAs(result);
            ProofPublicationAssertions.assertNormallyPublishedOnce(execution, result);
        } finally {
            hooks.release();
        }
    }

    private static ProofPlan observationPlan(ProofRuntimeHarness harness) {
        return ProofPlan.builder(
            "finalization-handoff-observation",
            "Finalization handoff observation",
            harness.subject,
            DEADLINE
        ).prerequisite("prerequisite", harness.prerequisite())
            .observation("observation", harness.connectionId, ProofTestFixture.PROFILE)
            .build();
    }

    private static ProofPlan holdPlan(
        ProofRuntimeHarness harness,
        SemanticHold hold
    ) {
        return ProofPlan.builder(
            "finalization-handoff-control",
            "Finalization handoff control",
            harness.subject,
            DEADLINE
        ).prerequisite("prerequisite", harness.prerequisite())
            .observation("observation", harness.connectionId, ProofTestFixture.PROFILE)
            .control("hold", hold, SemanticHoldState.FORWARDED)
            .build();
    }

    private static final class PausingBoundary implements ProofRuntimeHarness.BoundaryHooks {
        private final CountDownLatch selected = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private Runnable beforeResult = () -> {};

        @Override
        public void authoritativeOutcomeSelectedBeforeFinalization() {
            selected.countDown();
            await(release, "authoritative outcome boundary release");
        }

        @Override
        public void resultCreatedBeforeCompletionSubmission() {
            beforeResult.run();
        }

        private void awaitSelected() {
            await(selected, "authoritative outcome selection");
        }

        private void release() {
            release.countDown();
        }
    }

    private static final class FailNextNanoTime implements LongSupplier {
        private volatile boolean failNext;

        @Override
        public long getAsLong() {
            if (failNext) {
                failNext = false;
                throw new InjectedJournalFailure();
            }
            return 0L;
        }

        private void failNext() {
            failNext = true;
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

    private static final class InjectedJournalFailure extends RuntimeException {}
}
