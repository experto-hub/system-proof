package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofInteractionProvenance;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.proof.ProofRequirementKind;
import org.junit.jupiter.api.RepeatedTest;

class ProofCompletionDeliveryTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);

    @RepeatedTest(20)
    void shouldKeepBlockingPublicCompletionCallbacksOutsideProofFinalization()
        throws Exception {
        ProofRuntimeHarness harness = ProofRuntimeHarness.start();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch callbackRelease = new CountDownLatch(1);
        List<String> notificationOrder = new CopyOnWriteArrayList<>();
        AtomicReference<ProofResult> callbackResult = new AtomicReference<>();
        AtomicReference<ProofResult> callbackEvaluation = new AtomicReference<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(4)) {
            SemanticHold hold = harness.declareHold("held");
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(controlPlan(harness, hold, guard));

            CompletableFuture<Void> holdNotification = hold.completion()
                .thenAccept(state -> notificationOrder.add("hold"))
                .toCompletableFuture();
            CompletableFuture<Void> failedCallback = hold.completion()
                .thenRun(() -> {
                    throw new CallbackFailure();
                })
                .toCompletableFuture();
            CompletableFuture<Void> blockingCallback = guard.completion()
                .thenAccept(state -> {
                    notificationOrder.add("guard");
                    callbackResult.set(execution.result());
                    callbackEvaluation.set(execution.evaluate());
                    callbackEntered.countDown();
                    awaitUninterruptibly(callbackRelease);
                })
                .toCompletableFuture();

            Future<?> stimulus = executor.submit(() ->
                execution.runStimulus(() -> harness.publish("held"))
            );
            hold.reached().toCompletableFuture().get(5, TimeUnit.SECONDS);

            Future<?> terminalOperation = executor.submit(
                harness.deadlines::fireRacingCallback
            );
            terminalOperation.get(5, TimeUnit.SECONDS);
            assertThat(callbackEntered.await(5, TimeUnit.SECONDS)).isTrue();

            Future<ProofResult> resultAccess = executor.submit(execution::result);
            Future<ProofResult> evaluation = executor.submit(execution::evaluate);
            stimulus.get(5, TimeUnit.SECONDS);
            ProofResult frozen = resultAccess.get(5, TimeUnit.SECONDS);
            assertThat(evaluation.get(5, TimeUnit.SECONDS)).isSameAs(frozen);
            assertThat(callbackResult.get()).isSameAs(frozen);
            assertThat(callbackEvaluation.get()).isSameAs(frozen);
            assertThat(frozen.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            assertThat(notificationOrder).containsExactlyInAnyOrder("hold", "guard");

            Future<?> environmentClose = executor.submit(harness::close);
            environmentClose.get(5, TimeUnit.SECONDS);
            assertThat(execution.result()).isSameAs(frozen);

            callbackRelease.countDown();
            holdNotification.get(5, TimeUnit.SECONDS);
            blockingCallback.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(failedCallback::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(CallbackFailure.class);
            assertThat(execution.result()).isSameAs(frozen);
        } finally {
            callbackRelease.countDown();
            harness.close();
        }
    }

    @RepeatedTest(20)
    void shouldNotLetASatisfiedGuardCallbackBlockStimulusOrEvaluation() throws Exception {
        ProofRuntimeHarness harness = ProofRuntimeHarness.start();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch callbackRelease = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));
            CompletableFuture<Void> callback = guard.completion().thenRun(() -> {
                callbackEntered.countDown();
                awaitUninterruptibly(callbackRelease);
            }).toCompletableFuture();

            Future<?> stimulus = executor.submit(() -> execution.runStimulus(() -> {
                harness.publish("predecessor");
                harness.publish("successor");
            }));

            stimulus.get(5, TimeUnit.SECONDS);
            assertThat(callbackEntered.await(5, TimeUnit.SECONDS)).isTrue();
            ProofResult result = executor.submit(execution::evaluate)
                .get(5, TimeUnit.SECONDS);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.PROVED);
            assertThat(execution.result()).isSameAs(result);
            assertControlRoles(
                result,
                ProofInteractionProvenance.Role.PREDECESSOR,
                ProofInteractionProvenance.Role.SUCCESSOR
            );

            callbackRelease.countDown();
            callback.get(5, TimeUnit.SECONDS);
        } finally {
            callbackRelease.countDown();
            harness.close();
        }
    }

    @RepeatedTest(20)
    void shouldNotLetAViolatedGuardCallbackBlockStimulusOrResultAccess()
        throws Exception {
        ProofRuntimeHarness harness = ProofRuntimeHarness.start();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch callbackRelease = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));
            CompletableFuture<Void> callback = guard.completion().thenRun(() -> {
                callbackEntered.countDown();
                awaitUninterruptibly(callbackRelease);
            }).toCompletableFuture();

            Future<?> stimulus = executor.submit(() ->
                execution.runStimulus(() -> harness.publish("successor"))
            );

            stimulus.get(5, TimeUnit.SECONDS);
            assertThat(callbackEntered.await(5, TimeUnit.SECONDS)).isTrue();
            ProofResult result = executor.submit(execution::result)
                .get(5, TimeUnit.SECONDS);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.VIOLATED);
            assertThat(execution.evaluate()).isSameAs(result);
            assertControlRoles(result, ProofInteractionProvenance.Role.SUCCESSOR);

            callbackRelease.countDown();
            callback.get(5, TimeUnit.SECONDS);
        } finally {
            callbackRelease.countDown();
            harness.close();
        }
    }

    @RepeatedTest(20)
    void shouldNotLetAHoldReachedCallbackBlockPermitOrChangeTheProofOutcome()
        throws Exception {
        ProofRuntimeHarness harness = ProofRuntimeHarness.start();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch callbackRelease = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            SemanticHold hold = harness.declareHold("held");
            ProofExecution execution = harness.activate(holdPlan(harness, hold));
            CompletableFuture<Void> callback = hold.reached().thenRun(() -> {
                callbackEntered.countDown();
                awaitUninterruptibly(callbackRelease);
            }).toCompletableFuture();

            Future<?> stimulus = executor.submit(() -> execution.runStimulus(() -> {
                ProofRuntimeHarness.Recorded recorded = harness.record("held");
                harness.correlate(recorded, "held");
                ForwardingPermit permit = harness.route.coordinator()
                    .permit(recorded.interaction());
                assertThat(awaitDecision(permit)).isEqualTo(ForwardingDecision.FORWARD);
                permit.forwarded();
            }));

            assertThat(callbackEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(hold.release().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isNull();
            stimulus.get(5, TimeUnit.SECONDS);
            ProofResult result = executor.submit(execution::evaluate)
                .get(5, TimeUnit.SECONDS);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.PROVED);
            assertControlRoles(result, ProofInteractionProvenance.Role.HOLD);

            callbackRelease.countDown();
            callback.get(5, TimeUnit.SECONDS);
        } finally {
            callbackRelease.countDown();
            harness.close();
        }
    }

    @RepeatedTest(20)
    void shouldCompleteLaterCancelledRootsWhileTheFirstCallbackIsBlocked()
        throws Exception {
        ProofRuntimeHarness harness = ProofRuntimeHarness.start();
        CountDownLatch firstCallbackEntered = new CountDownLatch(1);
        CountDownLatch firstCallbackRelease = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            SemanticHold first = harness.declareHold("first");
            SemanticHold second = harness.declareHold("second");
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(
                cancellationPlan(harness, first, second, guard)
            );
            CompletableFuture<Void> firstCallback = first.completion().thenRun(() -> {
                firstCallbackEntered.countDown();
                awaitUninterruptibly(firstCallbackRelease);
            }).toCompletableFuture();
            CompletableFuture<SemanticHoldState> secondRoot = second.completion()
                .toCompletableFuture();
            CompletableFuture<SemanticPredecessorGuardState> guardRoot = guard.completion()
                .toCompletableFuture();

            executor.submit(harness.deadlines::fireRacingCallback)
                .get(5, TimeUnit.SECONDS);
            assertThat(firstCallbackEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(secondRoot.get(5, TimeUnit.SECONDS))
                .isEqualTo(SemanticHoldState.CANCELLED);
            assertThat(guardRoot.get(5, TimeUnit.SECONDS))
                .isEqualTo(SemanticPredecessorGuardState.CANCELLED);
            assertThat(execution.result().outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);

            firstCallbackRelease.countDown();
            firstCallback.get(5, TimeUnit.SECONDS);
        } finally {
            firstCallbackRelease.countDown();
            harness.close();
        }
    }

    @RepeatedTest(20)
    void shouldPublishOnlyAfterCompletionSubmissionAndLetCloseWaitForTheHandoff()
        throws Exception {
        CountDownLatch resultCreated = new CountDownLatch(1);
        CountDownLatch allowSubmission = new CountDownLatch(1);
        ProofRuntimeHarness.BoundaryHooks hooks = new ProofRuntimeHarness.BoundaryHooks() {
            @Override
            public void resultCreatedBeforeCompletionSubmission() {
                resultCreated.countDown();
                awaitUninterruptibly(allowSubmission);
            }
        };
        ProofRuntimeHarness harness = ProofRuntimeHarness.startWithBoundaryHooks(hooks);
        try (ExecutorService executor = Executors.newFixedThreadPool(3)) {
            SemanticHold hold = harness.declareHold("held");
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(controlPlan(harness, hold, guard));
            CompletableFuture<SemanticHoldState> holdRoot = hold.completion()
                .toCompletableFuture();
            CompletableFuture<SemanticPredecessorGuardState> guardRoot = guard.completion()
                .toCompletableFuture();

            Future<?> finalizer = executor.submit(harness.deadlines::fireRacingCallback);
            assertThat(resultCreated.await(5, TimeUnit.SECONDS)).isTrue();
            Future<ProofResult> resultAccess = executor.submit(execution::result);
            Future<?> environmentClose = executor.submit(harness::close);

            allowSubmission.countDown();
            finalizer.get(5, TimeUnit.SECONDS);
            ProofResult result = resultAccess.get(5, TimeUnit.SECONDS);
            environmentClose.get(5, TimeUnit.SECONDS);
            assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            assertThat(execution.result()).isSameAs(result);
            assertThat(execution.evaluate()).isSameAs(result);
            assertThat(holdRoot.get(5, TimeUnit.SECONDS))
                .isEqualTo(SemanticHoldState.CANCELLED);
            assertThat(guardRoot.get(5, TimeUnit.SECONDS))
                .isEqualTo(SemanticPredecessorGuardState.CANCELLED);
        } finally {
            allowSubmission.countDown();
            harness.close();
        }
    }

    @RepeatedTest(20)
    void shouldContainDispatchStartAndShutdownFailuresAfterResultFreeze() throws Exception {
        FailingCompletionDispatcher dispatcher = new FailingCompletionDispatcher();
        ProofRuntimeHarness harness = ProofRuntimeHarness.startWithCompletionDispatcher(
            dispatcher
        );
        SemanticHold hold = harness.declareHold("held");
        SemanticPredecessorGuard guard = harness.declareGuard();
        ProofExecution execution = harness.activate(controlPlan(harness, hold, guard));
        CompletableFuture<SemanticHoldState> holdRoot = hold.completion()
            .toCompletableFuture();
        CompletableFuture<SemanticPredecessorGuardState> guardRoot = guard.completion()
            .toCompletableFuture();

        harness.deadlines.fireRacingCallback();
        ProofResult result = execution.result();
        harness.close();

        assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
        assertThat(execution.result()).isSameAs(result);
        assertThat(execution.evaluate()).isSameAs(result);
        assertThat(holdRoot.get(5, TimeUnit.SECONDS))
            .isEqualTo(SemanticHoldState.CANCELLED);
        assertThat(guardRoot.get(5, TimeUnit.SECONDS))
            .isEqualTo(SemanticPredecessorGuardState.CANCELLED);
        assertThat(dispatcher.dispatchAttempts()).isGreaterThanOrEqualTo(4);
        assertThat(dispatcher.closeAttempts()).isEqualTo(1);
    }

    private static ProofPlan controlPlan(
        ProofRuntimeHarness harness,
        SemanticHold hold,
        SemanticPredecessorGuard guard
    ) {
        return ProofPlan.builder(
            "blocking-completion-callback",
            "Blocking completion callback",
            harness.subject,
            DEADLINE
        ).prerequisite(
            "prerequisite",
            harness.prerequisite()
        ).observation(
            "observation",
            harness.connectionId,
            ProofTestFixture.PROFILE
        ).control(
            "hold",
            hold,
            SemanticHoldState.FORWARDED
        ).control(
            "guard",
            guard,
            SemanticPredecessorGuardState.SATISFIED
        ).build();
    }

    private static ProofPlan guardPlan(
        ProofRuntimeHarness harness,
        SemanticPredecessorGuard guard
    ) {
        return ProofPlan.builder(
            "guard-completion-isolation",
            "Guard completion isolation",
            harness.subject,
            DEADLINE
        ).prerequisite(
            "prerequisite",
            harness.prerequisite()
        ).observation(
            "observation",
            harness.connectionId,
            ProofTestFixture.PROFILE
        ).control(
            "guard",
            guard,
            SemanticPredecessorGuardState.SATISFIED
        ).build();
    }

    private static ProofPlan holdPlan(
        ProofRuntimeHarness harness,
        SemanticHold hold
    ) {
        return ProofPlan.builder(
            "hold-completion-isolation",
            "Hold completion isolation",
            harness.subject,
            DEADLINE
        ).prerequisite(
            "prerequisite",
            harness.prerequisite()
        ).observation(
            "observation",
            harness.connectionId,
            ProofTestFixture.PROFILE
        ).control(
            "hold",
            hold,
            SemanticHoldState.FORWARDED
        ).build();
    }

    private static ProofPlan cancellationPlan(
        ProofRuntimeHarness harness,
        SemanticHold first,
        SemanticHold second,
        SemanticPredecessorGuard guard
    ) {
        return ProofPlan.builder(
            "cancelled-completion-isolation",
            "Cancelled completion isolation",
            harness.subject,
            DEADLINE
        ).prerequisite(
            "prerequisite",
            harness.prerequisite()
        ).observation(
            "observation",
            harness.connectionId,
            ProofTestFixture.PROFILE
        ).control(
            "first-hold",
            first,
            SemanticHoldState.FORWARDED
        ).control(
            "second-hold",
            second,
            SemanticHoldState.FORWARDED
        ).control(
            "guard",
            guard,
            SemanticPredecessorGuardState.SATISFIED
        ).build();
    }

    private static ForwardingDecision awaitDecision(ForwardingPermit permit) {
        try {
            return permit.awaitDecision();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while awaiting forwarding decision", interrupted);
        }
    }

    private static void assertControlRoles(
        ProofResult result,
        ProofInteractionProvenance.Role... expected
    ) {
        assertThat(result.resolutions())
            .filteredOn(value -> value.kind() == ProofRequirementKind.CONTROL)
            .singleElement()
            .satisfies(value -> assertThat(value.provenance())
                .extracting(ProofInteractionProvenance::role)
                .containsExactly(expected));
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class CallbackFailure extends RuntimeException {}

    private static final class FailingCompletionDispatcher
        implements SemanticControlCoordinator.CompletionDispatcher {
        private final AtomicInteger dispatchAttempts = new AtomicInteger();
        private final AtomicInteger closeAttempts = new AtomicInteger();

        @Override
        public void dispatch(
            Runnable completion,
            SemanticControlCoordinator.CompletionGate publicationGate
        ) {
            int attempt = dispatchAttempts.incrementAndGet();
            if (attempt == 1) {
                throw new RejectedExecutionException("injected rejection");
            }
            if (attempt == 2) {
                throw new ThreadStartFailure();
            }
            Thread.ofVirtual().start(() -> {
                publicationGate.awaitPublication();
                completion.run();
            });
        }

        @Override
        public void close() {
            closeAttempts.incrementAndGet();
            throw new ShutdownFailure();
        }

        private int dispatchAttempts() {
            return dispatchAttempts.get();
        }

        private int closeAttempts() {
            return closeAttempts.get();
        }
    }

    private static final class ThreadStartFailure extends RuntimeException {}

    private static final class ShutdownFailure extends RuntimeException {}
}
