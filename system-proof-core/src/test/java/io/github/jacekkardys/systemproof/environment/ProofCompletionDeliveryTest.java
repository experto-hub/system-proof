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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResult;
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
            assertThat(notificationOrder).containsExactly("hold", "guard");

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
}
