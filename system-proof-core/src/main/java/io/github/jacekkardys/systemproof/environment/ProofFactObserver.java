package io.github.jacekkardys.systemproof.environment;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;

/** Internal typed current-state sink; it owns no event history. */
interface ProofFactObserver {
    ProofFactObserver NONE = new ProofFactObserver() {};

    default void fact(ScenarioEvent event) {}

    /**
     * Applies the complete bounded fact set from one authoritative current-state operation before
     * selecting its terminal proof outcome. One owner-thread token retains successful journal
     * facts and direct journal/observation proof intents in call order; nested calls join that token
     * and do not flush it. The operation itself runs outside the proof monitor. Independent facts
     * remain ordered outside the token, and this boundary never replays the journal.
     */
    default <T> T factBatch(Supplier<T> action) {
        return Objects.requireNonNull(action, "action must not be null").get();
    }

    /**
     * Applies one authoritative operation while handing its terminal-outcome finalization boundary
     * to a caller that must first execute mandatory internal transition actions and submit gated
     * public completions. The handoff must be released outside every framework monitor.
     */
    default <T> T factBatch(
        Supplier<T> action,
        Consumer<FinalizationHandoff> handoffConsumer
    ) {
        T result = factBatch(action);
        Objects.requireNonNull(
            handoffConsumer,
            "handoffConsumer must not be null"
        ).accept(FinalizationHandoff.NONE);
        return result;
    }

    default void journalFailure(Throwable failure) {}

    @FunctionalInterface
    interface FinalizationHandoff {
        FinalizationHandoff NONE = () -> {};

        void release();
    }
}
