package io.github.jacekkardys.systemproof.environment;

import java.util.Objects;
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

    default void journalFailure(Throwable failure) {}
}
