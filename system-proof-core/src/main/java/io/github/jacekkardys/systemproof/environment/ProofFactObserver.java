package io.github.jacekkardys.systemproof.environment;

import java.util.Objects;
import java.util.function.Supplier;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;

/** Internal typed current-state sink; it owns no event history. */
interface ProofFactObserver {
    ProofFactObserver NONE = new ProofFactObserver() {};

    default void fact(ScenarioEvent event) {}

    default <T> T factBatch(Supplier<T> action) {
        return Objects.requireNonNull(action, "action must not be null").get();
    }

    default void journalFailure(Throwable failure) {}
}
