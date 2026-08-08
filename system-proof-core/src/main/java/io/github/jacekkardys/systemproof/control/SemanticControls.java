package io.github.jacekkardys.systemproof.control;

import java.time.Duration;

/** Environment-scoped facade for at most 256 one-shot semantic traffic controls. */
public interface SemanticControls {
    /** Declares one hold for later all-or-nothing proof-plan activation. */
    <T> SemanticHold declareHold(
        SemanticInteractionSelector<T> selector,
        Duration maximumHoldDuration
    );

    /** Declares one predecessor guard for later all-or-nothing proof-plan activation. */
    SemanticPredecessorGuard declareGuard(SemanticPredecessorGuardSpec specification);

    /**
     * Arms one selector before its stimulus.
     *
     * <p>The maximum duration starts when the selected interaction reaches the held state, not
     * while the selector is merely armed.
     */
    <T> SemanticHold arm(
        SemanticInteractionSelector<T> selector,
        Duration maximumHoldDuration
    );

    /** Arms one subject-scoped predecessor guard before either selected interaction occurs. */
    SemanticPredecessorGuard guard(SemanticPredecessorGuardSpec specification);
}
