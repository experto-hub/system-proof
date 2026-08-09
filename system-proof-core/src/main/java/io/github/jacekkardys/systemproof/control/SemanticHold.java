package io.github.jacekkardys.systemproof.control;

import java.util.concurrent.CompletionStage;
import io.github.jacekkardys.systemproof.observation.InteractionRef;

/**
 * One environment-owned, one-shot semantic hold handle.
 *
 * <p>Public completion roots are delivered independently after their framework state transition.
 * Synchronous dependent execution order is unspecified and never runs on the transition owner.
 */
public interface SemanticHold {
    SemanticHoldRef ref();

    SemanticHoldState state();

    /**
     * Completes after the interaction is recorded, correlated, held, and journaled before any of
     * its bytes are written downstream.
     */
    CompletionStage<InteractionRef> reached();

    /** Completes with the terminal lifecycle state after its journal fact is appended. */
    CompletionStage<SemanticHoldState> completion();

    /**
     * Authorizes exactly one forwarding attempt.
     *
     * <p>The returned stage completes only after the gateway reports successful write/flush or a
     * definitive failure. Calling this before {@link SemanticHoldState#REACHED_HELD} or more than
     * once returns an exceptionally completed stage.
     */
    CompletionStage<Void> release();

    /**
     * Cancels an armed or held control if cancellation wins the terminal transition.
     *
     * @return {@code true} when this call performed the transition
     */
    boolean cancel();
}
