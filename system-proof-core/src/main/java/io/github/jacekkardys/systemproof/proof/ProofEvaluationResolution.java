package io.github.jacekkardys.systemproof.proof;

import java.util.Objects;

/** Detached typed resolution of explicit evaluation versus the bounded proof deadline. */
public record ProofEvaluationResolution(
    ProofEvaluationState state,
    ProofResolution resolution,
    ProofResolutionReason reason
) {
    public ProofEvaluationResolution {
        state = Objects.requireNonNull(state, "state must not be null");
        resolution = Objects.requireNonNull(resolution, "resolution must not be null");
        reason = Objects.requireNonNull(reason, "reason must not be null");
        boolean valid = switch (state) {
            case COMPLETED -> resolution == ProofResolution.SATISFIED
                && reason == ProofResolutionReason.EVALUATION_COMPLETED;
            case FAILED -> resolution == ProofResolution.FAILED
                && reason == ProofResolutionReason.EVALUATION_FAILED;
            case NOT_STARTED, RUNNING -> resolution == ProofResolution.TIMED_OUT
                    && reason == ProofResolutionReason.DEADLINE_EXPIRED
                || resolution == ProofResolution.UNREACHED
                    && reason == ProofResolutionReason.EVALUATION_NOT_REACHED
                || resolution == ProofResolution.NOT_EVALUATED
                    && reason
                        == ProofResolutionReason.NOT_EVALUATED_AFTER_TERMINAL_OUTCOME;
        };
        if (!valid) {
            throw new IllegalArgumentException(
                "Proof evaluation state requires its exact lifecycle resolution and reason"
            );
        }
    }
}
