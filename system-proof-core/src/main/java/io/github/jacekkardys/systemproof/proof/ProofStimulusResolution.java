package io.github.jacekkardys.systemproof.proof;

import java.util.Objects;

/** Detached typed resolution of the mandatory one-shot proof stimulus. */
public record ProofStimulusResolution(
    ProofStimulusState state,
    ProofResolution resolution,
    ProofResolutionReason reason
) {
    public ProofStimulusResolution {
        state = Objects.requireNonNull(state, "state must not be null");
        resolution = Objects.requireNonNull(resolution, "resolution must not be null");
        reason = Objects.requireNonNull(reason, "reason must not be null");
        if (state == ProofStimulusState.COMPLETED
            && (resolution != ProofResolution.SATISFIED
                || reason != ProofResolutionReason.STIMULUS_COMPLETED)) {
            throw new IllegalArgumentException(
                "A completed stimulus must be SATISFIED with STIMULUS_COMPLETED reason"
            );
        }
        if (state == ProofStimulusState.FAILED
            && (resolution != ProofResolution.FAILED
                || reason != ProofResolutionReason.STIMULUS_FAILED)) {
            throw new IllegalArgumentException(
                "A failed stimulus must be FAILED with STIMULUS_FAILED reason"
            );
        }
        if (state == ProofStimulusState.NOT_STARTED
            || state == ProofStimulusState.RUNNING) {
            boolean valid = resolution == ProofResolution.TIMED_OUT
                    && reason == ProofResolutionReason.DEADLINE_EXPIRED
                || resolution == ProofResolution.UNREACHED
                    && (reason == ProofResolutionReason.STIMULUS_NOT_COMPLETED
                        || reason == ProofResolutionReason.ACTIVATION_NOT_REACHED)
                || resolution == ProofResolution.NOT_EVALUATED
                    && reason
                        == ProofResolutionReason.NOT_EVALUATED_AFTER_TERMINAL_OUTCOME;
            if (!valid) {
                throw new IllegalArgumentException(
                    "An incomplete stimulus requires one exact lifecycle gap resolution"
                );
            }
        }
    }
}
