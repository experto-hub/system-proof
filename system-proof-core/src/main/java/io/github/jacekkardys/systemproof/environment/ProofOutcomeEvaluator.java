package io.github.jacekkardys.systemproof.environment;

import java.util.List;
import java.util.Objects;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofResolution;

/** Protocol-neutral fail-closed interpretation of a frozen resolution snapshot. */
@FunctionalInterface
interface ProofOutcomeEvaluator {
    ProofOutcome evaluate(List<ProofResolution> resolutions);

    static ProofOutcomeEvaluator failClosed() {
        return resolutions -> {
            resolutions = List.copyOf(Objects.requireNonNull(
                resolutions,
                "resolutions must not be null"
            ));
            if (resolutions.isEmpty() || resolutions.stream().anyMatch(Objects::isNull)) {
                throw new IllegalArgumentException(
                    "Every required proof-plan item needs a resolution"
                );
            }
            if (resolutions.stream().anyMatch(value -> value == ProofResolution.VIOLATED)) {
                return ProofOutcome.VIOLATED;
            }
            if (resolutions.stream().anyMatch(value -> value == ProofResolution.FAILED)) {
                return ProofOutcome.ERROR;
            }
            return resolutions.stream().allMatch(value -> value == ProofResolution.SATISFIED)
                ? ProofOutcome.PROVED
                : ProofOutcome.INCONCLUSIVE;
        };
    }
}
