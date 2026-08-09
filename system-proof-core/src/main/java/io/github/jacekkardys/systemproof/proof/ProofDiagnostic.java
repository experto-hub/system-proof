package io.github.jacekkardys.systemproof.proof;

import java.util.Objects;
import io.github.jacekkardys.systemproof.journal.FailureDetails;

/** Bounded type-only diagnostic attached to a proof result. */
public record ProofDiagnostic(ProofFailureStage stage, FailureDetails failure) {
    public ProofDiagnostic {
        stage = Objects.requireNonNull(stage, "stage must not be null");
        failure = Objects.requireNonNull(failure, "failure must not be null");
    }
}
