package io.github.jacekkardys.systemproof.proof;

import java.util.Optional;
import io.github.jacekkardys.systemproof.journal.FailureDetails;

/** Environment-owned typed pre-stimulus prerequisite fact. */
public interface ProofPrerequisite {
    ProofPrerequisiteStatus status();

    Optional<FailureDetails> failure();
}
