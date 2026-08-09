package io.github.jacekkardys.systemproof.proof;

/** Secret-safe stage classification for a proof execution failure. */
public enum ProofFailureStage {
    ACTIVATION,
    OBSERVATION,
    CORRELATION,
    CONTROL,
    GATEWAY,
    JOURNAL,
    STIMULUS,
    EVALUATION,
    CLEANUP,
    TEARDOWN
}
