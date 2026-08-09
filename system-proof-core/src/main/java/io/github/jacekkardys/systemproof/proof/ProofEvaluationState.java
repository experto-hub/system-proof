package io.github.jacekkardys.systemproof.proof;

/** Public detached lifecycle state of explicit terminal proof evaluation. */
public enum ProofEvaluationState {
    NOT_STARTED,
    RUNNING,
    COMPLETED,
    FAILED
}
