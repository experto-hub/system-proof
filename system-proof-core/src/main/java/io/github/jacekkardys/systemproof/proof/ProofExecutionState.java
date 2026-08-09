package io.github.jacekkardys.systemproof.proof;

/** Explicit one-shot proof-execution lifecycle. */
public enum ProofExecutionState {
    DRAFT,
    ACTIVATING,
    ACTIVE,
    EVALUATING,
    COMPLETED
}
