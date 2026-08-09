package io.github.jacekkardys.systemproof.control;

/** Read-only lifecycle state of one semantic predecessor guard. */
public enum SemanticPredecessorGuardState {
    DECLARED,
    ARMED,
    PREDECESSOR_OBSERVED,
    PREDECESSOR_SATISFIED,
    SUCCESSOR_AUTHORIZED,
    SATISFIED,
    VIOLATED,
    CANCELLED,
    TIMED_OUT,
    FAILED
}
