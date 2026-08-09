package io.github.jacekkardys.systemproof.control;

/** Read-only lifecycle state of one semantic hold. */
public enum SemanticHoldState {
    DECLARED,
    ARMED,
    REACHED_HELD,
    RELEASING,
    FORWARDED,
    CANCELLED,
    TIMED_OUT,
    FAILED
}
