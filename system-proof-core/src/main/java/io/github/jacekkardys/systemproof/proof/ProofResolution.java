package io.github.jacekkardys.systemproof.proof;

/** Closed resolution for every required proof-plan item. */
public enum ProofResolution {
    SATISFIED,
    VIOLATED,
    MISSING,
    AMBIGUOUS,
    UNSUPPORTED,
    UNREACHED,
    TIMED_OUT,
    FAILED,
    NOT_EVALUATED
}
