package io.github.jacekkardys.systemproof.proof;

/** One environment-owned, one-shot controlled proof execution. */
public interface ProofExecution {
    ProofExecutionState state();

    /** Invokes the external stimulus at most once and only after successful activation. */
    void runStimulus(Runnable stimulus);

    /** Evaluates once; repeated calls return the exact same immutable result instance. */
    ProofResult evaluate();

    /** Returns the terminal result, or rejects access while the execution is not complete. */
    ProofResult result();
}
