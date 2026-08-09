package io.github.jacekkardys.systemproof.proof;

/** Environment-scoped facade for prerequisite facts and one proof-plan activation. */
public interface Proofs {
    ProofPrerequisite satisfiedPrerequisite();

    ProofPrerequisite unsupportedPrerequisite();

    ProofPrerequisite failedPrerequisite(Throwable failure);

    ProofExecution activate(ProofPlan plan);
}
