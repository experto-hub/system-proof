package io.github.jacekkardys.systemproof.proof;

/** Stable bounded secret-safe identity of one proof plan. */
public record ProofPlanId(String value) {
    public ProofPlanId {
        value = ProofText.requireIdentifier(value, "proof plan id");
    }

    @Override
    public String toString() {
        return value;
    }
}
