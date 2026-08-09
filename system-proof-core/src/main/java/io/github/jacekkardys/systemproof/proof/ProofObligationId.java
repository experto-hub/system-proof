package io.github.jacekkardys.systemproof.proof;

/** Stable bounded secret-safe identity of one required proof-plan item. */
public record ProofObligationId(String value) {
    public ProofObligationId {
        value = ProofText.requireIdentifier(value, "proof obligation id");
    }

    @Override
    public String toString() {
        return value;
    }
}
