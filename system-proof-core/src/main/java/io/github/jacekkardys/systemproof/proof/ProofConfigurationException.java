package io.github.jacekkardys.systemproof.proof;

/** A malformed or contradictory plan rejected before any proof outcome exists. */
public final class ProofConfigurationException extends IllegalArgumentException {
    public ProofConfigurationException(String message) {
        super(message);
    }
}
