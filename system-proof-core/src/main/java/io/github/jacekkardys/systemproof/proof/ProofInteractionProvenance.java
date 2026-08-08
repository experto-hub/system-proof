package io.github.jacekkardys.systemproof.proof;

import java.util.Objects;
import io.github.jacekkardys.systemproof.observation.InteractionRef;

/** Detached, role-aware reference to one decisive interaction. */
public record ProofInteractionProvenance(
    Role role,
    InteractionRef interaction
) {
    public ProofInteractionProvenance {
        role = Objects.requireNonNull(role, "role must not be null");
        interaction = Objects.requireNonNull(interaction, "interaction must not be null");
    }

    public static ProofInteractionProvenance correlation(InteractionRef interaction) {
        return new ProofInteractionProvenance(Role.CORRELATION, interaction);
    }

    public static ProofInteractionProvenance hold(InteractionRef interaction) {
        return new ProofInteractionProvenance(Role.HOLD, interaction);
    }

    public static ProofInteractionProvenance predecessor(InteractionRef interaction) {
        return new ProofInteractionProvenance(Role.PREDECESSOR, interaction);
    }

    public static ProofInteractionProvenance successor(InteractionRef interaction) {
        return new ProofInteractionProvenance(Role.SUCCESSOR, interaction);
    }

    public enum Role {
        CORRELATION,
        HOLD,
        PREDECESSOR,
        SUCCESSOR
    }
}
