package io.github.jacekkardys.systemproof.journal;

import java.util.Objects;
import java.util.Optional;
import io.github.jacekkardys.systemproof.control.SemanticHoldFailure;
import io.github.jacekkardys.systemproof.control.SemanticHoldRef;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.observation.EvidenceSchemaId;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/**
 * Immutable, secret-safe lifecycle fact for one semantic hold. An interaction reference is
 * present only after the hold actually reached {@link SemanticHoldState#REACHED_HELD}; a
 * selector candidate that failed before reach is never retained as held provenance.
 */
public record SemanticHoldEvent(
    SemanticHoldRef holdRef,
    SemanticHoldState state,
    ConnectionId connectionId,
    FlowDirection direction,
    EvidenceSchemaId evidenceSchema,
    Optional<ProofSubjectRef> proofSubject,
    Optional<InteractionRef> interactionRef,
    Optional<SemanticHoldFailure> failure
) implements ScenarioEvent {
    public SemanticHoldEvent {
        holdRef = Objects.requireNonNull(holdRef, "holdRef must not be null");
        state = Objects.requireNonNull(state, "state must not be null");
        connectionId = Objects.requireNonNull(connectionId, "connectionId must not be null");
        direction = Objects.requireNonNull(direction, "direction must not be null");
        evidenceSchema = Objects.requireNonNull(
            evidenceSchema,
            "evidenceSchema must not be null"
        );
        proofSubject = Objects.requireNonNull(
            proofSubject,
            "proofSubject must not be null"
        );
        interactionRef = Objects.requireNonNull(
            interactionRef,
            "interactionRef must not be null"
        );
        failure = Objects.requireNonNull(failure, "failure must not be null");
        if ((state == SemanticHoldState.FAILED) != failure.isPresent()) {
            throw new IllegalArgumentException(
                "A semantic hold failure classification is required only for FAILED state"
            );
        }
        validateInteraction(state, interactionRef, failure);
    }

    @Override
    public String toString() {
        return "SemanticHoldEvent[holdRef=opaque, state=" + state
            + ", connectionId=" + connectionId
            + ", direction=" + direction
            + ", evidenceSchema=" + evidenceSchema.namespace() + ":"
            + evidenceSchema.name() + ":v" + evidenceSchema.version()
            + ", proofSubject=" + (proofSubject.isPresent() ? "assigned" : "unassigned")
            + ", interactionPresent=" + interactionRef.isPresent()
            + ", failure=" + failure.map(Enum::name).orElse("none") + "]";
    }

    private static void validateInteraction(
        SemanticHoldState state,
        Optional<InteractionRef> interactionRef,
        Optional<SemanticHoldFailure> failure
    ) {
        if (state == SemanticHoldState.DECLARED || state == SemanticHoldState.ARMED) {
            if (interactionRef.isPresent()) {
                throw new IllegalArgumentException(
                    "A semantic hold before REACHED_HELD has no interaction"
                );
            }
            return;
        }
        if (state == SemanticHoldState.CANCELLED) {
            return;
        }
        if (state == SemanticHoldState.FAILED) {
            SemanticHoldFailure classification = failure.orElseThrow();
            boolean failedBeforeReach = classification == SemanticHoldFailure.AMBIGUOUS_MATCH
                || classification == SemanticHoldFailure.SELECTOR_EVALUATION;
            if (failedBeforeReach != interactionRef.isEmpty()) {
                throw new IllegalArgumentException(
                    failedBeforeReach
                        ? "A pre-reach semantic hold failure cannot retain an interaction"
                        : "A post-reach semantic hold failure requires its held interaction"
                );
            }
            return;
        }
        if (interactionRef.isEmpty()) {
            throw new IllegalArgumentException(
                "A reached semantic hold lifecycle fact requires an interaction"
            );
        }
    }
}
