package io.github.jacekkardys.systemproof.journal;

import java.util.Objects;
import java.util.Optional;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorBoundary;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardFailure;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardRef;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorViolation;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;

/**
 * Immutable, protocol-neutral, secret-safe fact for one predecessor guard. When both roles are
 * present, predecessor and successor must identify distinct interactions.
 */
public record SemanticPredecessorGuardEvent(
    SemanticPredecessorGuardRef guardRef,
    Kind kind,
    ProofSubjectRef proofSubject,
    SemanticPredecessorGuardState state,
    SemanticPredecessorBoundary requiredBoundary,
    Optional<InteractionRef> predecessor,
    Optional<InteractionRef> successor,
    Optional<ForwardingDecision> decision,
    Optional<SemanticPredecessorViolation> violation,
    Optional<SemanticPredecessorGuardFailure> failure
) implements ScenarioEvent {
    public SemanticPredecessorGuardEvent {
        guardRef = Objects.requireNonNull(guardRef, "guardRef must not be null");
        kind = Objects.requireNonNull(kind, "kind must not be null");
        proofSubject = Objects.requireNonNull(
            proofSubject,
            "proofSubject must not be null"
        );
        state = Objects.requireNonNull(state, "state must not be null");
        requiredBoundary = Objects.requireNonNull(
            requiredBoundary,
            "requiredBoundary must not be null"
        );
        predecessor = Objects.requireNonNull(predecessor, "predecessor must not be null");
        successor = Objects.requireNonNull(successor, "successor must not be null");
        decision = Objects.requireNonNull(decision, "decision must not be null");
        violation = Objects.requireNonNull(violation, "violation must not be null");
        failure = Objects.requireNonNull(failure, "failure must not be null");
        if (predecessor.isPresent() && predecessor.equals(successor)) {
            throw new IllegalArgumentException(
                "A predecessor guard cannot use one interaction for both roles"
            );
        }
        validate(kind, state, predecessor, successor, decision, violation, failure);
    }

    @Override
    public String toString() {
        return "SemanticPredecessorGuardEvent[guardRef=opaque, kind=" + kind
            + ", proofSubject=assigned, state=" + state
            + ", requiredBoundary=" + requiredBoundary
            + ", predecessorPresent=" + predecessor.isPresent()
            + ", successorPresent=" + successor.isPresent()
            + ", decision=" + decision.map(Enum::name).orElse("none")
            + ", violation=" + violation.map(Enum::name).orElse("none")
            + ", failure=" + failure.map(Enum::name).orElse("none") + "]";
    }

    private static void validate(
        Kind kind,
        SemanticPredecessorGuardState state,
        Optional<InteractionRef> predecessor,
        Optional<InteractionRef> successor,
        Optional<ForwardingDecision> decision,
        Optional<SemanticPredecessorViolation> violation,
        Optional<SemanticPredecessorGuardFailure> failure
    ) {
        if (kind == Kind.STATE
            && (state == SemanticPredecessorGuardState.FAILED) != failure.isPresent()) {
            throw new IllegalArgumentException(
                "A guard failure classification is required only for FAILED state"
            );
        }
        if (kind != Kind.STATE
            && kind != Kind.SUPPRESSED_FAILURE
            && failure.isPresent()) {
            throw new IllegalArgumentException(
                "Only a guard state or suppressed-failure fact can contain a failure classification"
            );
        }
        switch (kind) {
            case STATE -> {
                if (decision.isPresent() || violation.isPresent()) {
                    throw new IllegalArgumentException(
                        "A guard state fact cannot contain a decision or violation"
                    );
                }
            }
            case DECISION -> {
                if (successor.isEmpty() || decision.isEmpty()
                    || violation.isPresent() || failure.isPresent()) {
                    throw new IllegalArgumentException(
                        "A guard decision fact requires only a successor and decision"
                    );
                }
            }
            case RELATION -> {
                if (state != SemanticPredecessorGuardState.SATISFIED
                    || predecessor.isEmpty() || successor.isEmpty()
                    || decision.isPresent() || violation.isPresent() || failure.isPresent()) {
                    throw new IllegalArgumentException(
                        "A satisfied relation requires exact predecessor and successor references"
                    );
                }
            }
            case VIOLATION -> {
                if (state != SemanticPredecessorGuardState.VIOLATED
                    || successor.isEmpty()
                    || decision.filter(ForwardingDecision.CLOSE_SESSION::equals).isEmpty()
                    || violation.isEmpty() || failure.isPresent()) {
                    throw new IllegalArgumentException(
                        "A predecessor violation requires a rejected exact successor"
                    );
                }
            }
            case SUPPRESSED_FAILURE -> {
                if (state != SemanticPredecessorGuardState.VIOLATED
                    || decision.isPresent() || violation.isPresent() || failure.isEmpty()) {
                    throw new IllegalArgumentException(
                        "A suppressed guard failure requires a terminal violation and safe failure classification"
                    );
                }
            }
            case TERMINAL -> {
                if (state == SemanticPredecessorGuardState.SATISFIED) {
                    if (predecessor.isEmpty() || successor.isEmpty()
                        || decision.isPresent() || violation.isPresent()
                        || failure.isPresent()) {
                        throw new IllegalArgumentException(
                            "A satisfied terminal guard fact requires the exact predecessor and successor"
                        );
                    }
                } else if (state == SemanticPredecessorGuardState.VIOLATED) {
                    if (successor.isEmpty()
                        || decision.filter(ForwardingDecision.CLOSE_SESSION::equals).isEmpty()
                        || violation.isEmpty() || failure.isPresent()) {
                        throw new IllegalArgumentException(
                            "A violated terminal guard fact requires the rejected exact successor"
                        );
                    }
                } else {
                    throw new IllegalArgumentException(
                        "A terminal guard fact is supported only for SATISFIED or VIOLATED state"
                    );
                }
            }
        }
    }

    public enum Kind {
        STATE,
        DECISION,
        RELATION,
        VIOLATION,
        SUPPRESSED_FAILURE,
        TERMINAL
    }
}
