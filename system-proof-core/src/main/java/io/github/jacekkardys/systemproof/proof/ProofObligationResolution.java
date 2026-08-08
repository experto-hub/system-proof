package io.github.jacekkardys.systemproof.proof;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.proof.ProofInteractionProvenance.Role;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/** Detached typed resolution and bounded decisive provenance for one required plan item. */
public record ProofObligationResolution(
    ProofObligationId id,
    ProofRequirementKind kind,
    ProofRequirementDescriptor descriptor,
    ProofResolution resolution,
    ProofResolutionReason reason,
    Optional<ConnectionId> connectionId,
    List<ProofInteractionProvenance> provenance
) {
    private static final int MAXIMUM_PROVENANCE = 2;

    public ProofObligationResolution {
        id = Objects.requireNonNull(id, "id must not be null");
        kind = Objects.requireNonNull(kind, "kind must not be null");
        descriptor = Objects.requireNonNull(descriptor, "descriptor must not be null");
        if (descriptor.kind() != kind) {
            throw new IllegalArgumentException(
                "descriptor kind must match the resolved requirement kind"
            );
        }
        resolution = Objects.requireNonNull(resolution, "resolution must not be null");
        reason = Objects.requireNonNull(reason, "reason must not be null");
        connectionId = Objects.requireNonNull(connectionId, "connectionId must not be null");
        provenance = List.copyOf(Objects.requireNonNull(
            provenance,
            "provenance must not be null"
        ));
        if (provenance.stream().anyMatch(Objects::isNull)) {
            throw new NullPointerException("provenance must not contain null");
        }
        if (provenance.size() > MAXIMUM_PROVENANCE) {
            throw new IllegalArgumentException(
                "A proof resolution retains at most " + MAXIMUM_PROVENANCE
                    + " decisive interaction references"
            );
        }
        if (new HashSet<>(provenance.stream()
            .map(ProofInteractionProvenance::interaction)
            .toList()).size() != provenance.size()) {
            throw new IllegalArgumentException(
                "A proof resolution cannot retain a duplicate interaction reference"
            );
        }
        validateCompatibility(descriptor, resolution, reason, connectionId, provenance);
    }

    /** Detached references in provenance order, retained for convenient inspection. */
    public List<InteractionRef> interactions() {
        return provenance.stream().map(ProofInteractionProvenance::interaction).toList();
    }

    private static void validateCompatibility(
        ProofRequirementDescriptor descriptor,
        ProofResolution resolution,
        ProofResolutionReason reason,
        Optional<ConnectionId> connectionId,
        List<ProofInteractionProvenance> provenance
    ) {
        if (resolution == ProofResolution.NOT_EVALUATED
            || reason == ProofResolutionReason.NOT_EVALUATED_AFTER_TERMINAL_OUTCOME) {
            require(
                resolution == ProofResolution.NOT_EVALUATED
                    && reason == ProofResolutionReason.NOT_EVALUATED_AFTER_TERMINAL_OUTCOME
                    && provenance.isEmpty(),
                "NOT_EVALUATED requires its exact terminal reason without provenance"
            );
            validateOptionalConnection(descriptor, connectionId);
            return;
        }
        if (reason == ProofResolutionReason.ACTIVATION_NOT_REACHED) {
            require(
                resolution == ProofResolution.UNREACHED && provenance.isEmpty(),
                "ACTIVATION_NOT_REACHED requires an unreached item without provenance"
            );
            validateOptionalConnection(descriptor, connectionId);
            return;
        }
        require(
            reason != ProofResolutionReason.DEADLINE_EXPIRED,
            "A proof deadline is an evaluation lifecycle gap, not an obligation resolution"
        );
        switch (descriptor) {
            case ProofRequirementDescriptor.Prerequisite value -> {
                require(connectionId.isEmpty() && provenance.isEmpty(),
                    "A prerequisite cannot retain connection or interaction provenance");
                boolean valid = switch (value.expectedStatus()) {
                    case SATISFIED -> resolution == ProofResolution.SATISFIED
                        && reason == ProofResolutionReason.PREREQUISITE_SATISFIED;
                    case UNSUPPORTED -> resolution == ProofResolution.UNSUPPORTED
                        && reason == ProofResolutionReason.PREREQUISITE_UNSUPPORTED;
                    case FAILED -> resolution == ProofResolution.FAILED
                        && reason == ProofResolutionReason.PREREQUISITE_FAILED;
                };
                require(valid, "Prerequisite status, resolution, and reason must agree");
            }
            case ProofRequirementDescriptor.Observation value -> {
                requireConnection(value.connectionId(), connectionId);
                require(provenance.isEmpty(), "An observation cannot retain interaction provenance");
                require(
                    matches(resolution, reason,
                        ProofResolution.SATISFIED, ProofResolutionReason.OBSERVATION_ACTIVE,
                        ProofResolution.UNSUPPORTED, ProofResolutionReason.OBSERVATION_UNSUPPORTED,
                        ProofResolution.MISSING, ProofResolutionReason.OBSERVATION_LOST,
                        ProofResolution.FAILED, ProofResolutionReason.OBSERVATION_FAILED),
                    "Observation resolution and reason must agree"
                );
            }
            case ProofRequirementDescriptor.Correlation value -> {
                requireConnection(value.connectionId(), connectionId);
                boolean valid = resolution == ProofResolution.SATISFIED
                        && reason == ProofResolutionReason.CORRELATION_UNIQUE
                        && exactSingle(provenance, Role.CORRELATION, value.connectionId())
                    || resolution == ProofResolution.MISSING
                        && reason == ProofResolutionReason.CORRELATION_MISSING
                        && provenance.isEmpty()
                    || resolution == ProofResolution.AMBIGUOUS
                        && reason == ProofResolutionReason.CORRELATION_AMBIGUOUS
                        && provenance.isEmpty();
                require(valid, "Correlation resolution, reason, and provenance must agree");
            }
            case ProofRequirementDescriptor.HoldControl value ->
                validateHoldControl(value, resolution, reason, connectionId, provenance);
            case ProofRequirementDescriptor.GuardControl value ->
                validateGuardControl(value, resolution, reason, connectionId, provenance);
            case ProofRequirementDescriptor.HoldEvidence value -> {
                requireConnection(value.connectionId(), connectionId);
                require(
                    value.evidenceKind() == ProofEvidenceKind.HELD_INTERACTION,
                    "Hold evidence requires HELD_INTERACTION"
                );
                require(
                    resolution == ProofResolution.SATISFIED
                        && reason == ProofResolutionReason.EVIDENCE_PRESENT
                        && exactSingle(provenance, Role.HOLD, value.connectionId())
                    || resolution == ProofResolution.MISSING
                        && reason == ProofResolutionReason.EVIDENCE_MISSING
                        && provenance.isEmpty(),
                    "Hold-evidence resolution, reason, and provenance must agree"
                );
            }
            case ProofRequirementDescriptor.GuardEvidence value -> {
                requireConnection(value.connectionId(), connectionId);
                Role expectedRole = switch (value.evidenceKind()) {
                    case PREDECESSOR_INTERACTION -> Role.PREDECESSOR;
                    case SUCCESSOR_INTERACTION -> Role.SUCCESSOR;
                    case HELD_INTERACTION -> throw new IllegalArgumentException(
                        "Guard evidence cannot require a held interaction"
                    );
                };
                require(
                    resolution == ProofResolution.SATISFIED
                        && reason == ProofResolutionReason.EVIDENCE_PRESENT
                        && exactSingle(provenance, expectedRole, value.connectionId())
                    || resolution == ProofResolution.MISSING
                        && reason == ProofResolutionReason.EVIDENCE_MISSING
                        && provenance.isEmpty(),
                    "Guard-evidence resolution, reason, and provenance must agree"
                );
            }
            case ProofRequirementDescriptor.CausalRelation value ->
                validateRelation(value, resolution, reason, connectionId, provenance);
        }
    }

    private static void validateHoldControl(
        ProofRequirementDescriptor.HoldControl descriptor,
        ProofResolution resolution,
        ProofResolutionReason reason,
        Optional<ConnectionId> connectionId,
        List<ProofInteractionProvenance> provenance
    ) {
        require(
            descriptor.expectedState()
                == io.github.jacekkardys.systemproof.control.SemanticHoldState.FORWARDED,
            "A hold-control proof descriptor requires FORWARDED"
        );
        requireConnection(descriptor.connectionId(), connectionId);
        require(
            matches(resolution, reason,
                ProofResolution.SATISFIED,
                    ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE,
                ProofResolution.UNREACHED, ProofResolutionReason.CONTROL_UNREACHED,
                ProofResolution.TIMED_OUT, ProofResolutionReason.CONTROL_TIMED_OUT,
                ProofResolution.AMBIGUOUS,
                    ProofResolutionReason.CONTROL_CORRELATION_INVALIDATED,
                ProofResolution.MISSING, ProofResolutionReason.CONTROL_SESSION_ENDED,
                ProofResolution.FAILED, ProofResolutionReason.CONTROL_FAILED),
            "Hold-control resolution and reason must agree"
        );
        boolean exactHold = exactSingle(provenance, Role.HOLD, descriptor.connectionId());
        require(
            resolution == ProofResolution.UNREACHED
                ? provenance.isEmpty() || exactHold
                : exactHold,
            "Hold-control provenance must represent exactly its reachable hold progress"
        );
    }

    private static void validateGuardControl(
        ProofRequirementDescriptor.GuardControl descriptor,
        ProofResolution resolution,
        ProofResolutionReason reason,
        Optional<ConnectionId> connectionId,
        List<ProofInteractionProvenance> provenance
    ) {
        require(
            descriptor.expectedState()
                == io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState.SATISFIED,
            "A guard-control proof descriptor requires SATISFIED"
        );
        if (resolution == ProofResolution.VIOLATED) {
            require(
                reason == ProofResolutionReason.CAUSAL_RELATION_VIOLATED,
                "A violated guard requires CAUSAL_RELATION_VIOLATED"
            );
            requireViolationProvenance(
                descriptor.predecessorConnectionId(),
                descriptor.successorConnectionId(),
                connectionId,
                provenance
            );
            return;
        }
        require(connectionId.isEmpty(), "A non-violated guard has no single connection");
        require(
            matches(resolution, reason,
                ProofResolution.SATISFIED,
                    ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE,
                ProofResolution.UNREACHED, ProofResolutionReason.CONTROL_UNREACHED,
                ProofResolution.TIMED_OUT, ProofResolutionReason.CONTROL_TIMED_OUT,
                ProofResolution.AMBIGUOUS,
                    ProofResolutionReason.CONTROL_CORRELATION_INVALIDATED,
                ProofResolution.MISSING, ProofResolutionReason.CONTROL_SESSION_ENDED,
                ProofResolution.FAILED, ProofResolutionReason.CONTROL_FAILED),
            "Guard-control resolution and reason must agree"
        );
        if (resolution == ProofResolution.SATISFIED) {
            requireEstablishedProvenance(
                descriptor.predecessorConnectionId(),
                descriptor.successorConnectionId(),
                provenance
            );
        } else if (resolution == ProofResolution.TIMED_OUT) {
            requirePartialProvenance(descriptor.predecessorConnectionId(), provenance);
        } else {
            requireNonViolatedTerminalProvenance(
                descriptor.predecessorConnectionId(),
                descriptor.successorConnectionId(),
                provenance
            );
        }
    }

    private static void validateRelation(
        ProofRequirementDescriptor.CausalRelation descriptor,
        ProofResolution resolution,
        ProofResolutionReason reason,
        Optional<ConnectionId> connectionId,
        List<ProofInteractionProvenance> provenance
    ) {
        if (resolution == ProofResolution.VIOLATED) {
            require(
                reason == ProofResolutionReason.CAUSAL_RELATION_VIOLATED,
                "A violated relation requires CAUSAL_RELATION_VIOLATED"
            );
            requireViolationProvenance(
                descriptor.predecessorConnectionId(),
                descriptor.successorConnectionId(),
                connectionId,
                provenance
            );
            return;
        }
        require(connectionId.isEmpty(), "A causal relation has no single connection");
        require(
            resolution == ProofResolution.SATISFIED
                && reason == ProofResolutionReason.CAUSAL_RELATION_ESTABLISHED
            || resolution == ProofResolution.UNREACHED
                && reason == ProofResolutionReason.CAUSAL_RELATION_UNREACHED,
            "Causal-relation resolution and reason must agree"
        );
        if (resolution == ProofResolution.SATISFIED) {
            requireEstablishedProvenance(
                descriptor.predecessorConnectionId(),
                descriptor.successorConnectionId(),
                provenance
            );
        } else {
            requirePartialProvenance(descriptor.predecessorConnectionId(), provenance);
        }
    }

    private static void requireViolationProvenance(
        ConnectionId predecessor,
        ConnectionId successor,
        Optional<ConnectionId> connectionId,
        List<ProofInteractionProvenance> provenance
    ) {
        require(connectionId.filter(successor::equals).isPresent(),
            "A violated relation requires exact successor connection provenance");
        require(
            exactSingle(provenance, Role.SUCCESSOR, successor)
                || exactPair(provenance, predecessor, successor),
            "A violated relation requires an explicit successor interaction"
        );
    }

    private static void requireEstablishedProvenance(
        ConnectionId predecessor,
        ConnectionId successor,
        List<ProofInteractionProvenance> provenance
    ) {
        require(
            exactPair(provenance, predecessor, successor),
            "An established guard relation requires predecessor then successor provenance"
        );
    }

    private static void requirePartialProvenance(
        ConnectionId predecessor,
        List<ProofInteractionProvenance> provenance
    ) {
        require(
            provenance.isEmpty() || exactSingle(provenance, Role.PREDECESSOR, predecessor),
            "A partial guard resolution may retain only its predecessor interaction"
        );
    }

    private static void requireNonViolatedTerminalProvenance(
        ConnectionId predecessor,
        ConnectionId successor,
        List<ProofInteractionProvenance> provenance
    ) {
        require(
            provenance.isEmpty()
                || exactSingle(provenance, Role.PREDECESSOR, predecessor)
                || exactPair(provenance, predecessor, successor),
            "A non-violated terminal guard may retain predecessor progress and its successor"
        );
    }

    private static boolean exactSingle(
        List<ProofInteractionProvenance> provenance,
        Role role,
        ConnectionId connectionId
    ) {
        return provenance.size() == 1
            && exact(provenance.getFirst(), role, connectionId);
    }

    private static boolean exactPair(
        List<ProofInteractionProvenance> provenance,
        ConnectionId predecessor,
        ConnectionId successor
    ) {
        return provenance.size() == 2
            && exact(provenance.getFirst(), Role.PREDECESSOR, predecessor)
            && exact(provenance.getLast(), Role.SUCCESSOR, successor);
    }

    private static boolean exact(
        ProofInteractionProvenance provenance,
        Role role,
        ConnectionId connectionId
    ) {
        return provenance.role() == role
            && provenance.interaction().connectionId().equals(connectionId);
    }

    private static void validateOptionalConnection(
        ProofRequirementDescriptor descriptor,
        Optional<ConnectionId> connectionId
    ) {
        connectionId.ifPresent(value -> require(
            declaredConnections(descriptor).contains(value),
            "Retained connection contradicts the requirement descriptor"
        ));
    }

    private static List<ConnectionId> declaredConnections(
        ProofRequirementDescriptor descriptor
    ) {
        return switch (descriptor) {
            case ProofRequirementDescriptor.Prerequisite ignored -> List.of();
            case ProofRequirementDescriptor.Observation value -> List.of(value.connectionId());
            case ProofRequirementDescriptor.Correlation value -> List.of(value.connectionId());
            case ProofRequirementDescriptor.HoldControl value -> List.of(value.connectionId());
            case ProofRequirementDescriptor.GuardControl value -> List.of(
                value.predecessorConnectionId(), value.successorConnectionId());
            case ProofRequirementDescriptor.HoldEvidence value -> List.of(value.connectionId());
            case ProofRequirementDescriptor.GuardEvidence value -> List.of(value.connectionId());
            case ProofRequirementDescriptor.CausalRelation value -> List.of(
                value.predecessorConnectionId(), value.successorConnectionId());
        };
    }

    private static void requireConnection(
        ConnectionId expected,
        Optional<ConnectionId> actual
    ) {
        require(actual.filter(expected::equals).isPresent(),
            "Resolution connection must match its descriptor");
    }

    private static boolean matches(
        ProofResolution resolution,
        ProofResolutionReason reason,
        Object... pairs
    ) {
        for (int index = 0; index < pairs.length; index += 2) {
            if (resolution == pairs[index] && reason == pairs[index + 1]) {
                return true;
            }
        }
        return false;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
