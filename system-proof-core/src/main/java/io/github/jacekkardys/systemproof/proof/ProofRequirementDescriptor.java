package io.github.jacekkardys.systemproof.proof;

import java.util.Objects;
import io.github.jacekkardys.systemproof.control.SemanticHoldRef;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardRef;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.observation.EvidenceSchemaId;
import io.github.jacekkardys.systemproof.observation.RequiredObservationProfile;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/** Detached, bounded, secret-safe typed description of one evaluated requirement. */
public sealed interface ProofRequirementDescriptor permits
    ProofRequirementDescriptor.Prerequisite,
    ProofRequirementDescriptor.Observation,
    ProofRequirementDescriptor.Correlation,
    ProofRequirementDescriptor.HoldControl,
    ProofRequirementDescriptor.GuardControl,
    ProofRequirementDescriptor.HoldEvidence,
    ProofRequirementDescriptor.GuardEvidence,
    ProofRequirementDescriptor.CausalRelation {

    ProofRequirementKind kind();

    record Prerequisite(ProofPrerequisiteStatus expectedStatus)
        implements ProofRequirementDescriptor {
        public Prerequisite {
            expectedStatus = Objects.requireNonNull(
                expectedStatus,
                "expectedStatus must not be null"
            );
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.PREREQUISITE;
        }
    }

    record Observation(
        ConnectionId connectionId,
        RequiredObservationProfile profile
    ) implements ProofRequirementDescriptor {
        public Observation {
            connectionId = Objects.requireNonNull(connectionId, "connectionId must not be null");
            profile = Objects.requireNonNull(profile, "profile must not be null");
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.OBSERVATION;
        }
    }

    record Correlation(
        ProofSubjectRef subject,
        CorrelationKey key,
        ConnectionId connectionId,
        EvidenceSchemaId nativeReferenceSchema
    ) implements ProofRequirementDescriptor {
        public Correlation {
            subject = Objects.requireNonNull(subject, "subject must not be null");
            key = Objects.requireNonNull(key, "key must not be null");
            connectionId = Objects.requireNonNull(connectionId, "connectionId must not be null");
            nativeReferenceSchema = Objects.requireNonNull(
                nativeReferenceSchema,
                "nativeReferenceSchema must not be null"
            );
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.CORRELATION;
        }
    }

    record HoldControl(
        SemanticHoldRef controlRef,
        SemanticHoldState expectedState,
        ConnectionId connectionId
    ) implements ProofRequirementDescriptor {
        public HoldControl {
            controlRef = Objects.requireNonNull(controlRef, "controlRef must not be null");
            expectedState = Objects.requireNonNull(expectedState, "expectedState must not be null");
            connectionId = Objects.requireNonNull(connectionId, "connectionId must not be null");
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.CONTROL;
        }
    }

    record GuardControl(
        SemanticPredecessorGuardRef controlRef,
        SemanticPredecessorGuardState expectedState,
        ConnectionId predecessorConnectionId,
        ConnectionId successorConnectionId
    ) implements ProofRequirementDescriptor {
        public GuardControl {
            controlRef = Objects.requireNonNull(controlRef, "controlRef must not be null");
            expectedState = Objects.requireNonNull(expectedState, "expectedState must not be null");
            predecessorConnectionId = Objects.requireNonNull(
                predecessorConnectionId,
                "predecessorConnectionId must not be null"
            );
            successorConnectionId = Objects.requireNonNull(
                successorConnectionId,
                "successorConnectionId must not be null"
            );
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.CONTROL;
        }
    }

    record HoldEvidence(
        SemanticHoldRef controlRef,
        ProofEvidenceKind evidenceKind,
        ConnectionId connectionId
    ) implements ProofRequirementDescriptor {
        public HoldEvidence {
            controlRef = Objects.requireNonNull(controlRef, "controlRef must not be null");
            evidenceKind = Objects.requireNonNull(evidenceKind, "evidenceKind must not be null");
            connectionId = Objects.requireNonNull(connectionId, "connectionId must not be null");
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.EVIDENCE;
        }
    }

    record GuardEvidence(
        SemanticPredecessorGuardRef controlRef,
        ProofEvidenceKind evidenceKind,
        ConnectionId connectionId
    ) implements ProofRequirementDescriptor {
        public GuardEvidence {
            controlRef = Objects.requireNonNull(controlRef, "controlRef must not be null");
            evidenceKind = Objects.requireNonNull(evidenceKind, "evidenceKind must not be null");
            connectionId = Objects.requireNonNull(connectionId, "connectionId must not be null");
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.EVIDENCE;
        }
    }

    record CausalRelation(
        SemanticPredecessorGuardRef guardRef,
        ConnectionId predecessorConnectionId,
        ConnectionId successorConnectionId
    )
        implements ProofRequirementDescriptor {
        public CausalRelation {
            guardRef = Objects.requireNonNull(guardRef, "guardRef must not be null");
            predecessorConnectionId = Objects.requireNonNull(
                predecessorConnectionId,
                "predecessorConnectionId must not be null"
            );
            successorConnectionId = Objects.requireNonNull(
                successorConnectionId,
                "successorConnectionId must not be null"
            );
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.CAUSAL_RELATION;
        }
    }
}
