package io.github.jacekkardys.systemproof.proof;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldRef;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardRef;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.observation.EvidenceSchemaId;
import io.github.jacekkardys.systemproof.observation.RequiredObservationProfile;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/** Complete immutable protocol-neutral declaration for one controlled proof execution. */
public final class ProofPlan {
    private static final Duration MAXIMUM_DEADLINE = Duration.ofHours(24);
    private static final int MAXIMUM_REQUIREMENTS = 256;

    private final ProofPlanId id;
    private final String title;
    private final ProofSubjectRef primarySubject;
    private final Duration deadline;
    private final List<Requirement> requirements;

    private ProofPlan(Builder builder) {
        id = builder.id;
        title = builder.title;
        primarySubject = builder.primarySubject;
        deadline = builder.deadline;
        requirements = List.copyOf(builder.requirements);
        validateRequirements(requirements);
    }

    public static Builder builder(
        String id,
        String title,
        ProofSubjectRef primarySubject,
        Duration deadline
    ) {
        return new Builder(
            new ProofPlanId(id),
            ProofText.requireTitle(title),
            Objects.requireNonNull(primarySubject, "primarySubject must not be null"),
            requireDeadline(deadline)
        );
    }

    public ProofPlanId id() {
        return id;
    }

    public String title() {
        return title;
    }

    public ProofSubjectRef primarySubject() {
        return primarySubject;
    }

    public Duration deadline() {
        return deadline;
    }

    /** Returns requirements in their canonical declaration order. */
    public List<Requirement> requirements() {
        return requirements;
    }

    @Override
    public String toString() {
        return "ProofPlan[id=" + id + ", titleLength=" + title.length()
            + ", primarySubject=opaque, deadline=" + deadline
            + ", requirements=" + requirements.size() + "]";
    }

    private static Duration requireDeadline(Duration value) {
        value = Objects.requireNonNull(value, "deadline must not be null");
        if (value.isZero() || value.isNegative() || value.compareTo(MAXIMUM_DEADLINE) > 0) {
            throw new IllegalArgumentException(
                "deadline must be positive and at most " + MAXIMUM_DEADLINE
            );
        }
        return value;
    }

    private static void validateRequirements(List<Requirement> requirements) {
        if (requirements.isEmpty()) {
            throw new ProofConfigurationException(
                "A proof plan must declare at least one required obligation"
            );
        }
        if (requirements.size() > MAXIMUM_REQUIREMENTS) {
            throw new ProofConfigurationException(
                "A proof plan supports at most " + MAXIMUM_REQUIREMENTS
                    + " required obligations"
            );
        }
        Set<ProofObligationId> identities = new HashSet<>();
        List<Requirement> semanticRequirements = new ArrayList<>();
        List<SemanticHoldRef> holdControls = new ArrayList<>();
        List<SemanticPredecessorGuardRef> guardControls = new ArrayList<>();
        for (Requirement requirement : requirements) {
            if (!identities.add(requirement.id())) {
                throw new ProofConfigurationException(
                    "Duplicate proof obligation id '" + requirement.id() + "'"
                );
            }
            if (semanticRequirements.stream().anyMatch(
                existing -> sameSemanticRequirement(existing, requirement)
            )) {
                throw new ProofConfigurationException(
                    "Duplicate semantic proof requirement '" + requirement.id() + "'"
                );
            }
            semanticRequirements.add(requirement);
            if (requirement instanceof HoldControl control) {
                holdControls.add(control.holdRef());
            } else if (requirement instanceof GuardControl control) {
                guardControls.add(control.guardRef());
            }
        }
        for (Requirement requirement : requirements) {
            if (requirement instanceof HoldEvidence evidence
                && !containsIdentity(holdControls, evidence.holdRef())) {
                throw new ProofConfigurationException(
                    "Hold evidence '" + evidence.id()
                        + "' must reference a declared control obligation"
                );
            }
            if (requirement instanceof GuardEvidence evidence
                && !containsIdentity(guardControls, evidence.guardRef())) {
                throw new ProofConfigurationException(
                    "Guard evidence '" + evidence.id()
                        + "' must reference a declared control obligation"
                );
            }
            if (requirement instanceof CausalRelation relation
                && !containsIdentity(guardControls, relation.guardRef())) {
                throw new ProofConfigurationException(
                    "Causal relation '" + relation.id()
                        + "' must reference a declared guard obligation"
                );
            }
        }
    }

    private static boolean sameSemanticRequirement(Requirement left, Requirement right) {
        if (left.kind() != right.kind()) {
            return false;
        }
        if (left.getClass() != right.getClass()) {
            return false;
        }
        return switch (left) {
            case Prerequisite value ->
                value.prerequisite() == ((Prerequisite) right).prerequisite();
            case Observation value ->
                value.connectionId().equals(((Observation) right).connectionId());
            case Correlation value -> {
                Correlation other = (Correlation) right;
                yield value.connectionId().equals(other.connectionId())
                    && value.key().equals(other.key())
                    && value.nativeReferenceSchema().equals(other.nativeReferenceSchema());
            }
            case HoldControl value ->
                value.holdRef() == ((HoldControl) right).holdRef();
            case GuardControl value ->
                value.guardRef() == ((GuardControl) right).guardRef();
            case HoldEvidence value -> {
                HoldEvidence other = (HoldEvidence) right;
                yield value.holdRef() == other.holdRef()
                    && value.evidenceKind() == other.evidenceKind();
            }
            case GuardEvidence value -> {
                GuardEvidence other = (GuardEvidence) right;
                yield value.guardRef() == other.guardRef()
                    && value.evidenceKind() == other.evidenceKind();
            }
            case CausalRelation value ->
                value.guardRef() == ((CausalRelation) right).guardRef();
        };
    }

    private static boolean containsIdentity(List<?> values, Object candidate) {
        return values.stream().anyMatch(value -> value == candidate);
    }

    /** One closed immutable proof requirement. */
    public sealed interface Requirement permits
        Prerequisite,
        Observation,
        Correlation,
        HoldControl,
        GuardControl,
        HoldEvidence,
        GuardEvidence,
        CausalRelation {

        ProofObligationId id();

        ProofRequirementKind kind();
    }

    public record Prerequisite(
        ProofObligationId id,
        ProofPrerequisite prerequisite
    ) implements Requirement {
        public Prerequisite {
            id = Objects.requireNonNull(id, "id must not be null");
            prerequisite = Objects.requireNonNull(
                prerequisite,
                "prerequisite must not be null"
            );
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.PREREQUISITE;
        }
    }

    public record Observation(
        ProofObligationId id,
        ConnectionId connectionId,
        RequiredObservationProfile profile
    ) implements Requirement {
        public Observation {
            id = Objects.requireNonNull(id, "id must not be null");
            connectionId = Objects.requireNonNull(
                connectionId,
                "connectionId must not be null"
            );
            profile = Objects.requireNonNull(profile, "profile must not be null");
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.OBSERVATION;
        }
    }

    public record Correlation(
        ProofObligationId id,
        ConnectionId connectionId,
        CorrelationKey key,
        EvidenceSchemaId nativeReferenceSchema
    ) implements Requirement {
        public Correlation {
            id = Objects.requireNonNull(id, "id must not be null");
            connectionId = Objects.requireNonNull(
                connectionId,
                "connectionId must not be null"
            );
            key = Objects.requireNonNull(key, "key must not be null");
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

    public record HoldControl(
        ProofObligationId id,
        SemanticHoldRef holdRef,
        SemanticHoldState expectedState
    ) implements Requirement {
        public HoldControl {
            id = Objects.requireNonNull(id, "id must not be null");
            holdRef = Objects.requireNonNull(holdRef, "holdRef must not be null");
            expectedState = Objects.requireNonNull(
                expectedState,
                "expectedState must not be null"
            );
            if (expectedState != SemanticHoldState.FORWARDED) {
                throw new ProofConfigurationException(
                    "A required semantic hold must expect FORWARDED"
                );
            }
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.CONTROL;
        }
    }

    public record GuardControl(
        ProofObligationId id,
        SemanticPredecessorGuardRef guardRef,
        SemanticPredecessorGuardState expectedState
    ) implements Requirement {
        public GuardControl {
            id = Objects.requireNonNull(id, "id must not be null");
            guardRef = Objects.requireNonNull(guardRef, "guardRef must not be null");
            expectedState = Objects.requireNonNull(
                expectedState,
                "expectedState must not be null"
            );
            if (expectedState != SemanticPredecessorGuardState.SATISFIED) {
                throw new ProofConfigurationException(
                    "A required predecessor guard must expect SATISFIED"
                );
            }
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.CONTROL;
        }
    }

    public record HoldEvidence(
        ProofObligationId id,
        SemanticHoldRef holdRef,
        ProofEvidenceKind evidenceKind
    ) implements Requirement {
        public HoldEvidence {
            id = Objects.requireNonNull(id, "id must not be null");
            holdRef = Objects.requireNonNull(holdRef, "holdRef must not be null");
            evidenceKind = Objects.requireNonNull(
                evidenceKind,
                "evidenceKind must not be null"
            );
            if (evidenceKind != ProofEvidenceKind.HELD_INTERACTION) {
                throw new ProofConfigurationException(
                    "Hold evidence must require HELD_INTERACTION"
                );
            }
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.EVIDENCE;
        }
    }

    public record GuardEvidence(
        ProofObligationId id,
        SemanticPredecessorGuardRef guardRef,
        ProofEvidenceKind evidenceKind
    ) implements Requirement {
        public GuardEvidence {
            id = Objects.requireNonNull(id, "id must not be null");
            guardRef = Objects.requireNonNull(guardRef, "guardRef must not be null");
            evidenceKind = Objects.requireNonNull(
                evidenceKind,
                "evidenceKind must not be null"
            );
            if (evidenceKind == ProofEvidenceKind.HELD_INTERACTION) {
                throw new ProofConfigurationException(
                    "Guard evidence must require a predecessor or successor interaction"
                );
            }
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.EVIDENCE;
        }
    }

    public record CausalRelation(
        ProofObligationId id,
        SemanticPredecessorGuardRef guardRef
    ) implements Requirement {
        public CausalRelation {
            id = Objects.requireNonNull(id, "id must not be null");
            guardRef = Objects.requireNonNull(guardRef, "guardRef must not be null");
        }

        @Override
        public ProofRequirementKind kind() {
            return ProofRequirementKind.CAUSAL_RELATION;
        }
    }

    /** Mutable declaration builder that becomes unusable after {@link #build()}. */
    public static final class Builder {
        private final ProofPlanId id;
        private final String title;
        private final ProofSubjectRef primarySubject;
        private final Duration deadline;
        private final List<Requirement> requirements = new ArrayList<>();
        private boolean built;

        private Builder(
            ProofPlanId id,
            String title,
            ProofSubjectRef primarySubject,
            Duration deadline
        ) {
            this.id = id;
            this.title = title;
            this.primarySubject = primarySubject;
            this.deadline = deadline;
        }

        public Builder prerequisite(String id, ProofPrerequisite prerequisite) {
            return add(new Prerequisite(new ProofObligationId(id), prerequisite));
        }

        public Builder observation(
            String id,
            ConnectionId connectionId,
            RequiredObservationProfile profile
        ) {
            return add(new Observation(new ProofObligationId(id), connectionId, profile));
        }

        public Builder correlation(
            String id,
            ConnectionId connectionId,
            CorrelationKey key,
            EvidenceSchemaId nativeReferenceSchema
        ) {
            return add(new Correlation(
                new ProofObligationId(id),
                connectionId,
                key,
                nativeReferenceSchema
            ));
        }

        public Builder control(
            String id,
            SemanticHold hold,
            SemanticHoldState expectedState
        ) {
            Objects.requireNonNull(hold, "hold must not be null");
            return add(new HoldControl(
                new ProofObligationId(id),
                Objects.requireNonNull(hold.ref(), "hold ref must not be null"),
                expectedState
            ));
        }

        public Builder control(
            String id,
            SemanticPredecessorGuard guard,
            SemanticPredecessorGuardState expectedState
        ) {
            Objects.requireNonNull(guard, "guard must not be null");
            return add(new GuardControl(
                new ProofObligationId(id),
                Objects.requireNonNull(guard.ref(), "guard ref must not be null"),
                expectedState
            ));
        }

        public Builder evidence(String id, SemanticHold hold) {
            Objects.requireNonNull(hold, "hold must not be null");
            return add(new HoldEvidence(
                new ProofObligationId(id),
                Objects.requireNonNull(hold.ref(), "hold ref must not be null"),
                ProofEvidenceKind.HELD_INTERACTION
            ));
        }

        public Builder evidence(
            String id,
            SemanticPredecessorGuard guard,
            ProofEvidenceKind evidenceKind
        ) {
            Objects.requireNonNull(guard, "guard must not be null");
            return add(new GuardEvidence(
                new ProofObligationId(id),
                Objects.requireNonNull(guard.ref(), "guard ref must not be null"),
                evidenceKind
            ));
        }

        public Builder causalRelation(String id, SemanticPredecessorGuard guard) {
            Objects.requireNonNull(guard, "guard must not be null");
            return add(new CausalRelation(
                new ProofObligationId(id),
                Objects.requireNonNull(guard.ref(), "guard ref must not be null")
            ));
        }

        public ProofPlan build() {
            requireDraft();
            built = true;
            return new ProofPlan(this);
        }

        private Builder add(Requirement requirement) {
            requireDraft();
            if (requirements.size() == MAXIMUM_REQUIREMENTS) {
                throw new ProofConfigurationException(
                    "A proof plan supports at most " + MAXIMUM_REQUIREMENTS
                        + " required obligations"
                );
            }
            requirements.add(Objects.requireNonNull(requirement, "requirement must not be null"));
            return this;
        }

        private void requireDraft() {
            if (built) {
                throw new IllegalStateException(
                    "Proof plan builder cannot be mutated or reused after build"
                );
            }
        }
    }
}
