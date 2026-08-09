package io.github.jacekkardys.systemproof.environment;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import io.github.jacekkardys.systemproof.proof.CorrelationCardinality;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.CorrelationResult;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.proof.ProofSubjects;
import io.github.jacekkardys.systemproof.observation.EvidenceCodec;
import io.github.jacekkardys.systemproof.observation.EvidenceSchemaId;
import io.github.jacekkardys.systemproof.observation.EvidenceSnapshot;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.observation.SessionId;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/**
 * Environment-owned linearizable subject registry and current-state journal index.
 *
 * <p>Mutating publications enter the proof fact-batch boundary before this registry monitor.
 * They release the registry before proof evaluation receives the complete fact batch. The global
 * order is semantic controls, authoritative-operation boundary, proof subjects, journal
 * publication, proof evaluation; completion delivery runs only after all of them are released.
 */
final class ProofSubjectRegistry implements ProofSubjects {
    private static final long FIRST_SUBJECT_VALUE = 1L;

    private final Object owner = new Object();
    private final EnvironmentEventPublisher events;
    private final Map<ProofSubjectRef, SubjectState> subjects = new HashMap<>();
    private final Map<CorrelationKey, Set<ProofSubjectRef>> subjectsByKey =
        new HashMap<>();
    private long nextSubjectValue = FIRST_SUBJECT_VALUE;
    private boolean acceptingPublications = true;

    ProofSubjectRegistry(EnvironmentEventPublisher events) {
        this.events = Objects.requireNonNull(events, "events must not be null");
    }

    @Override
    public ProofSubjectRef create() {
        return events.proofFactBatch(() -> {
            synchronized (this) {
                requireAccepting("create proof subjects");
                ProofSubjectRef subject = createReference();
                events.proofSubjectCreated(subject);
                subjects.put(subject, new SubjectState());
                return subject;
            }
        });
    }

    @Override
    public void arm(ProofSubjectRef subject, CorrelationKey key) {
        events.proofFactBatch(() -> {
            synchronized (this) {
                armLocked(subject, key);
            }
            return null;
        });
    }

    private void armLocked(ProofSubjectRef subject, CorrelationKey key) {
        requireAccepting("arm proof subjects");
        SubjectState subjectState = requireSubject(subject);
        key = Objects.requireNonNull(key, "key must not be null");
        CorrelationKey armedKey = key;
        if (subjectState.resolutions.containsKey(key)) {
            return;
        }

        Set<ProofSubjectRef> existingSubjects = subjectsByKey.get(key);
        boolean sharedKey = existingSubjects != null && !existingSubjects.isEmpty();
        events.proofSubjectArmed(subject, key, sharedKey);

        if (sharedKey) {
            for (ProofSubjectRef existingSubject : existingSubjects) {
                SubjectState existingState = requireSubject(existingSubject);
                existingState.resolutions.get(key).replaceAll(
                    (schema, resolution) -> Ambiguous.INSTANCE
                );
                existingState.exactResolutions.replaceAll(
                    (exact, resolution) -> exact.key().equals(armedKey)
                        ? Ambiguous.INSTANCE
                        : resolution
                );
            }
        }
        subjectState.resolutions.put(key, new HashMap<>());
        subjectsByKey.computeIfAbsent(key, ignored -> new HashSet<>())
            .add(subject);
    }

    @Override
    public <T> CorrelationResult<T> correlation(
        ProofSubjectRef subject,
        CorrelationKey key,
        EvidenceCodec<T> nativeReferenceCodec
    ) {
        nativeReferenceCodec = Objects.requireNonNull(
            nativeReferenceCodec,
            "nativeReferenceCodec must not be null"
        );
        EvidenceSchemaId nativeReferenceSchema = Objects.requireNonNull(
            nativeReferenceCodec.schemaId(),
            "nativeReferenceCodec schemaId must not be null"
        );
        Resolution resolution;
        synchronized (this) {
            SubjectState subjectState = requireSubject(subject);
            key = Objects.requireNonNull(key, "key must not be null");
            Map<EvidenceSchemaId, Resolution> bySchema =
                subjectState.resolutions.get(key);
            if (bySchema == null) {
                throw new IllegalArgumentException(
                    "Correlation key schema '" + key.schema()
                        + "' is not armed for the selected proof subject"
                );
            }
            resolution = hasSharedOwnership(key)
                ? Ambiguous.INSTANCE
                : bySchema.getOrDefault(nativeReferenceSchema, Missing.INSTANCE);
        }

        return switch (resolution) {
            case Missing missing -> new CorrelationResult.Missing<>();
            case Ambiguous ambiguous -> new CorrelationResult.Ambiguous<>();
            case Unique unique -> new CorrelationResult.Unique<>(
                unique.interactionRef,
                unique.nativeReference.schemaId(),
                unique.nativeReference.decode(nativeReferenceCodec)
            );
        };
    }

    void publish(
        InteractionRef interactionRef,
        CorrelationContribution<?> contribution
    ) {
        events.proofFactBatch(() -> {
            synchronized (this) {
                publishLocked(interactionRef, contribution);
            }
            return null;
        });
    }

    private void publishLocked(
        InteractionRef interactionRef,
        CorrelationContribution<?> contribution
    ) {
        requireAccepting("publish correlation candidates");
        interactionRef = Objects.requireNonNull(
            interactionRef,
            "interactionRef must not be null"
        );
        contribution = Objects.requireNonNull(
            contribution,
            "contribution must not be null"
        );
        CorrelationKey key = contribution.key();
        EvidenceSnapshot nativeReference = contribution.nativeReferenceSnapshot();
        Set<ProofSubjectRef> armedSubjects =
            subjectsByKey.getOrDefault(key, Set.of());

        if (armedSubjects.isEmpty()) {
            events.correlationCandidate(
                Optional.empty(),
                key,
                interactionRef,
                nativeReference,
                CorrelationCardinality.MISSING
            );
            return;
        }
        if (armedSubjects.size() > 1) {
            events.correlationCandidate(
                Optional.empty(),
                key,
                interactionRef,
                nativeReference,
                CorrelationCardinality.AMBIGUOUS
            );
            return;
        }

        ProofSubjectRef subject = armedSubjects.iterator().next();
        SubjectState subjectState = requireSubject(subject);
        Map<EvidenceSchemaId, Resolution> bySchema = Objects.requireNonNull(
            subjectState.resolutions.get(key),
            "Armed proof subject has no correlation resolution"
        );
        EvidenceSchemaId nativeReferenceSchema = nativeReference.schemaId();
        ExactCorrelation exactCorrelation = new ExactCorrelation(
            key,
            interactionRef.connectionId(),
            nativeReferenceSchema
        );
        Resolution exactCurrent = subjectState.exactResolutions.getOrDefault(
            exactCorrelation,
            Missing.INSTANCE
        );
        if (!(exactCurrent instanceof Unique exactUnique
            && exactUnique.sameCandidate(interactionRef, nativeReference))) {
            subjectState.exactResolutions.put(
                exactCorrelation,
                exactCurrent == Missing.INSTANCE
                    ? new Unique(interactionRef, nativeReference)
                    : Ambiguous.INSTANCE
            );
        }
        Resolution current = bySchema.getOrDefault(
            nativeReferenceSchema,
            Missing.INSTANCE
        );
        if (current instanceof Unique unique
            && unique.sameCandidate(interactionRef, nativeReference)) {
            return;
        }

        CorrelationCardinality cardinality = current == Missing.INSTANCE
            ? CorrelationCardinality.UNIQUE
            : CorrelationCardinality.AMBIGUOUS;
        events.correlationCandidate(
            Optional.of(subject),
            key,
            interactionRef,
            nativeReference,
            cardinality
        );
        bySchema.put(
            nativeReferenceSchema,
            cardinality == CorrelationCardinality.UNIQUE
                ? new Unique(interactionRef, nativeReference)
                : Ambiguous.INSTANCE
        );
    }

    synchronized void completeExecution() {
        acceptingPublications = false;
    }

    synchronized void validateSubject(ProofSubjectRef subject) {
        requireAccepting("use proof subjects in semantic controls");
        requireSubject(subject);
    }

    synchronized void validateSubjectFlow(
        ProofSubjectRef subject,
        CorrelationKey key
    ) {
        SubjectState state = requireSubject(subject);
        key = Objects.requireNonNull(key, "key must not be null");
        if (!state.resolutions.containsKey(key)) {
            throw new IllegalArgumentException(
                "Correlation key schema '" + key.schema()
                    + "' is not armed for the selected proof subject"
            );
        }
    }

    synchronized Optional<NativeFlowResolution> soleUniqueNativeFlow(
        ProofSubjectRef subject,
        CorrelationKey key,
        EvidenceSchemaId nativeReferenceSchema
    ) {
        SubjectState selected = requireSubject(subject);
        CorrelationKey selectedKey = Objects.requireNonNull(
            key,
            "key must not be null"
        );
        nativeReferenceSchema = Objects.requireNonNull(
            nativeReferenceSchema,
            "nativeReferenceSchema must not be null"
        );
        Map<EvidenceSchemaId, Resolution> bySchema =
            selected.resolutions.get(selectedKey);
        if (bySchema == null || hasSharedOwnership(selectedKey)) {
            return Optional.empty();
        }
        Resolution resolution = bySchema.getOrDefault(
            nativeReferenceSchema,
            Missing.INSTANCE
        );
        if (!(resolution instanceof Unique selectedUnique)) {
            return Optional.empty();
        }
        NativeFlowResolution selectedFlow = new NativeFlowResolution(
            subject,
            selectedKey,
            selectedUnique.interactionRef,
            selectedUnique.interactionRef.sessionId(),
            selectedUnique.interactionRef.connectionId(),
            selectedUnique.nativeReference
        );
        if (anotherSubjectOwnsSameNativeFlow(subject, selectedFlow)) {
            return Optional.empty();
        }
        return Optional.of(selectedFlow);
    }

    synchronized boolean remainsSoleUniqueNativeFlow(
        NativeFlowResolution expected
    ) {
        expected = Objects.requireNonNull(expected, "expected must not be null");
        SubjectState selected = requireSubject(expected.subject);
        Map<EvidenceSchemaId, Resolution> bySchema =
            selected.resolutions.get(expected.key);
        Resolution current = bySchema == null
            ? null
            : bySchema.get(expected.nativeReference.schemaId());
        return current instanceof Unique unique
            && expected.sameOriginatingCandidate(unique)
            && !hasSharedOwnership(expected.key)
            && !anotherSubjectOwnsSameNativeFlow(expected.subject, expected);
    }

    private boolean anotherSubjectOwnsSameNativeFlow(
        ProofSubjectRef selectedSubject,
        NativeFlowResolution selectedFlow
    ) {
        for (Map.Entry<ProofSubjectRef, SubjectState> entry : subjects.entrySet()) {
            if (entry.getKey().equals(selectedSubject)) {
                continue;
            }
            boolean sameNativeFlow = entry.getValue().resolutions.values().stream()
                .flatMap(bySchema -> bySchema.values().stream())
                .filter(Unique.class::isInstance)
                .map(Unique.class::cast)
                .anyMatch(selectedFlow::sameSessionAndReference);
            if (sameNativeFlow) {
                return true;
            }
        }
        return false;
    }

    synchronized boolean isSoleUniqueSubjectFor(
        ProofSubjectRef subject,
        InteractionRef interactionRef
    ) {
        requireSubject(subject);
        InteractionRef candidate = Objects.requireNonNull(
            interactionRef,
            "interactionRef must not be null"
        );
        boolean selectedSubjectFound = false;
        for (Map.Entry<ProofSubjectRef, SubjectState> entry : subjects.entrySet()) {
            boolean subjectMatches = entry.getValue().resolutions.values().stream()
                .flatMap(bySchema -> bySchema.values().stream())
                .filter(Unique.class::isInstance)
                .map(Unique.class::cast)
                .anyMatch(unique -> unique.interactionRef.equals(candidate));
            if (!subjectMatches) {
                continue;
            }
            if (!entry.getKey().equals(subject)) {
                return false;
            }
            selectedSubjectFound = true;
        }
        return selectedSubjectFound;
    }

    synchronized void withCorrelationBoundary(
        ProofSubjectRef subject,
        List<CorrelationRequirement> requirements,
        Consumer<List<CorrelationSnapshot>> action
    ) {
        SubjectState selected = requireSubject(subject);
        requirements = List.copyOf(Objects.requireNonNull(
            requirements,
            "requirements must not be null"
        ));
        action = Objects.requireNonNull(action, "action must not be null");
        List<CorrelationSnapshot> snapshots = requirements.stream()
            .map(requirement -> correlationSnapshot(selected, requirement))
            .toList();
        action.accept(snapshots);
    }

    private CorrelationSnapshot correlationSnapshot(
        SubjectState selected,
        CorrelationRequirement requirement
    ) {
        if (!selected.resolutions.containsKey(requirement.key())) {
            return new CorrelationSnapshot(CorrelationCardinality.MISSING, Optional.empty());
        }
        if (hasSharedOwnership(requirement.key())) {
            return new CorrelationSnapshot(
                CorrelationCardinality.AMBIGUOUS,
                Optional.empty()
            );
        }
        if (requirement.acceptedInteraction().isEmpty()) {
            return new CorrelationSnapshot(
                CorrelationCardinality.MISSING,
                Optional.empty()
            );
        }
        Resolution resolution = selected.exactResolutions.getOrDefault(
            new ExactCorrelation(
                requirement.key(),
                requirement.connectionId(),
                requirement.nativeReferenceSchema()
            ),
            Missing.INSTANCE
        );
        return switch (resolution) {
            case Missing ignored -> new CorrelationSnapshot(
                CorrelationCardinality.MISSING,
                Optional.empty()
            );
            case Ambiguous ignored -> new CorrelationSnapshot(
                CorrelationCardinality.AMBIGUOUS,
                Optional.empty()
            );
            case Unique unique -> unique.interactionRef.equals(
                requirement.acceptedInteraction().orElseThrow()
            )
                ? new CorrelationSnapshot(
                    CorrelationCardinality.UNIQUE,
                    Optional.of(unique.interactionRef)
                )
                : new CorrelationSnapshot(
                    CorrelationCardinality.MISSING,
                    Optional.empty()
                );
        };
    }

    private ProofSubjectRef createReference() {
        if (nextSubjectValue < FIRST_SUBJECT_VALUE) {
            throw new IllegalStateException(
                "Proof-subject identity space is exhausted for this environment execution"
            );
        }
        ProofSubjectRef reference = new RuntimeProofSubjectRef(owner, nextSubjectValue);
        nextSubjectValue = nextSubjectValue == Long.MAX_VALUE
            ? Long.MIN_VALUE
            : nextSubjectValue + 1L;
        return reference;
    }

    private SubjectState requireSubject(ProofSubjectRef subject) {
        Objects.requireNonNull(subject, "subject must not be null");
        if (!(subject instanceof RuntimeProofSubjectRef reference)
            || !reference.belongsTo(owner)) {
            throw new IllegalArgumentException(
                "Proof subject belongs to a different environment execution"
            );
        }
        SubjectState state = subjects.get(subject);
        if (state == null) {
            throw new IllegalArgumentException(
                "Proof subject is not allocated by this environment execution"
            );
        }
        return state;
    }

    private void requireAccepting(String action) {
        if (!acceptingPublications) {
            throw new IllegalStateException(
                "Environment execution is complete and cannot " + action
            );
        }
    }

    private boolean hasSharedOwnership(CorrelationKey key) {
        return subjectsByKey.getOrDefault(key, Set.of()).size() > 1;
    }

    record CorrelationRequirement(
        CorrelationKey key,
        ConnectionId connectionId,
        EvidenceSchemaId nativeReferenceSchema,
        Optional<InteractionRef> acceptedInteraction
    ) {
        CorrelationRequirement {
            Objects.requireNonNull(key, "key must not be null");
            Objects.requireNonNull(connectionId, "connectionId must not be null");
            Objects.requireNonNull(
                nativeReferenceSchema,
                "nativeReferenceSchema must not be null"
            );
            acceptedInteraction = Objects.requireNonNull(
                acceptedInteraction,
                "acceptedInteraction must not be null"
            );
            acceptedInteraction.ifPresent(value -> {
                if (!value.connectionId().equals(connectionId)) {
                    throw new IllegalArgumentException(
                        "Accepted correlation interaction must use the required connection"
                    );
                }
            });
        }
    }

    record CorrelationSnapshot(
        CorrelationCardinality cardinality,
        Optional<InteractionRef> interaction
    ) {
        CorrelationSnapshot {
            Objects.requireNonNull(cardinality, "cardinality must not be null");
            interaction = Objects.requireNonNull(interaction, "interaction must not be null");
            if ((cardinality == CorrelationCardinality.UNIQUE) != interaction.isPresent()) {
                throw new IllegalArgumentException(
                    "Only a unique correlation snapshot retains an interaction reference"
                );
            }
        }
    }

    private static final class SubjectState {
        private final Map<
            CorrelationKey,
            Map<EvidenceSchemaId, Resolution>
        > resolutions = new HashMap<>();
        private final Map<ExactCorrelation, Resolution> exactResolutions = new HashMap<>();
    }

    private record ExactCorrelation(
        CorrelationKey key,
        ConnectionId connectionId,
        EvidenceSchemaId nativeReferenceSchema
    ) {
        private ExactCorrelation {
            Objects.requireNonNull(key, "key must not be null");
            Objects.requireNonNull(connectionId, "connectionId must not be null");
            Objects.requireNonNull(
                nativeReferenceSchema,
                "nativeReferenceSchema must not be null"
            );
        }
    }

    private static final class RuntimeProofSubjectRef implements ProofSubjectRef {
        private final Object owner;
        private final long value;

        private RuntimeProofSubjectRef(Object owner, long value) {
            this.owner = Objects.requireNonNull(owner, "owner must not be null");
            if (value < FIRST_SUBJECT_VALUE) {
                throw new IllegalArgumentException(
                    "proof-subject value must be at least " + FIRST_SUBJECT_VALUE
                );
            }
            this.value = value;
        }

        private boolean belongsTo(Object candidateOwner) {
            return owner == candidateOwner;
        }

        @Override
        public boolean equals(Object other) {
            return this == other
                || other instanceof RuntimeProofSubjectRef reference
                    && owner == reference.owner
                    && value == reference.value;
        }

        @Override
        public int hashCode() {
            return 31 * System.identityHashCode(owner) + Long.hashCode(value);
        }

        @Override
        public String toString() {
            return "proof-subject-" + value;
        }
    }

    private sealed interface Resolution permits Missing, Unique, Ambiguous {}

    private enum Missing implements Resolution {
        INSTANCE
    }

    private record Unique(
        InteractionRef interactionRef,
        EvidenceSnapshot nativeReference
    ) implements Resolution {
        private Unique {
            interactionRef = Objects.requireNonNull(
                interactionRef,
                "interactionRef must not be null"
            );
            nativeReference = Objects.requireNonNull(
                nativeReference,
                "nativeReference must not be null"
            );
        }

        private boolean sameCandidate(
            InteractionRef candidateInteraction,
            EvidenceSnapshot candidateReference
        ) {
            return interactionRef.equals(candidateInteraction)
                && nativeReference.equals(candidateReference);
        }
    }

    /** Immutable internal provenance retained for one sole unique native flow. */
    record NativeFlowResolution(
        ProofSubjectRef subject,
        CorrelationKey key,
        InteractionRef originatingInteraction,
        SessionId originatingSession,
        ConnectionId originatingConnection,
        EvidenceSnapshot nativeReference
    ) {
        NativeFlowResolution {
            subject = Objects.requireNonNull(subject, "subject must not be null");
            key = Objects.requireNonNull(key, "key must not be null");
            originatingInteraction = Objects.requireNonNull(
                originatingInteraction,
                "originatingInteraction must not be null"
            );
            originatingSession = Objects.requireNonNull(
                originatingSession,
                "originatingSession must not be null"
            );
            originatingConnection = Objects.requireNonNull(
                originatingConnection,
                "originatingConnection must not be null"
            );
            nativeReference = Objects.requireNonNull(
                nativeReference,
                "nativeReference must not be null"
            );
            if (!originatingInteraction.sessionId().equals(originatingSession)
                || !originatingInteraction.connectionId().equals(originatingConnection)) {
                throw new IllegalArgumentException(
                    "Native-flow provenance must match the originating interaction"
                );
            }
        }

        boolean containsCandidate(InteractionRef candidate) {
            Objects.requireNonNull(candidate, "candidate must not be null");
            return originatingConnection.equals(candidate.connectionId())
                && originatingSession.equals(candidate.sessionId());
        }

        private boolean sameOriginatingCandidate(Unique candidate) {
            return originatingInteraction.equals(candidate.interactionRef)
                && nativeReference.equals(candidate.nativeReference);
        }

        private boolean sameSessionAndReference(Unique candidate) {
            return originatingSession.equals(candidate.interactionRef.sessionId())
                && nativeReference.equals(candidate.nativeReference);
        }

        @Override
        public String toString() {
            return "NativeFlowResolution[originatingInteraction="
                + originatingInteraction
                + ", nativeReferenceSchema=" + nativeReference.schemaId()
                + ", encodedBytes=" + nativeReference.encodedSize() + "]";
        }
    }

    private enum Ambiguous implements Resolution {
        INSTANCE
    }
}
