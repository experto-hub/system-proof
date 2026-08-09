package io.github.jacekkardys.systemproof.environment;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import io.github.jacekkardys.systemproof.observation.EvidenceCodec;
import io.github.jacekkardys.systemproof.observation.EvidenceSnapshot;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.observation.SessionId;
import io.github.jacekkardys.systemproof.observation.RecordedInteraction;
import io.github.jacekkardys.systemproof.topology.ConnectionRef;

/** Environment-owned implementation of one connection-scoped observation capability. */
final class ConnectionObservationPublisher implements ConnectionObservations {
    private final ConnectionRef connection;
    private final EnvironmentEventPublisher events;
    private final ProofSubjectRegistry proofSubjects;
    private final ProofEvidenceWindowTracker evidenceWindows;
    private long nextSessionValue = SessionId.FIRST_VALUE;

    ConnectionObservationPublisher(
        ConnectionRef connection,
        EnvironmentEventPublisher events,
        ProofSubjectRegistry proofSubjects
    ) {
        this(connection, events, proofSubjects, new ProofEvidenceWindowTracker());
    }

    ConnectionObservationPublisher(
        ConnectionRef connection,
        EnvironmentEventPublisher events,
        ProofSubjectRegistry proofSubjects,
        ProofEvidenceWindowTracker evidenceWindows
    ) {
        this.connection = Objects.requireNonNull(connection, "connection must not be null");
        this.events = Objects.requireNonNull(events, "events must not be null");
        this.proofSubjects = Objects.requireNonNull(
            proofSubjects,
            "proofSubjects must not be null"
        );
        this.evidenceWindows = Objects.requireNonNull(
            evidenceWindows,
            "evidenceWindows must not be null"
        );
    }

    @Override
    public synchronized InteractionSession openSession() {
        if (nextSessionValue < SessionId.FIRST_VALUE) {
            throw new IllegalStateException(
                "Session identity space exhausted for connection '" + connection.id() + "'"
            );
        }
        SessionId sessionId = new SessionId(connection.id(), nextSessionValue);
        nextSessionValue = nextSessionValue == Long.MAX_VALUE
            ? Long.MIN_VALUE
            : nextSessionValue + 1L;
        return new ScopedInteractionSession(sessionId);
    }

    private final class ScopedInteractionSession implements InteractionSession {
        private final SessionId sessionId;
        private final Map<FlowDirection, StreamPublisher> streams;

        private ScopedInteractionSession(SessionId sessionId) {
            this.sessionId = sessionId;
            EnumMap<FlowDirection, StreamPublisher> publishers =
                new EnumMap<>(FlowDirection.class);
            for (FlowDirection direction : FlowDirection.values()) {
                publishers.put(direction, new StreamPublisher(sessionId, direction));
            }
            streams = Map.copyOf(publishers);
        }

        @Override
        public <T> InteractionRef observe(
            FlowDirection direction,
            EvidenceCodec<T> codec,
            T evidence
        ) {
            return record(direction, codec, evidence).interactionRef();
        }

        @Override
        public <T> RecordedInteraction record(
            FlowDirection direction,
            EvidenceCodec<T> codec,
            T evidence
        ) {
            Objects.requireNonNull(direction, "direction must not be null");
            return streams.get(direction).observe(codec, evidence);
        }

        @Override
        public void correlate(
            InteractionRef interactionRef,
            CorrelationContribution<?> contribution
        ) {
            Objects.requireNonNull(
                interactionRef,
                "interactionRef must not be null"
            );
            if (!sessionId.equals(interactionRef.sessionId())) {
                throw new IllegalArgumentException(
                    "Interaction reference does not belong to this physical session"
                );
            }
            StreamPublisher stream = streams.get(interactionRef.direction());
            if (!stream.wasObserved(interactionRef.ordinal())) {
                throw new IllegalArgumentException(
                    "Interaction reference was not recorded by this session"
                );
            }
            proofSubjects.publish(interactionRef, contribution);
        }
    }

    private final class StreamPublisher {
        private final SessionId sessionId;
        private final FlowDirection direction;
        private long nextOrdinal = InteractionRef.FIRST_ORDINAL;
        private long lastObservedOrdinal;

        private StreamPublisher(SessionId sessionId, FlowDirection direction) {
            this.sessionId = sessionId;
            this.direction = direction;
        }

        private synchronized <T> RecordedInteraction observe(
            EvidenceCodec<T> codec,
            T evidence
        ) {
            if (nextOrdinal < InteractionRef.FIRST_ORDINAL) {
                throw new IllegalStateException(
                    "Interaction ordinal space exhausted for session '" + sessionId
                        + "' direction " + direction
                );
            }
            EvidenceSnapshot snapshot = EvidenceSnapshot.capture(codec, evidence);
            long ordinal = nextOrdinal;
            nextOrdinal = nextOrdinal == Long.MAX_VALUE
                ? Long.MIN_VALUE
                : nextOrdinal + 1L;
            InteractionRef interactionRef =
                new InteractionRef(sessionId, direction, ordinal);
            evidenceWindows.recorded(interactionRef);
            events.interaction(connection, interactionRef, snapshot);
            lastObservedOrdinal = ordinal;
            return new RecordedInteraction(interactionRef, snapshot);
        }

        private synchronized boolean wasObserved(long ordinal) {
            return ordinal >= InteractionRef.FIRST_ORDINAL
                && ordinal <= lastObservedOrdinal;
        }
    }
}
