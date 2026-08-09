package io.github.jacekkardys.systemproof.testsupport;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import io.github.jacekkardys.systemproof.control.SemanticHoldRef;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorBoundary;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardFailure;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardRef;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorViolation;
import io.github.jacekkardys.systemproof.journal.CorrelationCandidateEvent;
import io.github.jacekkardys.systemproof.journal.ProofSubjectArmedEvent;
import io.github.jacekkardys.systemproof.journal.ProofSubjectCreatedEvent;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;
import io.github.jacekkardys.systemproof.journal.SemanticHoldEvent;
import io.github.jacekkardys.systemproof.journal.SemanticPredecessorGuardEvent;
import io.github.jacekkardys.systemproof.observation.EvidenceCodec;
import io.github.jacekkardys.systemproof.observation.EvidenceSchemaId;
import io.github.jacekkardys.systemproof.observation.EvidenceSnapshot;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.observation.SessionId;
import io.github.jacekkardys.systemproof.proof.CorrelationCardinality;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.CorrelationKeySchema;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/** Shared adversarial references and framework events for diagnostics tests. */
public final class OpaqueReferenceDiagnosticsFixture {
    private static final ConnectionId CONNECTION_ID = ConnectionId.of(
        "client[].out->server[].in"
    );
    private static final EvidenceCodec<String> EVIDENCE_CODEC = new EvidenceCodec<>() {
        private static final EvidenceSchemaId SCHEMA = new EvidenceSchemaId(
            "test",
            "opaque-reference",
            1
        );

        @Override
        public EvidenceSchemaId schemaId() {
            return SCHEMA;
        }

        @Override
        public byte[] encode(String evidence) {
            return evidence.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] encodedEvidence) {
            return new String(encodedEvidence, StandardCharsets.UTF_8);
        }
    };

    private OpaqueReferenceDiagnosticsFixture() {}

    public static List<ScenarioEvent> frameworkEvents(Probe probe) {
        InteractionRef interaction = new InteractionRef(
            new SessionId(CONNECTION_ID, SessionId.FIRST_VALUE),
            FlowDirection.CONSUMER_TO_PROVIDER,
            InteractionRef.FIRST_ORDINAL
        );
        InteractionRef successor = new InteractionRef(
            interaction.sessionId(),
            interaction.direction(),
            InteractionRef.FIRST_ORDINAL + 1
        );
        EvidenceSnapshot evidence = EvidenceSnapshot.capture(EVIDENCE_CODEC, "metadata-only");
        CorrelationKey key = CorrelationKey.ofDigest(
            new CorrelationKeySchema("test", "opaque-reference", 1),
            new byte[16]
        );
        return List.of(
            new ProofSubjectCreatedEvent(probe.proofSubject()),
            new ProofSubjectArmedEvent(probe.proofSubject(), key, false),
            new CorrelationCandidateEvent(
                Optional.of(probe.proofSubject()),
                key,
                interaction,
                evidence,
                CorrelationCardinality.UNIQUE
            ),
            new SemanticHoldEvent(
                probe.semanticHold(),
                SemanticHoldState.REACHED_HELD,
                CONNECTION_ID,
                FlowDirection.CONSUMER_TO_PROVIDER,
                EVIDENCE_CODEC.schemaId(),
                Optional.of(probe.proofSubject()),
                Optional.of(interaction),
                Optional.empty()
            ),
            guardEvent(
                probe,
                SemanticPredecessorGuardEvent.Kind.STATE,
                SemanticPredecessorGuardState.ARMED,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
            ),
            guardEvent(
                probe,
                SemanticPredecessorGuardEvent.Kind.DECISION,
                SemanticPredecessorGuardState.ARMED,
                Optional.empty(),
                Optional.of(successor),
                Optional.of(ForwardingDecision.FORWARD),
                Optional.empty(),
                Optional.empty()
            ),
            guardEvent(
                probe,
                SemanticPredecessorGuardEvent.Kind.RELATION,
                SemanticPredecessorGuardState.SATISFIED,
                Optional.of(interaction),
                Optional.of(successor),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
            ),
            guardEvent(
                probe,
                SemanticPredecessorGuardEvent.Kind.VIOLATION,
                SemanticPredecessorGuardState.VIOLATED,
                Optional.empty(),
                Optional.of(successor),
                Optional.of(ForwardingDecision.CLOSE_SESSION),
                Optional.of(SemanticPredecessorViolation.PREDECESSOR_NOT_ESTABLISHED),
                Optional.empty()
            ),
            guardEvent(
                probe,
                SemanticPredecessorGuardEvent.Kind.SUPPRESSED_FAILURE,
                SemanticPredecessorGuardState.VIOLATED,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(SemanticPredecessorGuardFailure.INTERNAL_FAILURE)
            )
        );
    }

    public static List<String> allCanaries() {
        return Arrays.stream(Behavior.values())
            .flatMap(behavior -> Stream.of(
                canary("proof-subject", behavior),
                canary("semantic-hold", behavior),
                canary("predecessor-guard", behavior)
            ))
            .toList();
    }

    private static SemanticPredecessorGuardEvent guardEvent(
        Probe probe,
        SemanticPredecessorGuardEvent.Kind kind,
        SemanticPredecessorGuardState state,
        Optional<InteractionRef> predecessor,
        Optional<InteractionRef> successor,
        Optional<ForwardingDecision> decision,
        Optional<SemanticPredecessorViolation> violation,
        Optional<SemanticPredecessorGuardFailure> failure
    ) {
        return new SemanticPredecessorGuardEvent(
            probe.predecessorGuard(),
            kind,
            probe.proofSubject(),
            state,
            SemanticPredecessorBoundary.CONFIRMED,
            predecessor,
            successor,
            decision,
            violation,
            failure
        );
    }

    private static String canary(String referenceKind, Behavior behavior) {
        return "opaque-" + referenceKind + "-"
            + behavior.name().toLowerCase(Locale.ROOT) + "-secret-canary";
    }

    private static String hostileText(
        String referenceKind,
        Behavior behavior,
        AtomicInteger calls
    ) {
        calls.incrementAndGet();
        String canary = canary(referenceKind, behavior);
        return switch (behavior) {
            case SECRET -> canary;
            case FORGED_LINE -> canary + System.lineSeparator()
                + "T+99:99:99.999 [FORGED] injected journal line";
            case LARGE -> canary + "x".repeat(1_000_000);
            case THROWING -> throw new IllegalStateException(canary);
        };
    }

    public enum Behavior {
        SECRET,
        FORGED_LINE,
        LARGE,
        THROWING
    }

    /** One independent hostile implementation of each open opaque reference interface. */
    public static final class Probe {
        private final AtomicInteger proofSubjectCalls = new AtomicInteger();
        private final AtomicInteger semanticHoldCalls = new AtomicInteger();
        private final AtomicInteger predecessorGuardCalls = new AtomicInteger();
        private final ProofSubjectRef proofSubject;
        private final SemanticHoldRef semanticHold;
        private final SemanticPredecessorGuardRef predecessorGuard;

        public Probe(Behavior behavior) {
            proofSubject = new ProofSubjectRef() {
                @Override
                public String toString() {
                    return hostileText("proof-subject", behavior, proofSubjectCalls);
                }
            };
            semanticHold = new SemanticHoldRef() {
                @Override
                public String toString() {
                    return hostileText("semantic-hold", behavior, semanticHoldCalls);
                }
            };
            predecessorGuard = new SemanticPredecessorGuardRef() {
                @Override
                public String toString() {
                    return hostileText(
                        "predecessor-guard",
                        behavior,
                        predecessorGuardCalls
                    );
                }
            };
        }

        public ProofSubjectRef proofSubject() {
            return proofSubject;
        }

        public SemanticHoldRef semanticHold() {
            return semanticHold;
        }

        public SemanticPredecessorGuardRef predecessorGuard() {
            return predecessorGuard;
        }

        public int toStringCalls() {
            return proofSubjectCalls.get()
                + semanticHoldCalls.get()
                + predecessorGuardCalls.get();
        }
    }
}
