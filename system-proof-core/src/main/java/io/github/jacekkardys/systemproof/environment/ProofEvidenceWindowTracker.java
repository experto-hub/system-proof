package io.github.jacekkardys.systemproof.environment;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.observation.SessionId;

/**
 * Observation-owned interaction watermarks for one proof evidence-window boundary.
 *
 * <p>Recording an interaction and opening the window contend on this one monitor. Membership is
 * then derived only from the immutable per-stream ordinal watermark captured at the boundary.
 * Callback arrival time, journal order, and wall-clock time are deliberately irrelevant.
 */
final class ProofEvidenceWindowTracker {
    private final Map<StreamId, Long> lastRecordedOrdinals = new HashMap<>();
    private final Runnable beforeWindowOpen;
    private final Runnable beforeInteractionRecord;
    private boolean windowOpened;

    ProofEvidenceWindowTracker() {
        this(() -> {}, () -> {});
    }

    ProofEvidenceWindowTracker(Runnable beforeWindowOpen) {
        this(beforeWindowOpen, () -> {});
    }

    ProofEvidenceWindowTracker(
        Runnable beforeWindowOpen,
        Runnable beforeInteractionRecord
    ) {
        this.beforeWindowOpen = Objects.requireNonNull(
            beforeWindowOpen,
            "beforeWindowOpen must not be null"
        );
        this.beforeInteractionRecord = Objects.requireNonNull(
            beforeInteractionRecord,
            "beforeInteractionRecord must not be null"
        );
    }

    void recorded(InteractionRef interaction) {
        interaction = Objects.requireNonNull(interaction, "interaction must not be null");
        beforeInteractionRecord.run();
        synchronized (this) {
            StreamId stream = StreamId.from(interaction);
            Long previous = lastRecordedOrdinals.get(stream);
            if (previous != null && interaction.ordinal() <= previous) {
                throw new IllegalStateException(
                    "Interaction ordinals must increase within one observed stream"
                );
            }
            lastRecordedOrdinals.put(stream, interaction.ordinal());
        }
    }

    EvidenceWindow openWindow(Consumer<EvidenceWindow> admitWindow) {
        admitWindow = Objects.requireNonNull(admitWindow, "admitWindow must not be null");
        beforeWindowOpen.run();
        synchronized (this) {
            if (windowOpened) {
                throw new IllegalStateException(
                    "A proof evidence window can be opened only once per environment execution"
                );
            }
            windowOpened = true;
            EvidenceWindow window = new EvidenceWindow(this, lastRecordedOrdinals);
            admitWindow.accept(window);
            return window;
        }
    }

    private record StreamId(SessionId sessionId, FlowDirection direction) {
        private StreamId {
            sessionId = Objects.requireNonNull(sessionId, "sessionId must not be null");
            direction = Objects.requireNonNull(direction, "direction must not be null");
        }

        private static StreamId from(InteractionRef interaction) {
            return new StreamId(interaction.sessionId(), interaction.direction());
        }
    }

    static final class EvidenceWindow {
        private final ProofEvidenceWindowTracker owner;
        private final Map<StreamId, Long> boundaryOrdinals;

        private EvidenceWindow(
            ProofEvidenceWindowTracker owner,
            Map<StreamId, Long> boundaryOrdinals
        ) {
            this.owner = Objects.requireNonNull(owner, "owner must not be null");
            this.boundaryOrdinals = Map.copyOf(
                Objects.requireNonNull(boundaryOrdinals, "boundaryOrdinals must not be null")
            );
        }

        boolean includes(InteractionRef interaction) {
            interaction = Objects.requireNonNull(interaction, "interaction must not be null");
            Long boundaryOrdinal = boundaryOrdinals.get(StreamId.from(interaction));
            return boundaryOrdinal == null || interaction.ordinal() > boundaryOrdinal;
        }

        boolean belongsTo(ProofEvidenceWindowTracker tracker) {
            return owner == tracker;
        }
    }
}
