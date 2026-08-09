package io.github.jacekkardys.systemproof.environment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;
import io.github.jacekkardys.systemproof.journal.JournalEntry;
import io.github.jacekkardys.systemproof.journal.JournalSequence;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;
import io.github.jacekkardys.systemproof.journal.ScenarioJournalSnapshot;

/** Authoritative append-only storage owned by one environment execution. */
final class ScenarioJournal {
    private final List<JournalEntry> entries = new ArrayList<>();
    private final LongSupplier nanoTime;
    private final long startedAt;
    private final boolean recordsDiagnosticTime;

    /** Creates storage with diagnostic elapsed time from {@link System#nanoTime()}. */
    ScenarioJournal() {
        this(System::nanoTime, true);
    }

    /** Creates storage with an injectable monotonic source for deterministic tests. */
    ScenarioJournal(LongSupplier nanoTime) {
        this(nanoTime, true);
    }

    /** Creates storage whose entries intentionally omit diagnostic elapsed time. */
    static ScenarioJournal withoutDiagnosticTime() {
        return new ScenarioJournal(() -> 0L, false);
    }

    private ScenarioJournal(LongSupplier nanoTime, boolean recordsDiagnosticTime) {
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime must not be null");
        this.recordsDiagnosticTime = recordsDiagnosticTime;
        startedAt = recordsDiagnosticTime ? nanoTime.getAsLong() : 0L;
    }

    /**
     * Atomically assigns the next local sequence and inserts one event. Successful return is the
     * authoritative durable in-memory commit point; rendering and log emission are downstream
     * diagnostics and cannot change this storage.
     */
    synchronized JournalEntry append(ScenarioEvent event) {
        Objects.requireNonNull(event, "event must not be null");
        JournalEntry entry = new JournalEntry(
            new JournalSequence(JournalSequence.FIRST_VALUE + entries.size()),
            diagnosticElapsedTime(),
            event
        );
        entries.add(entry);
        return entry;
    }

    synchronized ScenarioJournalSnapshot snapshot() {
        return new ScenarioJournalSnapshot(entries);
    }

    private Optional<Duration> diagnosticElapsedTime() {
        if (!recordsDiagnosticTime) {
            return Optional.empty();
        }
        long elapsedNanos = nanoTime.getAsLong() - startedAt;
        if (elapsedNanos < 0) {
            throw new IllegalStateException("Monotonic diagnostic time moved backwards");
        }
        return Optional.of(Duration.ofNanos(elapsedNanos));
    }
}
