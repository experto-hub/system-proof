package io.github.jacekkardys.systemproof.proof;

import java.util.Objects;

/** Bounded immutable compact human-readable report for one proof result. */
public final class ProofReport {
    static final int MAX_CHARACTERS = 64 * 1024;
    private static final String TRUNCATION_MARKER = "[PROOF REPORT TRUNCATED]";

    private final String content;

    ProofReport(String content) {
        content = Objects.requireNonNull(content, "content must not be null");
        this.content = content.length() <= MAX_CHARACTERS
            ? content
            : content.substring(0, MAX_CHARACTERS - TRUNCATION_MARKER.length())
                + TRUNCATION_MARKER;
    }

    public String content() {
        return content;
    }

    @Override
    public boolean equals(Object other) {
        return this == other
            || other instanceof ProofReport report && content.equals(report.content);
    }

    @Override
    public int hashCode() {
        return content.hashCode();
    }

    @Override
    public String toString() {
        return "ProofReport[length=" + content.length() + "]";
    }
}
