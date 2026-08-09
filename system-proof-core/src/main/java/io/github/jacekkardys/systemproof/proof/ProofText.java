package io.github.jacekkardys.systemproof.proof;

import java.util.Locale;
import java.util.Objects;

/** Shared validation for bounded proof metadata. */
final class ProofText {
    static final int MAX_TITLE_CHARACTERS = 256;

    private ProofText() {}

    static String requireIdentifier(String value, String description) {
        Objects.requireNonNull(value, description + " must not be null");
        if (value.length() > 128 || !value.matches("[a-zA-Z0-9][a-zA-Z0-9_.:/-]*")) {
            throw new IllegalArgumentException(
                description + " must be 1-128 ASCII identifier characters"
            );
        }
        return value.toLowerCase(Locale.ROOT);
    }

    static String requireTitle(String value) {
        Objects.requireNonNull(value, "proof plan title must not be null");
        if (value.isBlank() || value.length() > MAX_TITLE_CHARACTERS) {
            throw new IllegalArgumentException(
                "proof plan title must contain 1-" + MAX_TITLE_CHARACTERS
                    + " non-blank characters"
            );
        }
        for (int index = 0; index < value.length(); index++) {
            if (Character.isISOControl(value.charAt(index))) {
                throw new IllegalArgumentException(
                    "proof plan title must not contain control characters"
                );
            }
        }
        return value;
    }
}
