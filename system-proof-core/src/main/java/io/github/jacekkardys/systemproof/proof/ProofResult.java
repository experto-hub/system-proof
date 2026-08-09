package io.github.jacekkardys.systemproof.proof;

import java.util.List;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Detached deeply immutable closed result contract for one proof execution. */
public final class ProofResult {
    private static final int MAX_SECONDARY_DIAGNOSTICS = 32;
    private static final int MAX_RESOLUTIONS = 256;

    private final ProofPlanId planId;
    private final String title;
    private final ProofOutcome outcome;
    private final ProofSubjectRef primarySubject;
    private final ProofStimulusResolution stimulus;
    private final ProofEvaluationResolution evaluation;
    private final List<ProofObligationResolution> resolutions;
    private final Optional<ProofDiagnostic> primaryFailure;
    private final List<ProofDiagnostic> secondaryDiagnostics;
    private final ProofReport report;

    public ProofResult(
        ProofPlanId planId,
        String title,
        ProofOutcome outcome,
        ProofSubjectRef primarySubject,
        ProofStimulusResolution stimulus,
        List<ProofObligationResolution> resolutions,
        Optional<ProofDiagnostic> primaryFailure,
        List<ProofDiagnostic> secondaryDiagnostics
    ) {
        this(
            planId,
            title,
            outcome,
            primarySubject,
            stimulus,
            legacyEvaluation(outcome, primaryFailure),
            resolutions,
            primaryFailure,
            secondaryDiagnostics
        );
    }

    public ProofResult(
        ProofPlanId planId,
        String title,
        ProofOutcome outcome,
        ProofSubjectRef primarySubject,
        ProofStimulusResolution stimulus,
        ProofEvaluationResolution evaluation,
        List<ProofObligationResolution> resolutions,
        Optional<ProofDiagnostic> primaryFailure,
        List<ProofDiagnostic> secondaryDiagnostics
    ) {
        this.planId = Objects.requireNonNull(planId, "planId must not be null");
        this.title = ProofText.requireTitle(title);
        this.outcome = Objects.requireNonNull(outcome, "outcome must not be null");
        this.primarySubject = Objects.requireNonNull(
            primarySubject,
            "primarySubject must not be null"
        );
        this.stimulus = Objects.requireNonNull(stimulus, "stimulus must not be null");
        this.evaluation = Objects.requireNonNull(evaluation, "evaluation must not be null");
        this.resolutions = List.copyOf(
            Objects.requireNonNull(resolutions, "resolutions must not be null")
        );
        if (this.resolutions.isEmpty() || this.resolutions.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException(
                "resolutions must contain every required proof-plan item"
            );
        }
        if (this.resolutions.size() > MAX_RESOLUTIONS) {
            throw new IllegalArgumentException(
                "resolutions must contain at most " + MAX_RESOLUTIONS + " items"
            );
        }
        if (new HashSet<>(this.resolutions.stream()
            .map(ProofObligationResolution::id)
            .toList()).size() != this.resolutions.size()) {
            throw new IllegalArgumentException(
                "resolutions must contain each proof obligation exactly once"
            );
        }
        this.primaryFailure = Objects.requireNonNull(
            primaryFailure,
            "primaryFailure must not be null"
        );
        Objects.requireNonNull(
            secondaryDiagnostics,
            "secondaryDiagnostics must not be null"
        );
        this.secondaryDiagnostics = secondaryDiagnostics.stream()
            .sorted(java.util.Comparator
                .comparing((ProofDiagnostic value) -> value.stage().ordinal())
                .thenComparing(value -> value.failure().failureType()))
            .limit(MAX_SECONDARY_DIAGNOSTICS)
            .map(value -> Objects.requireNonNull(
                value,
                "secondaryDiagnostics must not contain null"
            ))
            .toList();
        validateOutcome();
        report = new ProofReport(render());
    }

    public ProofPlanId planId() {
        return planId;
    }

    public String title() {
        return title;
    }

    public ProofOutcome outcome() {
        return outcome;
    }

    public ProofSubjectRef primarySubject() {
        return primarySubject;
    }

    public ProofStimulusResolution stimulus() {
        return stimulus;
    }

    public ProofEvaluationResolution evaluation() {
        return evaluation;
    }

    public List<ProofObligationResolution> resolutions() {
        return resolutions;
    }

    public Optional<ProofDiagnostic> primaryFailure() {
        return primaryFailure;
    }

    public List<ProofDiagnostic> secondaryDiagnostics() {
        return secondaryDiagnostics;
    }

    public List<ProofObligationResolution> unresolved() {
        return resolutions.stream()
            .filter(value -> value.resolution() != ProofResolution.SATISFIED)
            .toList();
    }

    public Optional<ProofObligationResolution> decisiveResolution() {
        return switch (outcome) {
            case PROVED -> Optional.empty();
            case VIOLATED -> resolutions.stream()
                .filter(value -> value.resolution() == ProofResolution.VIOLATED)
                .findFirst();
            case ERROR -> resolutions.stream()
                .filter(value -> value.resolution() == ProofResolution.FAILED)
                .findFirst();
            case INCONCLUSIVE -> resolutions.stream()
                .filter(value -> value.resolution() != ProofResolution.SATISFIED)
                .findFirst();
        };
    }

    public ProofReport report() {
        return report;
    }

    /** Returns this result when the expected outcome matches, otherwise throws a safe assertion. */
    public ProofResult require(ProofOutcome expected) {
        expected = Objects.requireNonNull(expected, "expected must not be null");
        if (outcome != expected) {
            throw new AssertionError(
                "Expected proof outcome " + expected + " but was " + outcome
                    + System.lineSeparator() + report.content()
            );
        }
        return this;
    }

    @Override
    public String toString() {
        return "ProofResult[planId=" + planId + ", titleLength=" + title.length()
            + ", outcome=" + outcome + ", primarySubject=opaque, resolutions="
            + resolutions.size() + ", secondaryDiagnostics="
            + secondaryDiagnostics.size() + "]";
    }

    private void validateOutcome() {
        boolean allSatisfied = resolutions.stream()
            .allMatch(value -> value.resolution() == ProofResolution.SATISFIED);
        boolean stimulusSatisfied = stimulus.state() == ProofStimulusState.COMPLETED
            && stimulus.resolution() == ProofResolution.SATISFIED;
        boolean evaluationSatisfied = evaluation.state() == ProofEvaluationState.COMPLETED
            && evaluation.resolution() == ProofResolution.SATISFIED;
        boolean violated = resolutions.stream().anyMatch(
            value -> value.resolution() == ProofResolution.VIOLATED
        );
        boolean failed = resolutions.stream().anyMatch(
            value -> value.resolution() == ProofResolution.FAILED
        );
        boolean notEvaluated = resolutions.stream().anyMatch(
            value -> value.resolution() == ProofResolution.NOT_EVALUATED
        ) || stimulus.resolution() == ProofResolution.NOT_EVALUATED
            || evaluation.resolution() == ProofResolution.NOT_EVALUATED;
        if (outcome == ProofOutcome.PROVED
            && (!allSatisfied || !stimulusSatisfied || !evaluationSatisfied)) {
            throw new IllegalArgumentException(
                "PROVED requires completed evaluation, a completed stimulus, and every required item to be SATISFIED"
            );
        }
        if (outcome == ProofOutcome.VIOLATED && (!violated || failed
            || stimulus.resolution() == ProofResolution.FAILED
            || evaluation.resolution() == ProofResolution.FAILED)) {
            throw new IllegalArgumentException(
                "VIOLATED requires an explicit violated obligation and no failed item"
            );
        }
        if (outcome == ProofOutcome.INCONCLUSIVE) {
            boolean exactGap = !allSatisfied
                || stimulus.resolution() != ProofResolution.SATISFIED
                || evaluation.resolution() != ProofResolution.SATISFIED;
            if (!exactGap || violated || failed || notEvaluated
                || stimulus.resolution() == ProofResolution.FAILED
                || evaluation.resolution() == ProofResolution.FAILED) {
                throw new IllegalArgumentException(
                    "INCONCLUSIVE requires an exact unresolved gap and no violated, failed, or not-evaluated item"
                );
            }
        }
        if (outcome == ProofOutcome.ERROR && primaryFailure.isEmpty()
            && !failed && stimulus.resolution() != ProofResolution.FAILED
            && evaluation.resolution() != ProofResolution.FAILED) {
            throw new IllegalArgumentException(
                "ERROR requires a safe primary failure or failed obligation"
            );
        }
        if (outcome == ProofOutcome.ERROR && violated) {
            throw new IllegalArgumentException(
                "ERROR cannot contain an explicit violated obligation"
            );
        }
        if (outcome != ProofOutcome.ERROR && primaryFailure.isPresent()) {
            throw new IllegalArgumentException(
                "Only ERROR may contain a primary framework failure"
            );
        }
        if (outcome != ProofOutcome.VIOLATED && outcome != ProofOutcome.ERROR
            && notEvaluated) {
            throw new IllegalArgumentException(
                "NOT_EVALUATED is permitted only after terminal VIOLATED or ERROR"
            );
        }
    }

    private String render() {
        String lineSeparator = "\n";
        StringBuilder output = new StringBuilder();
        output.append("proof plan=").append(planId)
            .append(" title=").append(title)
            .append(" outcome=").append(outcome)
            .append(" subject=opaque")
            .append(lineSeparator);
        appendDecisive(output, lineSeparator);
        output.append("stimulus=").append(stimulus.state()).append('/')
            .append(stimulus.resolution()).append('/').append(stimulus.reason())
            .append(lineSeparator);
        output.append("evaluation=").append(evaluation.state()).append('/')
            .append(evaluation.resolution()).append('/').append(evaluation.reason())
            .append(lineSeparator);
        primaryFailure.ifPresent(value -> output.append("failure=")
            .append(value.stage()).append('/').append(value.failure().failureType())
            .append(lineSeparator));
        for (ProofObligationResolution resolution : resolutions) {
            output.append(resolution.kind()).append(' ')
                .append(resolution.id()).append(' ')
                .append(resolution.resolution()).append(' ')
                .append(resolution.reason());
            resolution.connectionId().ifPresent(value ->
                output.append(" connection=").append(value)
            );
            if (!resolution.provenance().isEmpty()) {
                output.append(" provenance=")
                    .append(resolution.provenance().stream()
                        .map(value -> value.role() + ":" + value.interaction())
                        .collect(Collectors.joining(",")));
            }
            output.append(lineSeparator);
        }
        for (ProofDiagnostic diagnostic : secondaryDiagnostics) {
            output.append("secondary=").append(diagnostic.stage()).append('/')
                .append(diagnostic.failure().failureType()).append(lineSeparator);
        }
        return output.toString();
    }

    private void appendDecisive(StringBuilder output, String lineSeparator) {
        decisiveResolution().ifPresentOrElse(
            value -> output.append("decisive=").append(value.kind()).append('/')
                .append(value.id()).append('/').append(value.reason())
                .append(lineSeparator),
            () -> output.append("decisive=").append(decisiveWithoutObligation())
                .append(lineSeparator)
        );
    }

    private String decisiveWithoutObligation() {
        if (outcome == ProofOutcome.PROVED) {
            return "all-required-items-satisfied";
        }
        if (outcome == ProofOutcome.INCONCLUSIVE
            && stimulus.resolution() != ProofResolution.SATISFIED) {
            return "STIMULUS/" + stimulus.reason();
        }
        if (outcome == ProofOutcome.INCONCLUSIVE
            && evaluation.resolution() != ProofResolution.SATISFIED) {
            return "EVALUATION/" + evaluation.reason();
        }
        return primaryFailure.map(value ->
            value.stage() + "/" + value.failure().failureType()
        ).orElse("none");
    }

    private static ProofEvaluationResolution legacyEvaluation(
        ProofOutcome outcome,
        Optional<ProofDiagnostic> primaryFailure
    ) {
        outcome = Objects.requireNonNull(outcome, "outcome must not be null");
        primaryFailure = Objects.requireNonNull(primaryFailure, "primaryFailure must not be null");
        if (outcome == ProofOutcome.PROVED || outcome == ProofOutcome.INCONCLUSIVE) {
            return new ProofEvaluationResolution(
                ProofEvaluationState.COMPLETED,
                ProofResolution.SATISFIED,
                ProofResolutionReason.EVALUATION_COMPLETED
            );
        }
        if (outcome == ProofOutcome.ERROR
            && primaryFailure.filter(value -> value.stage() == ProofFailureStage.EVALUATION)
                .isPresent()) {
            return new ProofEvaluationResolution(
                ProofEvaluationState.FAILED,
                ProofResolution.FAILED,
                ProofResolutionReason.EVALUATION_FAILED
            );
        }
        return new ProofEvaluationResolution(
            ProofEvaluationState.NOT_STARTED,
            ProofResolution.NOT_EVALUATED,
            ProofResolutionReason.NOT_EVALUATED_AFTER_TERMINAL_OUTCOME
        );
    }
}
