package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.journal.FailureDetails;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.observation.SessionId;
import io.github.jacekkardys.systemproof.proof.ProofDiagnostic;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.CorrelationKeySchema;
import io.github.jacekkardys.systemproof.proof.ProofEvidenceKind;
import io.github.jacekkardys.systemproof.proof.ProofEvaluationResolution;
import io.github.jacekkardys.systemproof.proof.ProofEvaluationState;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofFailureStage;
import io.github.jacekkardys.systemproof.proof.ProofInteractionProvenance;
import io.github.jacekkardys.systemproof.proof.ProofObligationId;
import io.github.jacekkardys.systemproof.proof.ProofObligationResolution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofPlanId;
import io.github.jacekkardys.systemproof.proof.ProofPrerequisiteStatus;
import io.github.jacekkardys.systemproof.proof.ProofRequirementDescriptor;
import io.github.jacekkardys.systemproof.proof.ProofRequirementKind;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.proof.ProofStimulusResolution;
import io.github.jacekkardys.systemproof.proof.ProofStimulusState;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

class ProofExecutionFollowUpTest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);

    @Test
    void shouldFailActivationWhenAControlBecomesTerminalBeforeTheEvidenceWindow() {
        try (ProofRuntimeHarness harness =
                 ProofRuntimeHarness.startWithImmediateControlTimeout()) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));

            ProofResult result = execution.result();

            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).hasValueSatisfying(failure ->
                assertThat(failure.stage()).isEqualTo(ProofFailureStage.ACTIVATION)
            );
        }
    }

    @RepeatedTest(20)
    void shouldKeepTheGuardTerminalFactOrEvaluationByTheirExactOrder() {
        try (ProofRuntimeHarness terminalFirst = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = terminalFirst.declareGuard();
            ProofExecution execution = terminalFirst.activate(guardPlan(terminalFirst, guard));

            execution.runStimulus(() -> terminalFirst.publish("successor"));

            assertThat(execution.result().outcome()).isEqualTo(ProofOutcome.VIOLATED);
        }
        try (ProofRuntimeHarness evaluationFirst = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = evaluationFirst.declareGuard();
            ProofExecution execution = evaluationFirst.activate(
                guardPlan(evaluationFirst, guard)
            );
            execution.runStimulus(() -> {});

            ProofResult frozen = execution.evaluate();
            evaluationFirst.publish("successor");

            assertThat(frozen.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            assertThat(execution.result()).isSameAs(frozen);
        }
    }

    @RepeatedTest(20)
    void shouldKeepTheSatisfiedGuardRelationOrEvaluationByTheirExactOrder() {
        try (ProofRuntimeHarness terminalFirst = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = terminalFirst.declareGuard();
            ProofExecution execution = terminalFirst.activate(guardPlan(terminalFirst, guard));

            execution.runStimulus(() -> {
                terminalFirst.publish("predecessor");
                terminalFirst.publish("successor");
            });

            assertThat(execution.evaluate().outcome()).isEqualTo(ProofOutcome.PROVED);
        }
        try (ProofRuntimeHarness evaluationFirst = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = evaluationFirst.declareGuard();
            ProofExecution execution = evaluationFirst.activate(
                guardPlan(evaluationFirst, guard)
            );
            execution.runStimulus(() -> {});

            ProofResult frozen = execution.evaluate();
            evaluationFirst.publish("predecessor");
            evaluationFirst.publish("successor");

            assertThat(frozen.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            assertThat(execution.result()).isSameAs(frozen);
        }
    }

    @RepeatedTest(20)
    void shouldInvalidateAnAcceptedUniqueCorrelationBeforeEvaluation() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            ProofExecution execution = fixture.environment.proofs().activate(
                correlationPlan(fixture, "shared-before-evaluation")
            );
            execution.runStimulus(() -> fixture.correlated("predecessor"));

            fixture.addSubjectForSameKey();

            ProofResult result = execution.evaluate();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
            assertThat(result.resolutions()).anyMatch(value ->
                value.resolution() == ProofResolution.AMBIGUOUS
            );
        }
    }

    @RepeatedTest(20)
    void shouldKeepAProvedCorrelationWhenEvaluationLinearizesFirst() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            ProofExecution execution = fixture.environment.proofs().activate(
                correlationPlan(fixture, "evaluation-before-sharing")
            );
            execution.runStimulus(() -> fixture.correlated("predecessor"));

            ProofResult frozen = execution.evaluate();
            fixture.addSubjectForSameKey();

            assertThat(frozen.outcome()).isEqualTo(ProofOutcome.PROVED);
            assertThat(execution.result()).isSameAs(frozen);
        }
    }

    @Test
    void shouldIsolateAnUnrelatedCorrelationKey() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            ProofExecution execution = fixture.environment.proofs().activate(
                correlationPlan(fixture, "unrelated-key")
            );
            execution.runStimulus(() -> fixture.correlated("predecessor"));
            fixture.environment.proofSubjects().arm(
                fixture.environment.proofSubjects().create(),
                fixture.successorKey
            );

            assertThat(execution.evaluate().outcome()).isEqualTo(ProofOutcome.PROVED);
        }
    }

    @Test
    void shouldRemainAmbiguousForTheSameNativeReferenceAfterKeySharing() {
        try (ProofTestFixture fixture = ProofTestFixture.start()) {
            ProofExecution execution = fixture.environment.proofs().activate(
                correlationPlan(fixture, "same-native-reference")
            );
            execution.runStimulus(() -> fixture.correlated("predecessor"));
            fixture.addSubjectForSameKey();
            fixture.correlated("predecessor");

            assertThat(execution.evaluate().resolutions()).anyMatch(value ->
                value.resolution() == ProofResolution.AMBIGUOUS
            );
        }
    }

    @RepeatedTest(20)
    void shouldLinearizeRequiredObservationFailureAndEvaluationInBothOrders() {
        try (ProofRuntimeHarness failureFirst = ProofRuntimeHarness.start()) {
            ProofExecution execution = observedPrerequisite(failureFirst, "failure-first");
            execution.runStimulus(() -> {});
            failureFirst.controls.observationFailed(failureFirst.connectionId);

            assertThat(execution.result().outcome()).isEqualTo(ProofOutcome.ERROR);
        }
        try (ProofRuntimeHarness evaluationFirst = ProofRuntimeHarness.start()) {
            ProofExecution execution = observedPrerequisite(
                evaluationFirst,
                "observation-evaluation-first"
            );
            execution.runStimulus(() -> {});

            ProofResult frozen = execution.evaluate();
            evaluationFirst.controls.observationFailed(evaluationFirst.connectionId);

            assertThat(frozen.outcome()).isEqualTo(ProofOutcome.PROVED);
            assertThat(execution.result()).isSameAs(frozen);
        }
    }

    @Test
    void shouldRejectEvaluationBeforeTheStimulus() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = prerequisite(harness, "before-stimulus");

            assertThatThrownBy(execution::evaluate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("successfully completed stimulus");
        }
    }

    @RepeatedTest(20)
    void shouldRejectEvaluationWhileTheStimulusIsRunning() throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = prerequisite(harness, "running-stimulus");
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                Future<?> stimulus = executor.submit(() -> execution.runStimulus(() -> {
                    entered.countDown();
                    await(release);
                }));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

                assertThatThrownBy(execution::evaluate)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("successfully completed stimulus");
                release.countDown();
                stimulus.get(5, TimeUnit.SECONDS);
            }

            assertThat(execution.evaluate().outcome()).isEqualTo(ProofOutcome.PROVED);
        }
    }

    @Test
    void shouldExposeARealStimulusGapWhenDeadlinePrecedesTheStimulus() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = prerequisite(harness, "deadline-before-stimulus");

            harness.deadlines.fireRacingCallback();

            assertDeadlineStimulusGap(execution.result(), ProofStimulusState.NOT_STARTED);
        }
    }

    @RepeatedTest(20)
    void shouldExposeARealStimulusGapWhenDeadlineRunsDuringTheStimulus()
        throws Exception {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = prerequisite(harness, "deadline-during-stimulus");
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
                Future<?> stimulus = executor.submit(() -> execution.runStimulus(() -> {
                    entered.countDown();
                    await(release);
                }));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

                harness.deadlines.fireRacingCallback();
                release.countDown();
                stimulus.get(5, TimeUnit.SECONDS);
            }

            assertDeadlineStimulusGap(execution.result(), ProofStimulusState.RUNNING);
        }
    }

    @Test
    void shouldRetainDeadlineCancellationFailureAfterEveryPrimaryOutcome() {
        assertDeadlineCancellation(ProofOutcome.PROVED, (harness, execution) -> {
            execution.runStimulus(() -> {});
            return execution.evaluate();
        });
        assertDeadlineCancellation(ProofOutcome.INCONCLUSIVE, (harness, execution) -> {
            execution.runStimulus(() -> {});
            return execution.evaluate();
        }, true);
        assertDeadlineCancellation(ProofOutcome.ERROR, (harness, execution) -> {
            execution.runStimulus(() -> {
                throw new StimulusFailure();
            });
            return execution.result();
        });
        try (ProofRuntimeHarness harness = failingDeadlineHarness()) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));
            execution.runStimulus(() -> harness.publish("successor"));
            assertCancellationResult(execution.result(), ProofOutcome.VIOLATED);
        }
    }

    @Test
    void shouldRetainAControlCancellationFailureWithoutReplacingThePrimary() {
        try (ProofRuntimeHarness harness =
                 ProofRuntimeHarness.startWithFailingControlCancellation()) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofExecution execution = harness.activate(guardPlan(harness, guard));

            execution.runStimulus(() -> {
                throw new StimulusFailure();
            });

            ProofResult result = execution.result();
            assertThat(result.outcome()).isEqualTo(ProofOutcome.ERROR);
            assertThat(result.primaryFailure()).hasValueSatisfying(value ->
                assertThat(value.stage()).isEqualTo(ProofFailureStage.STIMULUS)
            );
            assertThat(result.secondaryDiagnostics()).anySatisfy(value ->
                assertThat(value.failure().failureType())
                    .isEqualTo("ControlCancellationFailure")
            );
        }
    }

    @Test
    void shouldFreezeConcurrentSecondaryDiagnosticsInStableBoundedOrder()
        throws Exception {
        CountDownLatch cancellationEntered = new CountDownLatch(1);
        CountDownLatch cancellationRelease = new CountDownLatch(1);
        ProofRuntimeHarness.ManualDeadlineScheduler scheduler =
            new ProofRuntimeHarness.ManualDeadlineScheduler(
                cancellationEntered,
                cancellationRelease,
                null
            );
        try (ProofRuntimeHarness harness =
                 ProofRuntimeHarness.startWithDeadlineScheduler(scheduler);
             ExecutorService executor = Executors.newFixedThreadPool(10)) {
            ProofExecution execution = prerequisite(harness, "concurrent-secondary");
            execution.runStimulus(() -> {});
            Future<ProofResult> evaluation = executor.submit(execution::evaluate);
            assertThat(cancellationEntered.await(5, TimeUnit.SECONDS)).isTrue();
            Future<ProofResult> earlyResult = executor.submit(execution::result);

            List<Future<?>> publications = new ArrayList<>();
            for (int index = 0; index < 64; index++) {
                boolean alpha = index % 2 == 0;
                publications.add(executor.submit(() ->
                    harness.proofs.journalFailure(
                        alpha ? new AlphaSecondaryFailure() : new BetaSecondaryFailure()
                    )
                ));
            }
            for (Future<?> publication : publications) {
                publication.get(5, TimeUnit.SECONDS);
            }
            cancellationRelease.countDown();

            ProofResult result = evaluation.get(5, TimeUnit.SECONDS);
            assertThat(earlyResult.get(5, TimeUnit.SECONDS)).isSameAs(result);
            assertThat(result.secondaryDiagnostics()).hasSize(32)
                .allMatch(value -> value.failure().failureType()
                    .equals("AlphaSecondaryFailure"));
            assertThat(execution.result().report()).isEqualTo(result.report());
        }
    }

    @Test
    void shouldFreezeTheSameReportForEarlyAndLateResultAccess() {
        String early;
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = prerequisite(harness, "stable-result-access");
            harness.frameworkFailure();
            ProofResult frozen = execution.result();
            harness.proofs.journalFailure(new BetaSecondaryFailure());
            early = frozen.report().content();
            assertThat(execution.result()).isSameAs(frozen);
        }
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofExecution execution = prerequisite(harness, "stable-result-access");
            harness.frameworkFailure();
            harness.proofs.journalFailure(new BetaSecondaryFailure());
            ProofResult late = execution.result();

            assertThat(late.report().content()).isEqualTo(early);
            assertThat(late.secondaryDiagnostics()).isEmpty();
        }
    }

    @Test
    void shouldRejectContradictoryPublicResultMatrices() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ProofObligationResolution satisfied = prerequisiteResolution(
                ProofResolution.SATISFIED,
                ProofResolutionReason.PREREQUISITE_SATISFIED
            );
            ProofObligationResolution missing = observationResolution(
                ProofResolution.MISSING,
                ProofResolutionReason.OBSERVATION_LOST,
                harness.connectionId
            );
            ProofObligationResolution violated = violatedRelation(harness);
            ProofObligationResolution failed = prerequisiteResolution(
                ProofResolution.FAILED,
                ProofResolutionReason.PREREQUISITE_FAILED
            );

            assertInvalidResult(harness, ProofOutcome.PROVED, completedStimulus(), missing);
            assertInvalidResult(
                harness,
                ProofOutcome.PROVED,
                incompleteStimulus(),
                satisfied
            );
            assertInvalidResult(
                harness,
                ProofOutcome.VIOLATED,
                incompleteStimulus(),
                violated,
                failed
            );
            assertInvalidResult(
                harness,
                ProofOutcome.INCONCLUSIVE,
                completedStimulus(),
                satisfied
            );
            assertInvalidResult(
                harness,
                ProofOutcome.INCONCLUSIVE,
                incompleteStimulus(),
                failed
            );
            assertInvalidResult(harness, ProofOutcome.ERROR, incompleteStimulus(), missing);
            assertThatThrownBy(() -> new ProofResult(
                new ProofPlanId("invalid-deadline-proved"),
                "Invalid deadline proved",
                ProofOutcome.PROVED,
                harness.subject,
                completedStimulus(),
                new ProofEvaluationResolution(
                    ProofEvaluationState.RUNNING,
                    ProofResolution.TIMED_OUT,
                    ProofResolutionReason.DEADLINE_EXPIRED
                ),
                List.of(satisfied),
                Optional.empty(),
                List.of()
            )).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void shouldRejectForgedDescriptorResolutionReasonAndProvenanceMatrices() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            ConnectionId otherConnection = ConnectionId.of(
                "other-client[].required->other-server[].provided"
            );
            InteractionRef otherInteraction = new InteractionRef(
                new SessionId(otherConnection, 1),
                FlowDirection.CONSUMER_TO_PROVIDER,
                1
            );
            SemanticPredecessorGuard guard = harness.declareGuard();

            assertThatThrownBy(() -> new ProofObligationResolution(
                new ProofObligationId("forged-prerequisite"),
                ProofRequirementKind.PREREQUISITE,
                new ProofRequirementDescriptor.Prerequisite(
                    ProofPrerequisiteStatus.SATISFIED
                ),
                ProofResolution.VIOLATED,
                ProofResolutionReason.CAUSAL_RELATION_VIOLATED,
                Optional.empty(),
                List.of()
            )).isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(() -> new ProofObligationResolution(
                new ProofObligationId("forged-correlation-reason"),
                ProofRequirementKind.CORRELATION,
                new ProofRequirementDescriptor.Correlation(
                    harness.subject,
                    harness.key,
                    harness.connectionId,
                    ProofTestFixture.NATIVE_SCHEMA
                ),
                ProofResolution.MISSING,
                ProofResolutionReason.EVIDENCE_MISSING,
                Optional.of(harness.connectionId),
                List.of()
            )).isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(() -> new ProofObligationResolution(
                new ProofObligationId("forged-correlation-connection"),
                ProofRequirementKind.CORRELATION,
                new ProofRequirementDescriptor.Correlation(
                    harness.subject,
                    harness.key,
                    harness.connectionId,
                    ProofTestFixture.NATIVE_SCHEMA
                ),
                ProofResolution.SATISFIED,
                ProofResolutionReason.CORRELATION_UNIQUE,
                Optional.of(harness.connectionId),
                List.of(ProofInteractionProvenance.correlation(otherInteraction))
            )).isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(() -> new ProofObligationResolution(
                new ProofObligationId("forged-violation-provenance"),
                ProofRequirementKind.CAUSAL_RELATION,
                new ProofRequirementDescriptor.CausalRelation(
                    guard.ref(),
                    otherConnection,
                    harness.connectionId
                ),
                ProofResolution.VIOLATED,
                ProofResolutionReason.CAUSAL_RELATION_VIOLATED,
                Optional.of(harness.connectionId),
                List.of(ProofInteractionProvenance.successor(otherInteraction))
            )).isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(() -> new ProofObligationResolution(
                new ProofObligationId("forged-not-evaluated-reason"),
                ProofRequirementKind.OBSERVATION,
                new ProofRequirementDescriptor.Observation(
                    harness.connectionId,
                    ProofTestFixture.PROFILE
                ),
                ProofResolution.NOT_EVALUATED,
                ProofResolutionReason.OBSERVATION_LOST,
                Optional.of(harness.connectionId),
                List.of()
            )).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void shouldSortAndCapDiagnosticsSuppliedThroughThePublicResultContract() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            List<ProofDiagnostic> supplied = new ArrayList<>();
            for (int index = 0; index < 20; index++) {
                supplied.add(new ProofDiagnostic(
                    ProofFailureStage.JOURNAL,
                    FailureDetails.from(new BetaSecondaryFailure())
                ));
                supplied.add(new ProofDiagnostic(
                    ProofFailureStage.CLEANUP,
                    FailureDetails.from(new AlphaSecondaryFailure())
                ));
            }

            ProofResult result = new ProofResult(
                new ProofPlanId("public-diagnostic-order"),
                "Public diagnostic order",
                ProofOutcome.PROVED,
                harness.subject,
                completedStimulus(),
                List.of(prerequisiteResolution(
                    ProofResolution.SATISFIED,
                    ProofResolutionReason.PREREQUISITE_SATISFIED
                )),
                Optional.empty(),
                supplied
            );

            List<ProofDiagnostic> expected = supplied.stream()
                .sorted(java.util.Comparator
                    .comparing((ProofDiagnostic value) -> value.stage().ordinal())
                    .thenComparing(value -> value.failure().failureType()))
                .limit(32)
                .toList();
            assertThat(result.secondaryDiagnostics()).containsExactlyElementsOf(expected);
        }
    }

    @Test
    void shouldDetachExactSafeDescriptorsWithoutTheOriginalPlan() {
        String canary = "DESCRIPTOR_SECRET";
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            byte[] digest = canary.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            CorrelationKey canaryKey = CorrelationKey.ofDigest(
                new CorrelationKeySchema("system-proof-test", "descriptor-canary", 1),
                digest
            );
            harness.proofSubjects.arm(harness.subject, canaryKey);
            io.github.jacekkardys.systemproof.control.SemanticHold hold =
                harness.declareHold("held");
            SemanticPredecessorGuard guard = harness.declareGuard();
            ProofPlan plan = ProofPlan.builder(
                "detached-descriptors",
                "Detached descriptors",
                harness.subject,
                DEADLINE
            ).prerequisite(
                "prerequisite",
                harness.prerequisite()
            ).observation(
                "observation",
                harness.connectionId,
                ProofTestFixture.PROFILE
            ).correlation(
                "correlation",
                harness.connectionId,
                canaryKey,
                ProofTestFixture.NATIVE_SCHEMA
            ).control(
                "hold-control",
                hold,
                io.github.jacekkardys.systemproof.control.SemanticHoldState.FORWARDED
            ).control(
                "guard-control",
                guard,
                SemanticPredecessorGuardState.SATISFIED
            ).evidence(
                "hold-evidence",
                hold
            ).evidence(
                "predecessor-evidence",
                guard,
                ProofEvidenceKind.PREDECESSOR_INTERACTION
            ).causalRelation("relation", guard).build();
            ProofExecution execution = harness.activate(plan);
            execution.runStimulus(() -> {
                throw new StimulusFailure();
            });

            List<ProofRequirementDescriptor> descriptors = execution.result()
                .resolutions().stream()
                .map(ProofObligationResolution::descriptor)
                .toList();

            assertThat(descriptors).anySatisfy(value -> assertThat(value)
                .isInstanceOf(ProofRequirementDescriptor.Prerequisite.class));
            assertThat(descriptors).anySatisfy(value -> assertThat(value)
                .isEqualTo(new ProofRequirementDescriptor.Observation(
                    harness.connectionId,
                    ProofTestFixture.PROFILE
                )));
            assertThat(descriptors).anySatisfy(value -> assertThat(value)
                .isEqualTo(new ProofRequirementDescriptor.Correlation(
                    harness.subject,
                    canaryKey,
                    harness.connectionId,
                    ProofTestFixture.NATIVE_SCHEMA
                )));
            assertThat(descriptors).anyMatch(
                ProofRequirementDescriptor.HoldControl.class::isInstance
            ).anyMatch(ProofRequirementDescriptor.GuardControl.class::isInstance)
                .anyMatch(ProofRequirementDescriptor.HoldEvidence.class::isInstance)
                .anyMatch(ProofRequirementDescriptor.GuardEvidence.class::isInstance)
                .anyMatch(ProofRequirementDescriptor.CausalRelation.class::isInstance);
            assertThat(descriptors.toString()).doesNotContain(canary);
            assertThat(execution.result().report().content()).doesNotContain(canary);
        }
    }

    @Test
    void shouldKeepTheDecisiveReasonInAMaximumSizeTruncatedReport() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            String source = "s".repeat(980);
            String target = "t".repeat(980);
            ConnectionId connectionId = ConnectionId.of(
                source + "[].p->" + target + "[].p"
            );
            InteractionRef first = new InteractionRef(
                new SessionId(connectionId, 1),
                FlowDirection.CONSUMER_TO_PROVIDER,
                1
            );
            InteractionRef second = new InteractionRef(
                new SessionId(connectionId, 2),
                FlowDirection.CONSUMER_TO_PROVIDER,
                1
            );
            List<ProofObligationResolution> resolutions = new ArrayList<>();
            for (int index = 0; index < 256; index++) {
                resolutions.add(new ProofObligationResolution(
                    new ProofObligationId("obligation-" + index + "-" + "x".repeat(100)),
                    ProofRequirementKind.CAUSAL_RELATION,
                    new ProofRequirementDescriptor.CausalRelation(
                        guard.ref(),
                        connectionId,
                        connectionId
                    ),
                    ProofResolution.VIOLATED,
                    ProofResolutionReason.CAUSAL_RELATION_VIOLATED,
                    Optional.of(connectionId),
                    List.of(ProofInteractionProvenance.successor(second))
                ));
            }

            ProofResult result = new ProofResult(
                new ProofPlanId("maximum-report"),
                "Maximum report",
                ProofOutcome.VIOLATED,
                harness.subject,
                incompleteStimulus(),
                resolutions,
                Optional.empty(),
                List.of()
            );

            assertThat(result.report().content()).contains(
                "decisive=CAUSAL_RELATION/obligation-0-"
            ).contains("[PROOF REPORT TRUNCATED]");
            assertThat(result.report().content().indexOf("decisive="))
                .isLessThan(result.report().content().indexOf("[PROOF REPORT TRUNCATED]"));
        }
    }

    private static ProofPlan guardPlan(
        ProofRuntimeHarness harness,
        SemanticPredecessorGuard guard
    ) {
        return ProofPlan.builder(
            "guard-plan",
            "Guard plan",
            harness.subject,
            DEADLINE
        ).observation(
            "observation",
            harness.connectionId,
            ProofTestFixture.PROFILE
        ).control(
            "guard-control",
            guard,
            SemanticPredecessorGuardState.SATISFIED
        ).build();
    }

    private static ProofPlan correlationPlan(ProofTestFixture fixture, String id) {
        return ProofPlan.builder(id, "Correlation plan", fixture.subject, DEADLINE)
            .observation("observation", fixture.connectionId, ProofTestFixture.PROFILE)
            .correlation(
                "correlation",
                fixture.connectionId,
                fixture.key,
                ProofTestFixture.NATIVE_SCHEMA
            ).build();
    }

    private static ProofExecution prerequisite(ProofRuntimeHarness harness, String id) {
        return harness.activate(ProofPlan.builder(
            id,
            "Prerequisite plan",
            harness.subject,
            DEADLINE
        ).prerequisite("prerequisite", harness.prerequisite()).build());
    }

    private static ProofExecution observedPrerequisite(
        ProofRuntimeHarness harness,
        String id
    ) {
        return harness.activate(ProofPlan.builder(
            id,
            "Observed prerequisite plan",
            harness.subject,
            DEADLINE
        ).prerequisite("prerequisite", harness.prerequisite())
            .observation("observation", harness.connectionId, ProofTestFixture.PROFILE)
            .build());
    }

    private static void assertDeadlineStimulusGap(
        ProofResult result,
        ProofStimulusState expectedState
    ) {
        assertThat(result.outcome()).isEqualTo(ProofOutcome.INCONCLUSIVE);
        assertThat(result.stimulus().state()).isEqualTo(expectedState);
        assertThat(result.stimulus().resolution()).isEqualTo(ProofResolution.TIMED_OUT);
        assertThat(result.report().content()).contains("decisive=STIMULUS/DEADLINE_EXPIRED");
    }

    private static ProofRuntimeHarness failingDeadlineHarness() {
        return ProofRuntimeHarness.startWithDeadlineScheduler(
            new ProofRuntimeHarness.ManualDeadlineScheduler(
                null,
                null,
                new ProofRuntimeHarness.DeadlineCancellationFailure()
            )
        );
    }

    private static void assertDeadlineCancellation(
        ProofOutcome expected,
        OutcomeAction action
    ) {
        assertDeadlineCancellation(expected, action, false);
    }

    private static void assertDeadlineCancellation(
        ProofOutcome expected,
        OutcomeAction action,
        boolean correlationPlan
    ) {
        try (ProofRuntimeHarness harness = failingDeadlineHarness()) {
            ProofExecution execution = correlationPlan
                ? harness.activate(ProofPlan.builder(
                    "deadline-cancel-inconclusive",
                    "Deadline cancellation inconclusive",
                    harness.subject,
                    DEADLINE
                ).observation(
                    "observation",
                    harness.connectionId,
                    ProofTestFixture.PROFILE
                ).correlation(
                    "correlation",
                    harness.connectionId,
                    harness.key,
                    ProofTestFixture.NATIVE_SCHEMA
                ).build())
                : prerequisite(harness, "deadline-cancel-" + expected.name().toLowerCase());
            assertCancellationResult(action.run(harness, execution), expected);
        }
    }

    private static void assertCancellationResult(ProofResult result, ProofOutcome expected) {
        assertThat(result.outcome()).isEqualTo(expected);
        assertThat(result.secondaryDiagnostics()).anySatisfy(value -> {
            assertThat(value.stage()).isEqualTo(ProofFailureStage.CLEANUP);
            assertThat(value.failure().failureType())
                .isEqualTo("DeadlineCancellationFailure");
        });
        assertThat(result.report()).isEqualTo(result.report());
    }

    private static ProofObligationResolution prerequisiteResolution(
        ProofResolution resolution,
        ProofResolutionReason reason
    ) {
        return new ProofObligationResolution(
            new ProofObligationId("matrix-" + resolution.name().toLowerCase()),
            ProofRequirementKind.PREREQUISITE,
            new ProofRequirementDescriptor.Prerequisite(
                resolution == ProofResolution.FAILED
                    ? ProofPrerequisiteStatus.FAILED
                    : ProofPrerequisiteStatus.SATISFIED
            ),
            resolution,
            reason,
            Optional.empty(),
            List.of()
        );
    }

    private static ProofObligationResolution observationResolution(
        ProofResolution resolution,
        ProofResolutionReason reason,
        ConnectionId connectionId
    ) {
        return new ProofObligationResolution(
            new ProofObligationId("matrix-observation"),
            ProofRequirementKind.OBSERVATION,
            new ProofRequirementDescriptor.Observation(
                connectionId,
                ProofTestFixture.PROFILE
            ),
            resolution,
            reason,
            Optional.of(connectionId),
            List.of()
        );
    }

    private static ProofObligationResolution violatedRelation(ProofRuntimeHarness harness) {
        SemanticPredecessorGuard guard = harness.declareGuard();
        InteractionRef successor = new InteractionRef(
            new SessionId(harness.connectionId, 1),
            FlowDirection.CONSUMER_TO_PROVIDER,
            1
        );
        return new ProofObligationResolution(
            new ProofObligationId("matrix-violation"),
            ProofRequirementKind.CAUSAL_RELATION,
            new ProofRequirementDescriptor.CausalRelation(
                guard.ref(),
                harness.connectionId,
                harness.connectionId
            ),
            ProofResolution.VIOLATED,
            ProofResolutionReason.CAUSAL_RELATION_VIOLATED,
            Optional.of(harness.connectionId),
            List.of(ProofInteractionProvenance.successor(successor))
        );
    }

    private static ProofStimulusResolution completedStimulus() {
        return new ProofStimulusResolution(
            ProofStimulusState.COMPLETED,
            ProofResolution.SATISFIED,
            ProofResolutionReason.STIMULUS_COMPLETED
        );
    }

    private static ProofStimulusResolution incompleteStimulus() {
        return new ProofStimulusResolution(
            ProofStimulusState.NOT_STARTED,
            ProofResolution.NOT_EVALUATED,
            ProofResolutionReason.NOT_EVALUATED_AFTER_TERMINAL_OUTCOME
        );
    }

    private static void assertInvalidResult(
        ProofRuntimeHarness harness,
        ProofOutcome outcome,
        ProofStimulusResolution stimulus,
        ProofObligationResolution... resolutions
    ) {
        Optional<ProofDiagnostic> primary = outcome == ProofOutcome.ERROR
            ? Optional.empty()
            : Optional.empty();
        assertThatThrownBy(() -> new ProofResult(
            new ProofPlanId("invalid-" + outcome.name().toLowerCase()),
            "Invalid result",
            outcome,
            harness.subject,
            stimulus,
            List.of(resolutions),
            primary,
            List.of()
        )).isInstanceOf(IllegalArgumentException.class);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Test latch was not released");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while awaiting test latch", interrupted);
        }
    }

    @FunctionalInterface
    private interface OutcomeAction {
        ProofResult run(ProofRuntimeHarness harness, ProofExecution execution);
    }

    private static final class StimulusFailure extends RuntimeException {}

    private static final class AlphaSecondaryFailure extends RuntimeException {}

    private static final class BetaSecondaryFailure extends RuntimeException {}
}
