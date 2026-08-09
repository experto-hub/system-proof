package io.github.jacekkardys.systemproof.proof.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.component.AbstractComponent;
import io.github.jacekkardys.systemproof.component.ComponentId;
import io.github.jacekkardys.systemproof.component.ComponentType;
import io.github.jacekkardys.systemproof.configuration.RuntimeConfig;
import io.github.jacekkardys.systemproof.driver.ComponentRuntime;
import io.github.jacekkardys.systemproof.environment.Environment;
import io.github.jacekkardys.systemproof.environment.EnvironmentBuilder;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofOutcome;
import io.github.jacekkardys.systemproof.proof.ProofPlan;
import io.github.jacekkardys.systemproof.proof.ProofResult;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;

class ProofEvaluationSurfaceUsageTest {
    @Test
    void shouldUseTheExplicitPublicPlanActivationStimulusEvaluationSequence() {
        AtomicInteger stimulusCalls = new AtomicInteger();
        try (Environment environment = new EnvironmentBuilder()
            .components(new SurfaceComponent())
            .build()
            .start()) {
            ProofSubjectRef subject = environment.proofSubjects().create();
            ProofPlan plan = ProofPlan.builder(
                "public-surface-proof",
                "Public surface proof",
                subject,
                Duration.ofSeconds(5)
            ).prerequisite(
                "environment-ready",
                environment.proofs().satisfiedPrerequisite()
            ).build();

            ProofExecution execution = environment.proofs().activate(plan);
            execution.runStimulus(stimulusCalls::incrementAndGet);
            ProofResult result = execution.evaluate().require(ProofOutcome.PROVED);

            assertThat(stimulusCalls).hasValue(1);
            assertThat(execution.result()).isSameAs(result);
        }
    }

    private record SurfaceConfig() implements RuntimeConfig {}

    private static final class SurfaceComponent
        extends AbstractComponent<SurfaceConfig, Void> {
        private SurfaceComponent() {
            super(
                ComponentId.component(ComponentType.of("proof-surface")),
                new SurfaceConfig(),
                Void.class,
                (component, context) -> ComponentRuntime.<Void>runtime().build()
            );
        }
    }
}
