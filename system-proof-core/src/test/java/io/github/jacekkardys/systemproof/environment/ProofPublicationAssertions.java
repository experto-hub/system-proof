package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import io.github.jacekkardys.systemproof.proof.ProofExecution;
import io.github.jacekkardys.systemproof.proof.ProofResult;

final class ProofPublicationAssertions {
    private static final int CONCURRENT_READERS = 8;
    private static final int READS = 16;

    private ProofPublicationAssertions() {}

    static void assertNormallyPublishedOnce(
        ProofExecution execution,
        ProofResult expected
    ) {
        assertNormallyPublishedOnce(execution, expected, 0);
    }

    static void assertNormallyPublishedOnce(
        ProofExecution execution,
        ProofResult expected,
        int expectedRecoveryCount
    ) {
        assertThat(execution.result()).isSameAs(expected);
        assertThat(execution.evaluate()).isSameAs(expected);

        ExecutorService readers = Executors.newFixedThreadPool(CONCURRENT_READERS);
        try {
            List<Future<ProofResult>> reads = new ArrayList<>(READS);
            for (int index = 0; index < READS; index++) {
                boolean evaluate = index % 2 == 0;
                reads.add(readers.submit(evaluate ? execution::evaluate : execution::result));
            }
            for (Future<ProofResult> read : reads) {
                assertThat(read.get(5, TimeUnit.SECONDS)).isSameAs(expected);
            }
        } catch (Exception failure) {
            throw new AssertionError("Concurrent proof result publication did not finish", failure);
        } finally {
            readers.shutdownNow();
        }

        ProofExecutionCoordinator.PublicationInvariant invariant =
            ProofExecutionCoordinator.publicationInvariant(execution);
        assertThat(invariant.outcomeSelected()).isTrue();
        assertThat(invariant.primaryResolutionCount())
            .isEqualTo(invariant.expectedPrimaryResolutionCount());
        assertThat(invariant.strictPrimaryResolutions()).isTrue();
        assertThat(invariant.resultReadyCompletedNormally()).isTrue();
        assertThat(invariant.resultIdentityPublished()).isTrue();
        assertThat(invariant.finalizationReadyCompletedNormally()).isTrue();
        assertThat(invariant.finalizationComplete()).isTrue();
        assertThat(invariant.finalizing()).isFalse();
        assertThat(invariant.finalizationOwnerPresent()).isFalse();
        assertThat(invariant.authoritativeOutcomeBoundaryPending()).isFalse();
        assertThat(invariant.resultConstructionRecoveryCount())
            .isEqualTo(expectedRecoveryCount);
    }
}
