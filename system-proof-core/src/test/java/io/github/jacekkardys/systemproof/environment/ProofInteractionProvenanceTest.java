package io.github.jacekkardys.systemproof.environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.observation.SessionId;
import io.github.jacekkardys.systemproof.proof.ProofInteractionProvenance;
import io.github.jacekkardys.systemproof.proof.ProofObligationId;
import io.github.jacekkardys.systemproof.proof.ProofObligationResolution;
import io.github.jacekkardys.systemproof.proof.ProofRequirementDescriptor;
import io.github.jacekkardys.systemproof.proof.ProofRequirementKind;
import io.github.jacekkardys.systemproof.proof.ProofResolution;
import io.github.jacekkardys.systemproof.proof.ProofResolutionReason;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

class ProofInteractionProvenanceTest {

    @Test
    void shouldValidateSameConnectionGuardProvenanceByExplicitRole() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticPredecessorGuard guard = harness.declareGuard();
            ConnectionId connection = harness.connectionId;
            InteractionRef predecessor = interaction(connection, 1);
            InteractionRef successor = interaction(connection, 2);
            ProofRequirementDescriptor.GuardControl descriptor =
                new ProofRequirementDescriptor.GuardControl(
                    guard.ref(),
                    SemanticPredecessorGuardState.SATISFIED,
                    connection,
                    connection
                );

            assertThat(guardResolution(
                descriptor,
                ProofResolution.TIMED_OUT,
                ProofResolutionReason.CONTROL_TIMED_OUT,
                Optional.empty(),
                List.of(ProofInteractionProvenance.predecessor(predecessor))
            ).provenance()).singleElement().satisfies(value ->
                assertThat(value.role())
                    .isEqualTo(ProofInteractionProvenance.Role.PREDECESSOR)
            );
            assertThat(guardResolution(
                descriptor,
                ProofResolution.VIOLATED,
                ProofResolutionReason.CAUSAL_RELATION_VIOLATED,
                Optional.of(connection),
                List.of(ProofInteractionProvenance.successor(successor))
            ).provenance()).singleElement().satisfies(value ->
                assertThat(value.role())
                    .isEqualTo(ProofInteractionProvenance.Role.SUCCESSOR)
            );
            assertThat(guardResolution(
                descriptor,
                ProofResolution.SATISFIED,
                ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE,
                Optional.empty(),
                List.of(
                    ProofInteractionProvenance.predecessor(predecessor),
                    ProofInteractionProvenance.successor(successor)
                )
            ).interactions()).containsExactly(predecessor, successor);

            assertThatThrownBy(() -> guardResolution(
                descriptor,
                ProofResolution.TIMED_OUT,
                ProofResolutionReason.CONTROL_TIMED_OUT,
                Optional.empty(),
                List.of(ProofInteractionProvenance.successor(successor))
            )).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> guardResolution(
                descriptor,
                ProofResolution.UNREACHED,
                ProofResolutionReason.CONTROL_UNREACHED,
                Optional.empty(),
                List.of(ProofInteractionProvenance.successor(successor))
            )).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> guardResolution(
                descriptor,
                ProofResolution.FAILED,
                ProofResolutionReason.CONTROL_FAILED,
                Optional.empty(),
                List.of(ProofInteractionProvenance.successor(successor))
            )).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> guardResolution(
                descriptor,
                ProofResolution.SATISFIED,
                ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE,
                Optional.empty(),
                List.of(
                    ProofInteractionProvenance.predecessor(predecessor),
                    ProofInteractionProvenance.successor(predecessor)
                )
            )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate");
            assertThatThrownBy(() -> guardResolution(
                descriptor,
                ProofResolution.SATISFIED,
                ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE,
                Optional.empty(),
                List.of(
                    ProofInteractionProvenance.successor(successor),
                    ProofInteractionProvenance.predecessor(predecessor)
                )
            )).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> guardResolution(
                descriptor,
                ProofResolution.VIOLATED,
                ProofResolutionReason.CAUSAL_RELATION_VIOLATED,
                Optional.of(connection),
                List.of(ProofInteractionProvenance.predecessor(predecessor))
            )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("explicit successor");
        }
    }

    @Test
    void shouldAcceptOnlyReachableHoldControlProvenance() {
        try (ProofRuntimeHarness harness = ProofRuntimeHarness.start()) {
            SemanticHold hold = harness.declareHold("held");
            ConnectionId connection = harness.connectionId;
            InteractionRef held = interaction(connection, 1);
            InteractionRef other = interaction(connection, 2);
            ProofRequirementDescriptor.HoldControl descriptor =
                new ProofRequirementDescriptor.HoldControl(
                    hold.ref(),
                    SemanticHoldState.FORWARDED,
                    connection
                );

            assertThat(holdResolution(
                descriptor,
                ProofResolution.SATISFIED,
                ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE,
                List.of(ProofInteractionProvenance.hold(held))
            ).interactions()).containsExactly(held);
            assertThat(holdResolution(
                descriptor,
                ProofResolution.UNREACHED,
                ProofResolutionReason.CONTROL_UNREACHED,
                List.of()
            ).provenance()).isEmpty();
            assertThat(holdResolution(
                descriptor,
                ProofResolution.UNREACHED,
                ProofResolutionReason.CONTROL_UNREACHED,
                List.of(ProofInteractionProvenance.hold(held))
            ).interactions()).containsExactly(held);
            assertThat(holdResolution(
                descriptor,
                ProofResolution.TIMED_OUT,
                ProofResolutionReason.CONTROL_TIMED_OUT,
                List.of(ProofInteractionProvenance.hold(held))
            ).interactions()).containsExactly(held);
            assertThat(holdResolution(
                descriptor,
                ProofResolution.AMBIGUOUS,
                ProofResolutionReason.CONTROL_CORRELATION_INVALIDATED,
                List.of(ProofInteractionProvenance.hold(held))
            ).interactions()).containsExactly(held);
            assertThat(holdResolution(
                descriptor,
                ProofResolution.MISSING,
                ProofResolutionReason.CONTROL_SESSION_ENDED,
                List.of(ProofInteractionProvenance.hold(held))
            ).interactions()).containsExactly(held);
            assertThat(holdResolution(
                descriptor,
                ProofResolution.FAILED,
                ProofResolutionReason.CONTROL_FAILED,
                List.of(ProofInteractionProvenance.hold(held))
            ).interactions()).containsExactly(held);

            assertThatThrownBy(() -> holdResolution(
                descriptor,
                ProofResolution.SATISFIED,
                ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE,
                List.of()
            )).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> holdResolution(
                descriptor,
                ProofResolution.TIMED_OUT,
                ProofResolutionReason.CONTROL_TIMED_OUT,
                List.of()
            )).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> holdResolution(
                descriptor,
                ProofResolution.UNREACHED,
                ProofResolutionReason.CONTROL_UNREACHED,
                List.of(ProofInteractionProvenance.successor(held))
            )).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> holdResolution(
                descriptor,
                ProofResolution.SATISFIED,
                ProofResolutionReason.CONTROL_REACHED_EXPECTED_STATE,
                List.of(
                    ProofInteractionProvenance.hold(held),
                    ProofInteractionProvenance.hold(other)
                )
            )).isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static ProofObligationResolution guardResolution(
        ProofRequirementDescriptor.GuardControl descriptor,
        ProofResolution resolution,
        ProofResolutionReason reason,
        Optional<ConnectionId> connectionId,
        List<ProofInteractionProvenance> provenance
    ) {
        return new ProofObligationResolution(
            new ProofObligationId("guard-provenance"),
            ProofRequirementKind.CONTROL,
            descriptor,
            resolution,
            reason,
            connectionId,
            provenance
        );
    }

    private static ProofObligationResolution holdResolution(
        ProofRequirementDescriptor.HoldControl descriptor,
        ProofResolution resolution,
        ProofResolutionReason reason,
        List<ProofInteractionProvenance> provenance
    ) {
        return new ProofObligationResolution(
            new ProofObligationId("hold-provenance"),
            ProofRequirementKind.CONTROL,
            descriptor,
            resolution,
            reason,
            Optional.of(descriptor.connectionId()),
            provenance
        );
    }

    private static InteractionRef interaction(ConnectionId connectionId, long sessionValue) {
        return new InteractionRef(
            new SessionId(connectionId, sessionValue),
            FlowDirection.CONSUMER_TO_PROVIDER,
            1
        );
    }
}
