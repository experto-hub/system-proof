package io.github.jacekkardys.systemproof.testcontainers.gateway;

import static io.github.jacekkardys.systemproof.environment.ComponentPortFactory.requiresAtStartup;
import static io.github.jacekkardys.systemproof.environment.ComponentPortFactory.provides;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.groups.Tuple.tuple;
import static io.github.jacekkardys.systemproof.topology.Contract.contract;
import static io.github.jacekkardys.systemproof.endpoint.EndpointBinding.binding;
import static io.github.jacekkardys.systemproof.testcontainers.gateway.TcpEndpointAdapter.endpoint;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import io.github.jacekkardys.systemproof.environment.EnvironmentLogging;
import io.github.jacekkardys.systemproof.driver.ComponentDriver;
import io.github.jacekkardys.systemproof.driver.ComponentRuntime;
import io.github.jacekkardys.systemproof.environment.ConnectionRouting;
import io.github.jacekkardys.systemproof.environment.ConnectionRouteProvider;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.CorrelationResult;
import io.github.jacekkardys.systemproof.environment.EnvironmentStartException;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.journal.CorrelationCandidateEvent;
import io.github.jacekkardys.systemproof.journal.FailureEvent;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.journal.InteractionObservationEvent;
import io.github.jacekkardys.systemproof.journal.ScenarioEvent;
import io.github.jacekkardys.systemproof.component.AbstractComponent;
import io.github.jacekkardys.systemproof.component.ComponentId;
import io.github.jacekkardys.systemproof.component.ComponentType;
import io.github.jacekkardys.systemproof.environment.state.ConnectionState;
import io.github.jacekkardys.systemproof.topology.Contract;
import io.github.jacekkardys.systemproof.environment.Environment;
import io.github.jacekkardys.systemproof.environment.EnvironmentBuilder;
import io.github.jacekkardys.systemproof.environment.EnvironmentTopology;
import io.github.jacekkardys.systemproof.observation.EffectiveObservationStatus;
import io.github.jacekkardys.systemproof.observation.EvidenceSchemaId;
import io.github.jacekkardys.systemproof.topology.InteractionSpec;
import io.github.jacekkardys.systemproof.topology.ProtocolSpec;
import io.github.jacekkardys.systemproof.observation.ObservationRequirement;
import io.github.jacekkardys.systemproof.observation.RequiredObservationProfile;
import io.github.jacekkardys.systemproof.observation.RequiredObservationProfile.Capability;
import io.github.jacekkardys.systemproof.observation.RequiredObservationProfile.Feature;
import io.github.jacekkardys.systemproof.topology.ProvidedPort;
import io.github.jacekkardys.systemproof.topology.RequiredPort;
import io.github.jacekkardys.systemproof.environment.state.RoutingMode;
import io.github.jacekkardys.systemproof.configuration.RuntimeConfig;
import io.github.jacekkardys.systemproof.configuration.Secret;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldFailure;
import io.github.jacekkardys.systemproof.control.SemanticInteractionSelector;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardSpec;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorRequirement;
import io.github.jacekkardys.systemproof.journal.SemanticHoldEvent;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

class InteractionGatewayTest {
    private static final ComponentType CLIENT = ComponentType.of("gateway-client");
    private static final ComponentType SERVER = ComponentType.of("gateway-server");
    private static final Contract<CommandEndpoint> COMMAND =
        contract("command", CommandEndpoint.class);
    private static final Contract<SessionEndpoint> SESSION =
        contract("session", SessionEndpoint.class);

    @Test
    void shouldRejectSemanticArmForOutsideOptionalAndTransparentConnections() {
        ControllableGatewayListener listener = ControllableGatewayListener.scripted(32139);
        RoutedEnvironment environment = observedEnvironment(
            ObservationRequirement.OPTIONAL,
            listener
        );
        ConnectionId optional = environment.runtimeConnections().stream()
            .filter(snapshot -> snapshot.observationRequirement()
                == ObservationRequirement.OPTIONAL)
            .findFirst()
            .orElseThrow()
            .id();
        ConnectionId transparent = environment.runtimeConnections().stream()
            .filter(snapshot -> snapshot.observationRequirement()
                == ObservationRequirement.DISABLED)
            .findFirst()
            .orElseThrow()
            .id();

        assertThatThrownBy(() -> environment.controls().arm(
            semanticSelector(ConnectionId.of("outside[].out->missing[].in")),
            Duration.ofSeconds(5)
        )).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("outside the environment");
        assertThatThrownBy(() -> environment.controls().arm(
            semanticSelector(optional),
            Duration.ofSeconds(5)
        )).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("does not declare semantic-control capability");
        assertThatThrownBy(() -> environment.controls().arm(
            semanticSelector(transparent),
            Duration.ofSeconds(5)
        )).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("does not declare semantic-control capability");

        environment.close();
    }

    @Test
    void shouldRejectWrongProtocolAdapterBeforeRoutePreparation() {
        LengthPrefixedProtocolAdapter delegate = new LengthPrefixedProtocolAdapter();
        ProtocolAdapter<LengthPrefixedProtocolAdapter.FrameEvidence> wrongProtocol =
            new ProtocolAdapter<>() {
                @Override
                public Optional<ProtocolObservationContract> observationContract() {
                    ProtocolObservationContract declared = delegate.observationContract()
                        .orElseThrow();
                    return Optional.of(new ProtocolObservationContract(
                        "smpp",
                        "smpp",
                        declared.endpointType(),
                        declared.evidenceSchema(),
                        declared.nativeFlowReferenceSchema(),
                        declared.capabilities(),
                        declared.supportedFeatures()
                    ));
                }

                @Override
                public io.github.jacekkardys.systemproof.observation.EvidenceCodec<
                    LengthPrefixedProtocolAdapter.FrameEvidence
                > evidenceCodec() {
                    return delegate.evidenceCodec();
                }

                @Override
                public ProtocolSession<LengthPrefixedProtocolAdapter.FrameEvidence>
                    openSession(ProtocolLimits limits) {
                    throw new AssertionError("An incompatible adapter must not open a session");
                }
            };
        RoutedEnvironment environment = observedEnvironment(
            ObservationRequirement.REQUIRED,
            ControllableGatewayListener.scripted(32142),
            wrongProtocol
        );

        EnvironmentStartException thrown = catchThrowableOfType(
            environment::start,
            EnvironmentStartException.class
        );

        assertThat(thrown.getCause())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "Protocol adapter is incompatible with connection '"
                    + environment.connections().getFirst().id()
                    + "': protocol mismatch"
            );
    }

    @Test
    void shouldConsumeEveryRequiredObservationProfileFieldBeforeTraffic() {
        EvidenceSchemaId otherSchema = new EvidenceSchemaId(
            "system-proof-test",
            "other-frame",
            1
        );
        Set<Capability> baseCapabilities = Set.of(
            Capability.CORRELATION_CONTRIBUTIONS,
            Capability.SEMANTIC_CONTROL
        );

        assertRequiredProfileRejected(
            requiredLengthProfile(otherSchema, baseCapabilities, Set.of()),
            "required evidence schema"
        );
        assertRequiredProfileRejected(
            new RequiredObservationProfile(
                LengthPrefixedProtocolAdapter.CODEC.schemaId(),
                Optional.of(otherSchema),
                baseCapabilities,
                Set.of()
            ),
            "required native-flow schema"
        );
        assertRequiredProfileRejected(
            requiredLengthProfile(
                LengthPrefixedProtocolAdapter.CODEC.schemaId(),
                baseCapabilities,
                Set.of(Feature.ENCRYPTED_TRANSPORT)
            ),
            "required protocol features"
        );
    }

    @Test
    void shouldAcceptOnlyAnExplicitlySupportedRequiredFeature() {
        LengthPrefixedProtocolAdapter delegate = new LengthPrefixedProtocolAdapter();
        ProtocolAdapter<LengthPrefixedProtocolAdapter.FrameEvidence> supporting =
            new ProtocolAdapter<>() {
                @Override
                public Optional<ProtocolObservationContract> observationContract() {
                    ProtocolObservationContract declared = delegate.observationContract()
                        .orElseThrow();
                    return Optional.of(new ProtocolObservationContract(
                        declared.protocolId(),
                        declared.protocolScheme(),
                        declared.endpointType(),
                        declared.evidenceSchema(),
                        declared.nativeFlowReferenceSchema(),
                        declared.capabilities(),
                        Set.of(Feature.ENCRYPTED_TRANSPORT)
                    ));
                }

                @Override
                public io.github.jacekkardys.systemproof.observation.EvidenceCodec<
                    LengthPrefixedProtocolAdapter.FrameEvidence
                > evidenceCodec() {
                    return delegate.evidenceCodec();
                }

                @Override
                public ProtocolSession<LengthPrefixedProtocolAdapter.FrameEvidence> openSession(
                    ProtocolLimits limits
                ) {
                    return delegate.openSession(limits);
                }
            };
        RequiredObservationProfile profile = requiredLengthProfile(
            LengthPrefixedProtocolAdapter.CODEC.schemaId(),
            Set.of(
                Capability.CORRELATION_CONTRIBUTIONS,
                Capability.SEMANTIC_CONTROL
            ),
            Set.of(Feature.ENCRYPTED_TRANSPORT)
        );
        RoutedEnvironment environment = observedEnvironment(
            ObservationRequirement.REQUIRED,
            ControllableGatewayListener.scripted(32144),
            supporting,
            profile
        );

        try {
            environment.start();
            assertThat(environment.isRunning()).isTrue();
        } finally {
            environment.close();
        }
    }

    @Test
    void shouldExposeProductionArmReachReleaseWorkflowWithJournaledZeroByteHold()
        throws Exception {
        List<InetSocketAddress> listenerAddresses = new ArrayList<>();
        AtomicReference<FrameServer> frameServer = new AtomicReference<>();
        Server server = correlationServer(frameServer);
        Client client = new Client((component, context) -> {
            Client typed = (Client) component;
            return ComponentRuntime.<ResolvedRoutes>runtime()
                .operations(new ResolvedRoutes(
                    context.resolve(typed.command),
                    context.resolve(typed.session)
                ))
                .build();
        });
        InteractionGateway gateway = new InteractionGateway(port -> {});
        EnvironmentBuilder builder = new EnvironmentBuilder()
            .components(client, server)
            .connect(client.command, server.command)
            .connect(client.session, server.session);
        ConnectionRouting routing = ConnectionRouting.routed(
            COMMAND,
            requiredLengthProfile(),
            gateway.tcp(
                commandAdapter("semantic-hold-route", listenerAddresses, new ArrayList<>()),
                new LengthPrefixedProtocolAdapter(),
                new ProtocolLimits(128, 256)
            )
        ).withRoute(
            SESSION,
            gateway.tcp(sessionAdapter(
                "session-route",
                listenerAddresses,
                new ArrayList<>()
            ))
        );
        RoutedEnvironment environment = routedEnvironment(builder, routing);
        String payload = "production-semantic-hold";
        String payloadSha256 = LengthPrefixedProtocolAdapter.sha256(
            payload.getBytes(UTF_8)
        );
        SemanticHold hold = environment.controls().arm(
            SemanticInteractionSelector.matching(
                ConnectionId.between(client.command, server.command),
                FlowDirection.CONSUMER_TO_PROVIDER,
                LengthPrefixedProtocolAdapter.CODEC,
                evidence -> evidence.payloadBytes() == payload.getBytes(UTF_8).length
                    && evidence.payloadSha256().equals(payloadSha256)
            ),
            Duration.ofSeconds(5)
        );

        try {
            environment.start();
            byte[] frame = LengthPrefixedProtocolAdapter.frame(payload);
            try (Socket socket = connect(listenerAddresses.getFirst())) {
                socket.getOutputStream().write(frame);
                socket.getOutputStream().flush();

                assertThat(hold.reached().toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .isNotNull();
                assertThat(hold.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
                frameServer.get().assertNoBytes();

                var release = hold.release();
                assertThat(release.toCompletableFuture().get(5, TimeUnit.SECONDS)).isNull();
                assertThat(frameServer.get().awaitPayload()).isEqualTo(payload.getBytes(UTF_8));
            }

            assertThat(environment.journalSnapshot().entries().stream()
                .map(entry -> entry.event())
                .filter(SemanticHoldEvent.class::isInstance)
                .map(SemanticHoldEvent.class::cast)
                .map(SemanticHoldEvent::state))
                .containsExactly(
                    SemanticHoldState.ARMED,
                    SemanticHoldState.REACHED_HELD,
                    SemanticHoldState.RELEASING,
                    SemanticHoldState.FORWARDED
                );
        } finally {
            environment.close();
        }
    }

    @Test
    void shouldForwardExactSuccessorBytesOnceAfterConfirmedPredecessor() throws Exception {
        SemanticGuardGateway fixture = semanticGuardGateway(2);
        String predecessorPayload = "confirmed-predecessor";
        String successorPayload = "positive-successor";
        CorrelationKey predecessorKey = LengthPrefixedProtocolAdapter.correlationKey(
            predecessorPayload
        );
        CorrelationKey successorKey = LengthPrefixedProtocolAdapter.correlationKey(
            successorPayload
        );
        ProofSubjectRef subject = fixture.environment.proofSubjects().create();
        fixture.environment.proofSubjects().arm(subject, predecessorKey);
        fixture.environment.proofSubjects().arm(subject, successorKey);
        SemanticPredecessorGuard guard = fixture.environment.controls().guard(
            SemanticPredecessorGuardSpec.requiring(
                subject,
                SemanticPredecessorRequirement.confirmed(frameSelector(
                    fixture.connectionId,
                    subject,
                    predecessorPayload
                )),
                frameSelector(fixture.connectionId, subject, successorPayload),
                Duration.ofSeconds(5)
            )
        );

        try {
            fixture.environment.start();
            try (Socket socket = connect(fixture.listenerAddresses.getFirst())) {
                byte[] predecessor = LengthPrefixedProtocolAdapter.frame(
                    predecessorPayload
                );
                byte[] successor = LengthPrefixedProtocolAdapter.frame(successorPayload);
                socket.getOutputStream().write(predecessor);
                socket.getOutputStream().flush();
                fixture.server.get().awaitFrames(1);
                socket.getOutputStream().write(successor);
                socket.getOutputStream().flush();

                assertThat(fixture.server.get().awaitFrames(2))
                    .containsExactly(predecessor, successor);
            }
            assertThat(guard.completion().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isEqualTo(SemanticPredecessorGuardState.SATISFIED);
        } finally {
            fixture.environment.close();
        }
    }

    @Test
    void shouldCloseSuccessorSessionWithoutForwardingAnyByteOnPredecessorViolation()
        throws Exception {
        SemanticGuardGateway fixture = semanticGuardGateway(1);
        String predecessorPayload = "missing-predecessor";
        String successorPayload = "early-positive-successor";
        CorrelationKey predecessorKey = LengthPrefixedProtocolAdapter.correlationKey(
            predecessorPayload
        );
        CorrelationKey successorKey = LengthPrefixedProtocolAdapter.correlationKey(
            successorPayload
        );
        ProofSubjectRef subject = fixture.environment.proofSubjects().create();
        fixture.environment.proofSubjects().arm(subject, predecessorKey);
        fixture.environment.proofSubjects().arm(subject, successorKey);
        SemanticPredecessorGuard guard = fixture.environment.controls().guard(
            SemanticPredecessorGuardSpec.requiring(
                subject,
                SemanticPredecessorRequirement.confirmed(frameSelector(
                    fixture.connectionId,
                    subject,
                    predecessorPayload
                )),
                frameSelector(fixture.connectionId, subject, successorPayload),
                Duration.ofSeconds(5)
            )
        );

        try {
            fixture.environment.start();
            try (Socket socket = connect(fixture.listenerAddresses.getFirst())) {
                socket.getOutputStream().write(
                    LengthPrefixedProtocolAdapter.frame(successorPayload)
                );
                socket.getOutputStream().flush();

                assertThat(guard.completion().toCompletableFuture().get(
                    5,
                    TimeUnit.SECONDS
                )).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
                assertPeerClosed(socket);
                fixture.server.get().assertClosedWithoutFrames();
            }
        } finally {
            fixture.environment.close();
        }
    }

    @Test
    void shouldNotTreatForwardedCommitAttemptAsCommitConfirmation() throws Exception {
        SemanticGuardGateway fixture = semanticGuardGateway(2);
        String commitAttemptPayload = "commit-attempt-forwarded";
        String commitConfirmationPayload = "commit-confirmation-absent";
        String httpSuccessorPayload = "positive-http-successor";
        ProofSubjectRef subject = fixture.environment.proofSubjects().create();
        fixture.environment.proofSubjects().arm(
            subject,
            LengthPrefixedProtocolAdapter.correlationKey(commitAttemptPayload)
        );
        fixture.environment.proofSubjects().arm(
            subject,
            LengthPrefixedProtocolAdapter.correlationKey(commitConfirmationPayload)
        );
        fixture.environment.proofSubjects().arm(
            subject,
            LengthPrefixedProtocolAdapter.correlationKey(httpSuccessorPayload)
        );
        SemanticPredecessorGuard guard = fixture.environment.controls().guard(
            SemanticPredecessorGuardSpec.requiring(
                subject,
                SemanticPredecessorRequirement.confirmed(frameSelector(
                    fixture.connectionId,
                    subject,
                    commitConfirmationPayload
                )),
                frameSelector(fixture.connectionId, subject, httpSuccessorPayload),
                Duration.ofSeconds(5)
            )
        );

        try {
            fixture.environment.start();
            try (Socket socket = connect(fixture.listenerAddresses.getFirst())) {
                byte[] commitAttempt = LengthPrefixedProtocolAdapter.frame(
                    commitAttemptPayload
                );
                socket.getOutputStream().write(commitAttempt);
                socket.getOutputStream().flush();
                assertThat(fixture.server.get().awaitFrames(1))
                    .containsExactly(commitAttempt);

                socket.getOutputStream().write(
                    LengthPrefixedProtocolAdapter.frame(httpSuccessorPayload)
                );
                socket.getOutputStream().flush();

                assertThat(guard.completion().toCompletableFuture().get(
                    5,
                    TimeUnit.SECONDS
                )).isEqualTo(SemanticPredecessorGuardState.VIOLATED);
                assertPeerClosed(socket);
                fixture.server.get().assertClosedWithFrames(commitAttempt);
            }
        } finally {
            fixture.environment.close();
        }
    }

    @Test
    void shouldKeepTwoSubjectBoundHoldsIsolatedThroughTheGateway() throws Exception {
        SubjectGatewayFixture fixture = subjectGateway(
            new InteractionGateway(port -> {}),
            LengthPrefixedProtocolAdapter.correlating()
        );
        String leftPayload = "gateway-left-subject";
        ProofSubjectRef leftSubject = fixture.environment().proofSubjects().create();
        ProofSubjectRef rightSubject = fixture.environment().proofSubjects().create();
        fixture.environment().proofSubjects().arm(
            leftSubject,
            LengthPrefixedProtocolAdapter.correlationKey(leftPayload)
        );
        fixture.environment().proofSubjects().arm(
            rightSubject,
            LengthPrefixedProtocolAdapter.correlationKey("gateway-right-subject")
        );
        SemanticHold left = fixture.environment().controls().arm(
            semanticSelector(fixture.connectionId()).forSubject(leftSubject),
            Duration.ofSeconds(5)
        );
        SemanticHold right = fixture.environment().controls().arm(
            semanticSelector(fixture.connectionId()).forSubject(rightSubject),
            Duration.ofSeconds(5)
        );

        try {
            fixture.environment().start();
            try (Socket socket = connect(fixture.listenerAddresses().getFirst())) {
                socket.getOutputStream().write(
                    LengthPrefixedProtocolAdapter.frame(leftPayload)
                );
                socket.getOutputStream().flush();

                assertThat(left.reached().toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .isNotNull();
                assertThat(left.state()).isEqualTo(SemanticHoldState.REACHED_HELD);
                assertThat(right.state()).isEqualTo(SemanticHoldState.ARMED);
                fixture.frameServer().get().assertNoBytes();

                assertThat(left.release().toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .isNull();
                assertThat(fixture.frameServer().get().awaitPayload())
                    .isEqualTo(leftPayload.getBytes(UTF_8));
                assertThat(right.state()).isEqualTo(SemanticHoldState.ARMED);
                assertThat(right.cancel()).isTrue();
            }
        } finally {
            fixture.environment().close();
        }
    }

    @Test
    void shouldNotReachSubjectBoundHoldForCrossCorrelatedGatewayInteraction()
        throws Exception {
        CorrelationKey leftKey = LengthPrefixedProtocolAdapter.correlationKey(
            "cross-left-key"
        );
        CorrelationKey rightKey = LengthPrefixedProtocolAdapter.correlationKey(
            "cross-right-key"
        );
        SubjectGatewayFixture fixture = subjectGateway(
            new InteractionGateway(port -> {}),
            LengthPrefixedProtocolAdapter.correlating(leftKey, rightKey)
        );
        ProofSubjectRef leftSubject = fixture.environment().proofSubjects().create();
        ProofSubjectRef rightSubject = fixture.environment().proofSubjects().create();
        fixture.environment().proofSubjects().arm(leftSubject, leftKey);
        fixture.environment().proofSubjects().arm(rightSubject, rightKey);
        SemanticHold left = fixture.environment().controls().arm(
            semanticSelector(fixture.connectionId()).forSubject(leftSubject),
            Duration.ofSeconds(5)
        );
        String payload = "one-interaction-two-subjects";

        try {
            fixture.environment().start();
            try (Socket socket = connect(fixture.listenerAddresses().getFirst())) {
                socket.getOutputStream().write(
                    LengthPrefixedProtocolAdapter.frame(payload)
                );
                socket.getOutputStream().flush();

                assertThat(fixture.frameServer().get().awaitPayload())
                    .isEqualTo(payload.getBytes(UTF_8));
                CorrelationResult<?> leftCorrelation = fixture.environment()
                    .proofSubjects()
                    .correlation(
                        leftSubject,
                        leftKey,
                        LengthPrefixedProtocolAdapter.NATIVE_REFERENCE_CODEC
                    );
                CorrelationResult<?> rightCorrelation = fixture.environment()
                    .proofSubjects()
                    .correlation(
                        rightSubject,
                        rightKey,
                        LengthPrefixedProtocolAdapter.NATIVE_REFERENCE_CODEC
                    );
                assertThat(leftCorrelation).isInstanceOf(CorrelationResult.Unique.class);
                assertThat(rightCorrelation).isInstanceOf(CorrelationResult.Unique.class);
                assertThat(((CorrelationResult.Unique<?>) leftCorrelation).interactionRef())
                    .isEqualTo(
                        ((CorrelationResult.Unique<?>) rightCorrelation).interactionRef()
                    );
                assertThat(left.state()).isEqualTo(SemanticHoldState.ARMED);
                assertThat(left.cancel()).isTrue();
            }
        } finally {
            fixture.environment().close();
        }
    }

    @Test
    void shouldCloseHeldSessionWithoutForwardingWhenCorrelationInvalidatesBeforeRelease()
        throws Exception {
        CorrelationKey key = LengthPrefixedProtocolAdapter.correlationKey(
            "release-invalidation-key"
        );
        SubjectGatewayFixture fixture = subjectGateway(
            new InteractionGateway(port -> {}),
            LengthPrefixedProtocolAdapter.correlating(key)
        );
        String heldPayload = "held-native-flow-secret";
        String invalidatingPayload = "distinct-native-flow-secret";
        ProofSubjectRef subject = fixture.environment().proofSubjects().create();
        fixture.environment().proofSubjects().arm(subject, key);
        SemanticHold hold = fixture.environment().controls().arm(
            SemanticInteractionSelector.matching(
                fixture.connectionId(),
                FlowDirection.CONSUMER_TO_PROVIDER,
                LengthPrefixedProtocolAdapter.CODEC,
                evidence -> evidence.payloadSha256().equals(
                    LengthPrefixedProtocolAdapter.sha256(heldPayload.getBytes(UTF_8))
                )
            ).forSubject(subject).through(
                key,
                LengthPrefixedProtocolAdapter.NATIVE_REFERENCE_CODEC,
                evidence -> new LengthPrefixedProtocolAdapter.FrameNativeReference(
                    evidence.direction(),
                    evidence.payloadBytes(),
                    evidence.payloadSha256()
                )
            ),
            Duration.ofSeconds(5)
        );

        try {
            fixture.environment().start();
            try (Socket held = connect(fixture.listenerAddresses().getFirst())) {
                held.getOutputStream().write(
                    LengthPrefixedProtocolAdapter.frame(heldPayload)
                );
                held.getOutputStream().flush();
                hold.reached().toCompletableFuture().get(5, TimeUnit.SECONDS);
                fixture.frameServer().get().assertNoBytes();

                try (Socket invalidating = connect(
                    fixture.listenerAddresses().getFirst()
                )) {
                    invalidating.getOutputStream().write(
                        LengthPrefixedProtocolAdapter.frame(invalidatingPayload)
                    );
                    invalidating.getOutputStream().flush();
                    awaitAmbiguousCorrelation(fixture.environment(), subject, key);

                    var release = hold.release();

                    assertThat(hold.completion().toCompletableFuture().get(
                        5,
                        TimeUnit.SECONDS
                    )).isEqualTo(SemanticHoldState.FAILED);
                    assertCompletedExceptionally(release.toCompletableFuture());
                    assertPeerClosed(held);
                    fixture.frameServer().get().assertClosedWithoutPayload();
                    assertThat(fixture.environment().journalSnapshot().entries().stream()
                        .map(entry -> entry.event())
                        .filter(SemanticHoldEvent.class::isInstance)
                        .map(SemanticHoldEvent.class::cast)
                        .filter(event -> event.state() == SemanticHoldState.FAILED))
                        .singleElement()
                        .satisfies(event -> assertThat(event.failure())
                            .contains(SemanticHoldFailure.CORRELATION_INVALIDATED));
                    assertThat(new io.github.jacekkardys.systemproof.diagnostics.JournalRenderer()
                        .render(fixture.environment().journalSnapshot()))
                        .doesNotContain(heldPayload, invalidatingPayload);
                }
            }
        } finally {
            fixture.environment().close();
        }
    }

    @Test
    void shouldFailReleasedHoldOnFullGatewayWriteFailureWithoutRetry()
        throws Exception {
        assertFullGatewayForwardingFailure(ForwardingFailurePoint.WRITE);
    }

    @Test
    void shouldFailReleasedHoldOnFullGatewayFlushFailureWithoutRetry()
        throws Exception {
        assertFullGatewayForwardingFailure(ForwardingFailurePoint.FLUSH);
    }

    @Test
    void shouldCancelReachedHoldOnEnvironmentCloseWithoutForwardingOrRouteDegradation()
        throws Exception {
        List<InetSocketAddress> listenerAddresses = new ArrayList<>();
        AtomicReference<FrameServer> frameServer = new AtomicReference<>();
        Server server = correlationServer(frameServer);
        Client client = new Client((component, context) -> {
            Client typed = (Client) component;
            return ComponentRuntime.<ResolvedRoutes>runtime()
                .operations(new ResolvedRoutes(
                    context.resolve(typed.command),
                    context.resolve(typed.session)
                ))
                .build();
        });
        InteractionGateway gateway = new InteractionGateway(port -> {});
        EnvironmentBuilder builder = new EnvironmentBuilder()
            .components(client, server)
            .connect(client.command, server.command)
            .connect(client.session, server.session);
        ConnectionRouting routing = ConnectionRouting.routed(
            COMMAND,
            requiredLengthProfile(),
            gateway.tcp(
                commandAdapter("teardown-hold-route", listenerAddresses, new ArrayList<>()),
                new LengthPrefixedProtocolAdapter(),
                new ProtocolLimits(128, 256)
            )
        ).withRoute(
            SESSION,
            gateway.tcp(sessionAdapter(
                "session-route",
                listenerAddresses,
                new ArrayList<>()
            ))
        );
        RoutedEnvironment environment = routedEnvironment(builder, routing);
        String payload = "teardown-held-payload";
        String payloadSha256 = LengthPrefixedProtocolAdapter.sha256(
            payload.getBytes(UTF_8)
        );
        SemanticHold hold = environment.controls().arm(
            SemanticInteractionSelector.matching(
                ConnectionId.between(client.command, server.command),
                FlowDirection.CONSUMER_TO_PROVIDER,
                LengthPrefixedProtocolAdapter.CODEC,
                evidence -> evidence.payloadSha256().equals(payloadSha256)
            ),
            Duration.ofSeconds(30)
        );

        environment.start();
        try (Socket socket = connect(listenerAddresses.getFirst())) {
            socket.getOutputStream().write(LengthPrefixedProtocolAdapter.frame(payload));
            socket.getOutputStream().flush();
            hold.reached().toCompletableFuture().get(5, TimeUnit.SECONDS);
            frameServer.get().assertNoBytes();

            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                var close = executor.submit(environment::close);
                close.get(2, TimeUnit.SECONDS);
            }

            assertThat(hold.state()).isEqualTo(SemanticHoldState.CANCELLED);
            assertCompletedExceptionally(hold.release().toCompletableFuture());
            assertThat(frameServer.get().hasPayload()).isFalse();
            assertThat(environment.runtimeConnections())
                .filteredOn(snapshot -> snapshot.observationRequirement()
                    == ObservationRequirement.REQUIRED)
                .singleElement()
                .satisfies(snapshot -> {
                    assertThat(snapshot.state()).isEqualTo(ConnectionState.STOPPED);
                    assertThat(snapshot.effectiveObservationStatus())
                        .isEqualTo(EffectiveObservationStatus.INACTIVE);
                });
        } finally {
            environment.close();
        }
    }

    @Test
    void shouldForwardOrdinaryFrameEmittedByComponentCleanup() throws Exception {
        List<InetSocketAddress> listenerAddresses = new ArrayList<>();
        AtomicReference<FrameServer> frameServer = new AtomicReference<>();
        AtomicInteger cleanupFrames = new AtomicInteger();
        byte[] cleanupFrame = LengthPrefixedProtocolAdapter.frame("cleanup-logout");
        Server server = correlationServer(frameServer);
        Client client = new Client((component, context) -> {
            Client typed = (Client) component;
            CommandEndpoint command = context.resolve(typed.command);
            SessionEndpoint session = context.resolve(typed.session);
            return ComponentRuntime.<ResolvedRoutes>runtime(() -> {
                try (Socket socket = connect(listenerAddresses.getFirst())) {
                    socket.getOutputStream().write(cleanupFrame);
                    socket.getOutputStream().flush();
                    assertThat(frameServer.get().awaitPayload())
                        .isEqualTo("cleanup-logout".getBytes(UTF_8));
                    cleanupFrames.incrementAndGet();
                }
            })
                .operations(new ResolvedRoutes(command, session))
                .build();
        });
        InteractionGateway gateway = new InteractionGateway(port -> {});
        EnvironmentBuilder builder = new EnvironmentBuilder()
            .components(client, server)
            .connect(client.command, server.command)
            .connect(client.session, server.session);
        ConnectionRouting routing = ConnectionRouting.routed(
            COMMAND,
            requiredLengthProfile(),
            gateway.tcp(
                commandAdapter("cleanup-route", listenerAddresses, new ArrayList<>()),
                new LengthPrefixedProtocolAdapter(),
                new ProtocolLimits(128, 256)
            )
        ).withRoute(
            SESSION,
            gateway.tcp(sessionAdapter(
                "cleanup-session-route",
                listenerAddresses,
                new ArrayList<>()
            ))
        );
        RoutedEnvironment environment = routedEnvironment(builder, routing);

        environment.start();
        environment.close();

        assertThat(cleanupFrames).hasValue(1);
        assertThat(environment.journalSnapshot().entries().stream()
            .map(entry -> entry.event())
            .filter(SemanticHoldEvent.class::isInstance))
            .isEmpty();
    }

    @Test
    void shouldTimeOutReachedHoldWithoutForwardingOrDegradingTheRoute()
        throws Exception {
        List<InetSocketAddress> listenerAddresses = new ArrayList<>();
        AtomicReference<FrameServer> frameServer = new AtomicReference<>();
        Server server = correlationServer(frameServer);
        Client client = new Client((component, context) -> {
            Client typed = (Client) component;
            return ComponentRuntime.<ResolvedRoutes>runtime()
                .operations(new ResolvedRoutes(
                    context.resolve(typed.command),
                    context.resolve(typed.session)
                ))
                .build();
        });
        InteractionGateway gateway = new InteractionGateway(port -> {});
        EnvironmentBuilder builder = new EnvironmentBuilder()
            .components(client, server)
            .connect(client.command, server.command)
            .connect(client.session, server.session);
        ConnectionRouting routing = ConnectionRouting.routed(
            COMMAND,
            requiredLengthProfile(),
            gateway.tcp(
                commandAdapter("timeout-hold-route", listenerAddresses, new ArrayList<>()),
                new LengthPrefixedProtocolAdapter(),
                new ProtocolLimits(128, 256)
            )
        ).withRoute(
            SESSION,
            gateway.tcp(sessionAdapter(
                "session-route",
                listenerAddresses,
                new ArrayList<>()
            ))
        );
        RoutedEnvironment environment = routedEnvironment(builder, routing);
        String payload = "timeout-held-payload";
        String payloadSha256 = LengthPrefixedProtocolAdapter.sha256(
            payload.getBytes(UTF_8)
        );
        SemanticHold hold = environment.controls().arm(
            SemanticInteractionSelector.matching(
                ConnectionId.between(client.command, server.command),
                FlowDirection.CONSUMER_TO_PROVIDER,
                LengthPrefixedProtocolAdapter.CODEC,
                evidence -> evidence.payloadSha256().equals(payloadSha256)
            ),
            Duration.ofSeconds(1)
        );

        try {
            environment.start();
            try (Socket socket = connect(listenerAddresses.getFirst())) {
                socket.getOutputStream().write(
                    LengthPrefixedProtocolAdapter.frame(payload)
                );
                socket.getOutputStream().flush();
                hold.reached().toCompletableFuture().get(5, TimeUnit.SECONDS);
                frameServer.get().assertNoBytes();

                assertThat(hold.completion().toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .isEqualTo(SemanticHoldState.TIMED_OUT);
                assertPeerClosed(socket);
                assertCompletedExceptionally(hold.release().toCompletableFuture());
                assertThat(frameServer.get().hasPayload()).isFalse();
                assertThat(environment.runtimeConnections())
                    .filteredOn(snapshot -> snapshot.observationRequirement()
                        == ObservationRequirement.REQUIRED)
                    .singleElement()
                    .satisfies(snapshot -> {
                        assertThat(snapshot.state()).isEqualTo(ConnectionState.RUNNING);
                        assertThat(snapshot.effectiveObservationStatus())
                            .isEqualTo(EffectiveObservationStatus.ACTIVE);
                    });
            }
        } finally {
            environment.close();
        }
    }

    @Test
    void shouldExposeActiveRequiredAndUnsupportedOptionalObservationStatuses()
        throws Exception {
        List<InetSocketAddress> listenerAddresses = new ArrayList<>();
        Server server = server(new ArrayList<>(), new AtomicInteger());
        Client client = new Client((component, context) -> {
            Client typed = (Client) component;
            return ComponentRuntime.<ResolvedRoutes>runtime()
                .operations(new ResolvedRoutes(
                    context.resolve(typed.command),
                    context.resolve(typed.session)
                ))
                .build();
        });
        InteractionGateway gateway = new InteractionGateway(port -> {});
        EnvironmentBuilder builder = new EnvironmentBuilder()
            .components(client, server)
            .connect(client.command, server.command)
            .connect(client.session, server.session);
        ConnectionRouting routing = ConnectionRouting.routed(
            COMMAND,
            requiredLengthProfile(),
            gateway.tcp(
                commandAdapter(
                    "required-route",
                    listenerAddresses,
                    new ArrayList<>()
                ),
                new LengthPrefixedProtocolAdapter(),
                new ProtocolLimits(128, 256)
            )
        ).withRoute(
            SESSION,
            ObservationRequirement.OPTIONAL,
            gateway.tcp(sessionAdapter(
                "optional-route",
                listenerAddresses,
                new ArrayList<>()
            ))
        );
        RoutedEnvironment environment = routedEnvironment(builder, routing);

        try {
            environment.start();

            assertThat(environment.runtimeConnections())
                .extracting(
                    snapshot -> snapshot.observationRequirement(),
                    snapshot -> snapshot.effectiveObservationStatus()
                )
                .containsExactly(
                    tuple(
                        ObservationRequirement.REQUIRED,
                        EffectiveObservationStatus.ACTIVE
                    ),
                    tuple(
                        ObservationRequirement.OPTIONAL,
                        EffectiveObservationStatus.UNSUPPORTED
                    )
                );

            try (Socket socket = connect(listenerAddresses.getFirst())) {
                socket.getOutputStream().write(
                    LengthPrefixedProtocolAdapter.control(
                        LengthPrefixedProtocolAdapter.UNSUPPORTED_ENCRYPTION
                    )
                );
                socket.getOutputStream().flush();
                assertPeerClosed(socket);
            }
            assertThat(environment.runtimeConnections())
                .extracting(snapshot -> snapshot.effectiveObservationStatus())
                .containsExactly(
                    EffectiveObservationStatus.FAILED,
                    EffectiveObservationStatus.UNSUPPORTED
                );
        } finally {
            environment.close();
        }
        listenerAddresses.forEach(InteractionGatewayTest::assertPortCanBeRebound);
    }

    @Test
    void shouldExposeRequiredListenerFailureAndRedactItDuringEnvironmentCleanup()
        throws Exception {
        ControllableGatewayListener listener = ControllableGatewayListener.scripted(32140);
        IOException listenerFailure = new IOException(
            "listener-secret at 127.0.0.1:32140"
        );
        IOException cleanupFailure = new IOException(
            "cleanup-secret at 127.0.0.1:42140"
        );
        IOException socketCleanupFailure = new IOException(
            "socket-cleanup-secret at 127.0.0.1:52140"
        );
        ControlledCloseSocket socket = ControlledCloseSocket.failingWith(
            socketCleanupFailure
        );
        listener.accept(socket);
        listener.failOnClose(cleanupFailure);
        RoutedEnvironment environment = observedEnvironment(
            ObservationRequirement.REQUIRED,
            listener
        );

        environment.start();
        assertThat(observationStatus(environment, ObservationRequirement.REQUIRED))
            .isEqualTo(EffectiveObservationStatus.ACTIVE);
        socket.awaitSetupEntered();
        listener.awaitAcceptCalls(2);
        listener.fail(listenerFailure);
        awaitObservationStatus(
            environment,
            ObservationRequirement.REQUIRED,
            EffectiveObservationStatus.FAILED
        );
        ConnectionId failedConnection = environment.runtimeConnections().stream()
            .filter(snapshot -> snapshot.observationRequirement()
                == ObservationRequirement.REQUIRED)
            .findFirst()
            .orElseThrow()
            .id();
        assertThatThrownBy(() -> environment.controls().arm(
            semanticSelector(failedConnection),
            Duration.ofSeconds(5)
        )).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("does not currently have active semantic-control capability");
        socket.releaseSetup();
        socket.awaitCloseEntered();

        Throwable thrown = catchThrowable(environment::close);

        assertThat(thrown)
            .isInstanceOf(IllegalStateException.class)
            .hasCause(listenerFailure);
        assertThat(listenerFailure.getSuppressed())
            .containsExactly(cleanupFailure, socketCleanupFailure);
        assertThat(socket.closeCalls()).isEqualTo(1);
        assertThat(observationStatus(environment, ObservationRequirement.REQUIRED))
            .isEqualTo(EffectiveObservationStatus.FAILED);
        assertThat(environment.runtimeConnections())
            .filteredOn(snapshot -> snapshot.observationRequirement()
                == ObservationRequirement.REQUIRED)
            .singleElement()
            .satisfies(snapshot -> assertThat(snapshot.state())
                .isEqualTo(ConnectionState.FAILED));

        FailureEvent.ConnectionCleanup journalFailure = environment.journalSnapshot()
            .entries()
            .stream()
            .map(entry -> entry.event())
            .filter(FailureEvent.ConnectionCleanup.class::isInstance)
            .map(FailureEvent.ConnectionCleanup.class::cast)
            .findFirst()
            .orElseThrow();
        assertThat(journalFailure.failure().failureType()).isEqualTo("IOException");
        assertThat(journalFailure.toString()).doesNotContain(
            "listener-secret",
            "cleanup-secret",
            "socket-cleanup-secret",
            "127.0.0.1",
            "32140",
            "42140",
            "52140"
        );
        assertThat(environment.diagnostics().content())
            .contains("Connection cleanup failed: IOException")
            .doesNotContain(
                "listener-secret",
                "cleanup-secret",
                "socket-cleanup-secret",
                "127.0.0.1",
                "32140",
                "42140",
                "52140"
            );
        environment.close();
        assertThat(listener.closeCalls()).isEqualTo(1);
    }

    @Test
    void shouldPreserveOptionalDegradationAfterEnvironmentShutdown() throws Exception {
        ControllableGatewayListener listener = ControllableGatewayListener.scripted(32141);
        IOException listenerFailure = new IOException("optional listener failed");
        RoutedEnvironment environment = observedEnvironment(
            ObservationRequirement.OPTIONAL,
            listener
        );

        environment.start();
        assertThat(observationStatus(environment, ObservationRequirement.OPTIONAL))
            .isEqualTo(EffectiveObservationStatus.ACTIVE);
        listener.awaitAcceptCalls(1);
        listener.fail(listenerFailure);
        awaitObservationStatus(
            environment,
            ObservationRequirement.OPTIONAL,
            EffectiveObservationStatus.DEGRADED
        );

        Throwable thrown = catchThrowable(environment::close);

        assertThat(thrown)
            .isInstanceOf(IllegalStateException.class)
            .hasCause(listenerFailure);
        assertThat(observationStatus(environment, ObservationRequirement.OPTIONAL))
            .isEqualTo(EffectiveObservationStatus.DEGRADED);
        assertThat(environment.runtimeConnections())
            .filteredOn(snapshot -> snapshot.observationRequirement()
                == ObservationRequirement.OPTIONAL)
            .singleElement()
            .satisfies(snapshot -> assertThat(snapshot.state())
                .isEqualTo(ConnectionState.FAILED));
        environment.close();
        assertThat(listener.closeCalls()).isEqualTo(1);
    }

    @Test
    void shouldResolveEnvironmentProofSubjectThroughTheProductionGatewayPath()
        throws Exception {
        List<InetSocketAddress> listenerAddresses = new ArrayList<>();
        AtomicReference<FrameServer> frameServer = new AtomicReference<>();
        Server server = correlationServer(frameServer);
        Client client = new Client((component, context) -> {
            Client typed = (Client) component;
            return ComponentRuntime.<ResolvedRoutes>runtime()
                .operations(new ResolvedRoutes(
                    context.resolve(typed.command),
                    context.resolve(typed.session)
                ))
                .build();
        });
        InteractionGateway gateway = new InteractionGateway(port -> {});
        EnvironmentBuilder builder = new EnvironmentBuilder()
            .components(client, server)
            .connect(client.command, server.command)
            .connect(client.session, server.session);
        ConnectionRouting routing = ConnectionRouting.routed(
            COMMAND,
            requiredLengthProfile(),
            gateway.tcp(
                commandAdapter(
                    "correlation-route",
                    listenerAddresses,
                    new ArrayList<>()
                ),
                LengthPrefixedProtocolAdapter.correlating(),
                new ProtocolLimits(128, 256)
            )
        ).withRoute(
            SESSION,
            gateway.tcp(sessionAdapter(
                "session-route",
                listenerAddresses,
                new ArrayList<>()
            ))
        );
        RoutedEnvironment environment = routedEnvironment(builder, routing);
        String payload = "production-registry-correlation";
        CorrelationKey key = LengthPrefixedProtocolAdapter.correlationKey(payload);
        ProofSubjectRef subject = environment.proofSubjects().create();
        environment.proofSubjects().arm(subject, key);

        try {
            environment.start();
            byte[] frame = LengthPrefixedProtocolAdapter.frame(payload);
            try (Socket socket = connect(listenerAddresses.getFirst())) {
                socket.getOutputStream().write(frame);
                socket.getOutputStream().flush();
                assertThat(frameServer.get().awaitPayload())
                    .isEqualTo(payload.getBytes(UTF_8));
            }

            CorrelationResult<
                LengthPrefixedProtocolAdapter.FrameNativeReference
            > result = environment.proofSubjects().correlation(
                subject,
                key,
                LengthPrefixedProtocolAdapter.NATIVE_REFERENCE_CODEC
            );
            assertThat(result).isInstanceOf(CorrelationResult.Unique.class);
            CorrelationResult.Unique<
                LengthPrefixedProtocolAdapter.FrameNativeReference
            > unique = (CorrelationResult.Unique<
                LengthPrefixedProtocolAdapter.FrameNativeReference
            >) result;
            assertThat(unique.nativeReference().direction())
                .isEqualTo(FlowDirection.CONSUMER_TO_PROVIDER);
            assertThat(unique.nativeReference().payloadBytes())
                .isEqualTo(payload.getBytes(UTF_8).length);
            assertThat(unique.nativeReference().payloadSha256())
                .isEqualTo(LengthPrefixedProtocolAdapter.sha256(
                    payload.getBytes(UTF_8)
                ));

            List<ScenarioEvent> events = environment.journalSnapshot().entries().stream()
                .map(entry -> entry.event())
                .toList();
            InteractionObservationEvent observation = events.stream()
                .filter(InteractionObservationEvent.class::isInstance)
                .map(InteractionObservationEvent.class::cast)
                .findFirst()
                .orElseThrow();
            CorrelationCandidateEvent candidate = events.stream()
                .filter(CorrelationCandidateEvent.class::isInstance)
                .map(CorrelationCandidateEvent.class::cast)
                .findFirst()
                .orElseThrow();
            assertThat(candidate.proofSubject()).contains(subject);
            assertThat(candidate.interactionRef()).isEqualTo(observation.interactionRef());
            assertThat(unique.interactionRef()).isEqualTo(observation.interactionRef());
            assertThat(events.indexOf(candidate)).isGreaterThan(events.indexOf(observation));
        } finally {
            environment.close();
        }
        listenerAddresses.forEach(InteractionGatewayTest::assertPortCanBeRebound);
    }

    @Test
    void shouldKeepDistinctTypedRoutesAndLongLivedSessionsConnectionOwned() throws Exception {
        List<String> lifecycle = new ArrayList<>();
        List<InetSocketAddress> listenerAddresses = new ArrayList<>();
        AtomicInteger providerCloses = new AtomicInteger();
        Server server = server(lifecycle, providerCloses);
        Client client = new Client((component, context) -> {
            Client typed = (Client) component;
            ResolvedRoutes routes = new ResolvedRoutes(
                context.resolve(typed.command),
                context.resolve(typed.session)
            );
            assertThat(exchangeUnchecked(listenerAddresses.get(0), "startup-check"))
                .isEqualTo("command-provider:startup-check");
            assertThat(exchangeUnchecked(listenerAddresses.get(1), "startup-check"))
                .isEqualTo("session-provider:startup-check");
            lifecycle.add("consumer-start");
            return ComponentRuntime.<ResolvedRoutes>runtime()
                .operations(routes)
                .build();
        });
        InteractionGateway gateway = new InteractionGateway(
            port -> lifecycle.add("expose:" + port)
        );
        TcpEndpointAdapter<CommandEndpoint> commands =
            commandAdapter("command-route", listenerAddresses, lifecycle);
        TcpEndpointAdapter<SessionEndpoint> sessions =
            sessionAdapter("session-route", listenerAddresses, lifecycle);
        RoutedEnvironment environment = environment(
            server,
            client,
            gateway,
            commands,
            sessions
        );
        Socket longLivedSession = null;

        try {
            environment.start();

            assertThat(lifecycle)
                .hasSize(6)
                .startsWith("provider-start")
                .endsWith("consumer-start");
            assertThat(lifecycle.get(1)).startsWith("expose:");
            assertThat(lifecycle.get(2)).startsWith("command-route:");
            assertThat(lifecycle.get(3)).startsWith("expose:");
            assertThat(lifecycle.get(4)).startsWith("session-route:");
            assertThat(listenerAddresses).hasSize(2).doesNotHaveDuplicates();
            assertThat(environment.runtimeConnections())
                .hasSize(2)
                .allSatisfy(snapshot -> {
                    assertThat(snapshot.state()).isEqualTo(ConnectionState.RUNNING);
                    assertThat(snapshot.routingMode()).isEqualTo(RoutingMode.ROUTED);
                    assertThat(snapshot.observationRequirement())
                        .isEqualTo(ObservationRequirement.DISABLED);
                    assertThat(snapshot.effectiveObservationStatus())
                        .isEqualTo(EffectiveObservationStatus.DISABLED);
                });

            ResolvedRoutes resolved = environment.routes(client);
            assertThat(resolved.command().host())
                .isEqualTo(InteractionGateway.CONTAINER_HOST);
            assertThat(resolved.session().host())
                .isEqualTo(InteractionGateway.CONTAINER_HOST);
            assertThat(resolved.command().port())
                .isNotEqualTo(resolved.session().port());
            assertThat(environment.diagnostics().content())
                .doesNotContain(
                    "session-secret",
                    InteractionGateway.CONTAINER_HOST,
                    InteractionGateway.TEST_HOST
                );
            listenerAddresses.forEach(address ->
                assertThat(environment.diagnostics().content())
                    .doesNotContain(Integer.toString(address.getPort()))
            );

            assertThat(exchange(listenerAddresses.get(0), "request"))
                .isEqualTo("command-provider:request");
            longLivedSession = connect(listenerAddresses.get(1));
            assertThat(exchange(longLivedSession, "bind")).isEqualTo("session-provider:bind");
            assertThat(exchange(longLivedSession, "submit-1"))
                .isEqualTo("session-provider:submit-1");
            assertThat(exchange(longLivedSession, "submit-2"))
                .isEqualTo("session-provider:submit-2");

            environment.close();

            assertPeerClosed(longLivedSession);
            assertThat(providerCloses).hasValue(1);
            assertThat(environment.runtimeConnections())
                .allSatisfy(snapshot ->
                    assertThat(snapshot.state()).isEqualTo(ConnectionState.STOPPED)
                );
            listenerAddresses.forEach(InteractionGatewayTest::assertPortCanBeRebound);
        } finally {
            if (longLivedSession != null) {
                longLivedSession.close();
            }
            environment.close();
        }
    }

    @Test
    void shouldReleaseBothRoutesAfterConsumerStartupFails() {
        List<InetSocketAddress> listenerAddresses = new ArrayList<>();
        AtomicInteger providerCloses = new AtomicInteger();
        IllegalStateException startupFailure =
            new IllegalStateException("Injected consumer startup failure");
        Server server = server(new ArrayList<>(), providerCloses);
        Client client = new Client((component, context) -> {
            Client typed = (Client) component;
            context.resolve(typed.command);
            context.resolve(typed.session);
            throw startupFailure;
        });
        InteractionGateway gateway = new InteractionGateway(port -> {});
        RoutedEnvironment environment = environment(
            server,
            client,
            gateway,
            commandAdapter("command-route", listenerAddresses, new ArrayList<>()),
            sessionAdapter("session-route", listenerAddresses, new ArrayList<>())
        );

        EnvironmentStartException thrown = catchThrowableOfType(
            environment::start,
            EnvironmentStartException.class
        );

        assertThat(thrown.getCause()).isSameAs(startupFailure);
        assertThat(listenerAddresses).hasSize(2);
        assertThat(providerCloses).hasValue(1);
        assertThat(environment.runtimeConnections())
            .allSatisfy(snapshot -> {
                assertThat(snapshot.state()).isEqualTo(ConnectionState.STOPPED);
                assertThat(snapshot.directTargetAvailable()).isFalse();
                assertThat(snapshot.consumerTargetAvailable()).isFalse();
            });
        listenerAddresses.forEach(InteractionGatewayTest::assertPortCanBeRebound);
    }

    @Test
    void shouldFailFastAndReleaseTheListenerWhenHostRoutingIsUnsupported() {
        AtomicInteger exposedPort = new AtomicInteger();
        AtomicInteger providerCloses = new AtomicInteger();
        AtomicInteger consumerStarts = new AtomicInteger();
        String diagnosticSecret = "unsupported-host-routing-secret";
        IllegalStateException exposureFailure =
            new IllegalStateException("Host forwarding unavailable: " + diagnosticSecret);
        Server server = server(new ArrayList<>(), providerCloses);
        Client client = new Client((component, context) -> {
            consumerStarts.incrementAndGet();
            throw new AssertionError("Consumer must not start");
        });
        InteractionGateway gateway = new InteractionGateway(port -> {
            exposedPort.set(port);
            throw exposureFailure;
        });
        RoutedEnvironment environment = environment(
            server,
            client,
            gateway,
            commandAdapter("command-route", new ArrayList<>(), new ArrayList<>()),
            sessionAdapter("session-route", new ArrayList<>(), new ArrayList<>())
        );

        EnvironmentStartException thrown = catchThrowableOfType(
            environment::start,
            EnvironmentStartException.class
        );

        assertThat(thrown.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasCause(exposureFailure)
            .hasMessageContaining(
                "InteractionGateway could not expose its listener",
                environment.connections().getFirst().id().toString(),
                "Testcontainers host routing",
                "Docker Desktop",
                "host.testcontainers.internal"
            );
        assertThat(exposedPort).hasPositiveValue();
        assertThat(consumerStarts).hasValue(0);
        assertThat(providerCloses).hasValue(1);
        assertThat(thrown.diagnostics().content())
            .contains("Connection materialization failed: IllegalStateException")
            .doesNotContain(
                diagnosticSecret,
                "session-secret",
                Integer.toString(exposedPort.get())
            );
        assertPortCanBeRebound(exposedPort.get());
    }

    @Test
    void shouldReleaseTheEarlierRouteWhenLaterHostExposureFails() {
        List<Integer> exposedPorts = new ArrayList<>();
        AtomicInteger providerCloses = new AtomicInteger();
        AtomicInteger consumerStarts = new AtomicInteger();
        IllegalStateException exposureFailure =
            new IllegalStateException("Second host forwarding unavailable");
        Server server = server(new ArrayList<>(), providerCloses);
        Client client = new Client((component, context) -> {
            consumerStarts.incrementAndGet();
            throw new AssertionError("Consumer must not start");
        });
        InteractionGateway gateway = new InteractionGateway(port -> {
            exposedPorts.add(port);
            if (exposedPorts.size() == 2) {
                throw exposureFailure;
            }
        });
        RoutedEnvironment environment = environment(
            server,
            client,
            gateway,
            commandAdapter("command-route", new ArrayList<>(), new ArrayList<>()),
            sessionAdapter("session-route", new ArrayList<>(), new ArrayList<>())
        );

        EnvironmentStartException thrown = catchThrowableOfType(
            environment::start,
            EnvironmentStartException.class
        );

        assertThat(thrown.getCause())
            .isInstanceOf(IllegalStateException.class)
            .hasCause(exposureFailure)
            .hasMessageContaining(environment.connections().get(1).id().toString());
        assertThat(exposedPorts).hasSize(2).doesNotHaveDuplicates();
        assertThat(consumerStarts).hasValue(0);
        assertThat(providerCloses).hasValue(1);
        exposedPorts.forEach(InteractionGatewayTest::assertPortCanBeRebound);
    }

    @Test
    void shouldRejectAnEndpointAdapterThatBypassesTheGatewayListener() {
        AtomicInteger exposedPort = new AtomicInteger();
        AtomicInteger providerCloses = new AtomicInteger();
        AtomicInteger consumerStarts = new AtomicInteger();
        Server server = server(new ArrayList<>(), providerCloses);
        Client client = new Client((component, context) -> {
            consumerStarts.incrementAndGet();
            throw new AssertionError("Consumer must not start");
        });
        InteractionGateway gateway = new InteractionGateway(exposedPort::set);
        TcpEndpointAdapter<CommandEndpoint> bypassingAdapter = endpoint(
            value -> new InetSocketAddress(value.host(), value.port()),
            (value, host, port) -> value
        );
        RoutedEnvironment environment = environment(
            server,
            client,
            gateway,
            bypassingAdapter,
            sessionAdapter("session-route", new ArrayList<>(), new ArrayList<>())
        );

        EnvironmentStartException thrown = catchThrowableOfType(
            environment::start,
            EnvironmentStartException.class
        );

        assertThat(thrown.getCause())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Replacement endpoint must use the requested TCP address");
        assertThat(exposedPort).hasPositiveValue();
        assertThat(consumerStarts).hasValue(0);
        assertThat(providerCloses).hasValue(1);
        assertPortCanBeRebound(exposedPort.get());
    }

    private static RoutedEnvironment environment(
        Server server,
        Client client,
        InteractionGateway gateway,
        TcpEndpointAdapter<CommandEndpoint> commands,
        TcpEndpointAdapter<SessionEndpoint> sessions
    ) {
        EnvironmentBuilder builder = new EnvironmentBuilder()
            .components(client, server)
            .connect(client.command, server.command)
            .connect(client.session, server.session);
        ConnectionRouting routing = ConnectionRouting.routed(
            COMMAND,
            gateway.tcp(commands)
        ).withRoute(
            SESSION,
            gateway.tcp(sessions)
        );
        return routedEnvironment(builder, routing);
    }

    private static SubjectGatewayFixture subjectGateway(
        InteractionGateway gateway,
        LengthPrefixedProtocolAdapter protocolAdapter
    ) {
        List<InetSocketAddress> listenerAddresses = new ArrayList<>();
        AtomicReference<FrameServer> frameServer = new AtomicReference<>();
        Server server = correlationServer(frameServer);
        Client client = new Client((component, context) -> {
            Client typed = (Client) component;
            return ComponentRuntime.<ResolvedRoutes>runtime()
                .operations(new ResolvedRoutes(
                    context.resolve(typed.command),
                    context.resolve(typed.session)
                ))
                .build();
        });
        EnvironmentBuilder builder = new EnvironmentBuilder()
            .components(client, server)
            .connect(client.command, server.command)
            .connect(client.session, server.session);
        ConnectionRouting routing = ConnectionRouting.routed(
            COMMAND,
            requiredLengthProfile(),
            gateway.tcp(
                commandAdapter(
                    "subject-hold-route",
                    listenerAddresses,
                    new ArrayList<>()
                ),
                protocolAdapter,
                new ProtocolLimits(128, 256)
            )
        ).withRoute(
            SESSION,
            gateway.tcp(sessionAdapter(
                "subject-session-route",
                listenerAddresses,
                new ArrayList<>()
            ))
        );
        return new SubjectGatewayFixture(
            routedEnvironment(builder, routing),
            listenerAddresses,
            frameServer,
            ConnectionId.between(client.command, server.command)
        );
    }

    private static SemanticGuardGateway semanticGuardGateway(int expectedFrames) {
        List<InetSocketAddress> listenerAddresses = new ArrayList<>();
        AtomicReference<FrameSequenceServer> frameServer = new AtomicReference<>();
        Server server = server(
            new ArrayList<>(),
            new AtomicInteger(),
            () -> {
                FrameSequenceServer opened = FrameSequenceServer.open(expectedFrames);
                frameServer.set(opened);
                return opened;
            }
        );
        Client client = new Client((component, context) -> {
            Client typed = (Client) component;
            return ComponentRuntime.<ResolvedRoutes>runtime()
                .operations(new ResolvedRoutes(
                    context.resolve(typed.command),
                    context.resolve(typed.session)
                ))
                .build();
        });
        EnvironmentBuilder builder = new EnvironmentBuilder()
            .components(client, server)
            .connect(client.command, server.command)
            .connect(client.session, server.session);
        InteractionGateway gateway = new InteractionGateway(port -> {});
        ConnectionRouting routing = ConnectionRouting.routed(
            COMMAND,
            requiredLengthProfile(),
            gateway.tcp(
                commandAdapter(
                    "semantic-predecessor-route",
                    listenerAddresses,
                    new ArrayList<>()
                ),
                LengthPrefixedProtocolAdapter.correlating(),
                new ProtocolLimits(128, 256)
            )
        ).withRoute(
            SESSION,
            gateway.tcp(sessionAdapter(
                "semantic-predecessor-session-route",
                listenerAddresses,
                new ArrayList<>()
            ))
        );
        return new SemanticGuardGateway(
            routedEnvironment(builder, routing),
            listenerAddresses,
            frameServer,
            ConnectionId.between(client.command, server.command)
        );
    }

    private static SemanticInteractionSelector<
        LengthPrefixedProtocolAdapter.FrameEvidence
    > frameSelector(
        ConnectionId connectionId,
        ProofSubjectRef subject,
        String payload
    ) {
        byte[] bytes = payload.getBytes(UTF_8);
        String digest = LengthPrefixedProtocolAdapter.sha256(bytes);
        return SemanticInteractionSelector.matching(
            connectionId,
            FlowDirection.CONSUMER_TO_PROVIDER,
            LengthPrefixedProtocolAdapter.CODEC,
            evidence -> evidence.payloadBytes() == bytes.length
                && evidence.payloadSha256().equals(digest)
        ).forSubject(subject);
    }

    private static void assertFullGatewayForwardingFailure(
        ForwardingFailurePoint failurePoint
    ) throws Exception {
        AtomicReference<FailingForwardingOutput> failingOutput = new AtomicReference<>();
        InteractionGateway gateway = new InteractionGateway(
            port -> {},
            ServerSocketGatewayListener::open,
            (direction, destination) -> {
                if (direction != FlowDirection.CONSUMER_TO_PROVIDER) {
                    return destination;
                }
                FailingForwardingOutput output = new FailingForwardingOutput(
                    destination,
                    failurePoint
                );
                if (!failingOutput.compareAndSet(null, output)) {
                    throw new AssertionError("Consumer forwarding output was decorated twice");
                }
                return output;
            }
        );
        SubjectGatewayFixture fixture = subjectGateway(
            gateway,
            new LengthPrefixedProtocolAdapter()
        );
        String payload = "full-gateway-" + failurePoint.name().toLowerCase();
        SemanticHold hold = fixture.environment().controls().arm(
            SemanticInteractionSelector.matching(
                fixture.connectionId(),
                FlowDirection.CONSUMER_TO_PROVIDER,
                LengthPrefixedProtocolAdapter.CODEC,
                evidence -> evidence.payloadSha256().equals(
                    LengthPrefixedProtocolAdapter.sha256(payload.getBytes(UTF_8))
                )
            ),
            Duration.ofSeconds(5)
        );

        try {
            fixture.environment().start();
            try (Socket socket = connect(fixture.listenerAddresses().getFirst())) {
                socket.getOutputStream().write(
                    LengthPrefixedProtocolAdapter.frame(payload)
                );
                socket.getOutputStream().flush();
                hold.reached().toCompletableFuture().get(5, TimeUnit.SECONDS);
                fixture.frameServer().get().assertNoBytes();

                var release = hold.release();

                assertThat(hold.completion().toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .isEqualTo(SemanticHoldState.FAILED);
                assertCompletedExceptionally(release.toCompletableFuture());
                assertThat(hold.state()).isEqualTo(SemanticHoldState.FAILED);
                assertPeerClosed(socket);
                FailingForwardingOutput output = failingOutput.get();
                assertThat(output).isNotNull();
                assertThat(output.writeAttempts()).isEqualTo(1);
                assertThat(output.flushAttempts()).isEqualTo(
                    failurePoint == ForwardingFailurePoint.WRITE ? 0 : 1
                );
                assertThat(fixture.environment().journalSnapshot().entries().stream()
                    .map(entry -> entry.event())
                    .filter(SemanticHoldEvent.class::isInstance)
                    .map(SemanticHoldEvent.class::cast)
                    .filter(event -> event.state() == SemanticHoldState.FAILED))
                    .singleElement()
                    .satisfies(event -> assertThat(event.failure())
                        .contains(SemanticHoldFailure.WRITE_FAILURE));
            }
        } finally {
            fixture.environment().close();
        }
    }

    private static RoutedEnvironment observedEnvironment(
        ObservationRequirement requirement,
        ControllableGatewayListener listener
    ) {
        return observedEnvironment(
            requirement,
            listener,
            new LengthPrefixedProtocolAdapter()
        );
    }

    private static RoutedEnvironment observedEnvironment(
        ObservationRequirement requirement,
        ControllableGatewayListener listener,
        ProtocolAdapter<LengthPrefixedProtocolAdapter.FrameEvidence> adapter
    ) {
        return observedEnvironment(
            requirement,
            listener,
            adapter,
            requiredLengthProfile()
        );
    }

    private static RoutedEnvironment observedEnvironment(
        ObservationRequirement requirement,
        ControllableGatewayListener listener,
        ProtocolAdapter<LengthPrefixedProtocolAdapter.FrameEvidence> adapter,
        RequiredObservationProfile requiredObservationProfile
    ) {
        AtomicInteger openedListeners = new AtomicInteger();
        InteractionGateway gateway = new InteractionGateway(
            port -> {},
            () -> openedListeners.getAndIncrement() == 0
                ? listener
                : ServerSocketGatewayListener.open()
        );
        Server server = server(new ArrayList<>(), new AtomicInteger());
        Client client = new Client((component, context) -> {
            Client typed = (Client) component;
            return ComponentRuntime.<ResolvedRoutes>runtime()
                .operations(new ResolvedRoutes(
                    context.resolve(typed.command),
                    context.resolve(typed.session)
                ))
                .build();
        });
        EnvironmentBuilder builder = new EnvironmentBuilder()
            .components(client, server)
            .connect(client.command, server.command)
            .connect(client.session, server.session);
        ConnectionRouteProvider<CommandEndpoint> observedRoute = gateway.tcp(
                commandAdapter("observed-route", new ArrayList<>(), new ArrayList<>()),
                adapter,
                new ProtocolLimits(128, 256)
        );
        ConnectionRouting routing = (requirement == ObservationRequirement.REQUIRED
            ? ConnectionRouting.routed(COMMAND, requiredObservationProfile, observedRoute)
            : ConnectionRouting.routed(COMMAND, requirement, observedRoute)
        ).withRoute(
            SESSION,
            gateway.tcp(sessionAdapter(
                "transparent-route",
                new ArrayList<>(),
                new ArrayList<>()
            ))
        );
        return routedEnvironment(builder, routing);
    }

    private static EffectiveObservationStatus observationStatus(
        Environment environment,
        ObservationRequirement requirement
    ) {
        return environment.runtimeConnections()
            .stream()
            .filter(snapshot -> snapshot.observationRequirement() == requirement)
            .findFirst()
            .orElseThrow()
            .effectiveObservationStatus();
    }

    private static SemanticInteractionSelector<
        LengthPrefixedProtocolAdapter.FrameEvidence
    > semanticSelector(ConnectionId connectionId) {
        return SemanticInteractionSelector.matching(
            connectionId,
            FlowDirection.CONSUMER_TO_PROVIDER,
            LengthPrefixedProtocolAdapter.CODEC,
            ignored -> true
        );
    }

    private static RequiredObservationProfile requiredLengthProfile() {
        return requiredLengthProfile(
            LengthPrefixedProtocolAdapter.CODEC.schemaId(),
            Set.of(
                Capability.CORRELATION_CONTRIBUTIONS,
                Capability.SEMANTIC_CONTROL
            ),
            Set.of()
        );
    }

    private static RequiredObservationProfile requiredLengthProfile(
        EvidenceSchemaId evidenceSchema,
        Set<Capability> capabilities,
        Set<Feature> requiredFeatures
    ) {
        return new RequiredObservationProfile(
            evidenceSchema,
            Optional.of(LengthPrefixedProtocolAdapter.NATIVE_REFERENCE_CODEC.schemaId()),
            capabilities,
            requiredFeatures
        );
    }

    private static void assertRequiredProfileRejected(
        RequiredObservationProfile requiredObservationProfile,
        String mismatch
    ) {
        RoutedEnvironment environment = observedEnvironment(
            ObservationRequirement.REQUIRED,
            ControllableGatewayListener.scripted(32143),
            new LengthPrefixedProtocolAdapter(),
            requiredObservationProfile
        );

        EnvironmentStartException thrown = catchThrowableOfType(
            environment::start,
            EnvironmentStartException.class
        );

        assertThat(thrown.getCause())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(
                "Protocol adapter is incompatible with connection '"
                    + environment.connections().getFirst().id()
                    + "': " + mismatch + " mismatch"
            );
    }

    private static void awaitObservationStatus(
        Environment environment,
        ObservationRequirement requirement,
        EffectiveObservationStatus expected
    ) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (observationStatus(environment, requirement) != expected
            && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(observationStatus(environment, requirement)).isEqualTo(expected);
    }

    private static void awaitAmbiguousCorrelation(
        Environment environment,
        ProofSubjectRef subject,
        CorrelationKey key
    ) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        CorrelationResult<?> result;
        do {
            result = environment.proofSubjects().correlation(
                subject,
                key,
                LengthPrefixedProtocolAdapter.NATIVE_REFERENCE_CODEC
            );
            if (result instanceof CorrelationResult.Ambiguous<?>) {
                return;
            }
            Thread.onSpinWait();
        } while (System.nanoTime() < deadline);
        assertThat(result).isInstanceOf(CorrelationResult.Ambiguous.class);
    }

    private static Server server(
        List<String> lifecycle,
        AtomicInteger providerCloses
    ) {
        return server(
            lifecycle,
            providerCloses,
            () -> LineServer.open("command-provider:")
        );
    }

    private static Server correlationServer(
        AtomicReference<FrameServer> frameServerReference
    ) {
        return server(
            new ArrayList<>(),
            new AtomicInteger(),
            () -> {
                FrameServer frameServer = FrameServer.open();
                frameServerReference.set(frameServer);
                return frameServer;
            }
        );
    }

    private static Server server(
        List<String> lifecycle,
        AtomicInteger providerCloses,
        Supplier<TestServer> commandServerFactory
    ) {
        return new Server((component, context) -> {
            TestServer commandServer = commandServerFactory.get();
            LineServer sessionServer;
            try {
                sessionServer = LineServer.open("session-provider:");
            } catch (RuntimeException failure) {
                try {
                    commandServer.close();
                } catch (Exception cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
            lifecycle.add("provider-start");
            return ComponentRuntime.<Void>runtime(() -> {
                Throwable failure = null;
                try {
                    sessionServer.close();
                } catch (Exception closeFailure) {
                    failure = closeFailure;
                }
                try {
                    commandServer.close();
                } catch (Exception closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
                providerCloses.incrementAndGet();
                if (failure instanceof Exception exception) {
                    throw exception;
                }
            })
                .provides(
                    ((Server) component).command,
                    binding(
                        new CommandEndpoint("127.0.0.1", commandServer.port()),
                        new CommandEndpoint("127.0.0.1", commandServer.port())
                    )
                )
                .provides(
                    ((Server) component).session,
                    binding(
                        new SessionEndpoint(
                            "127.0.0.1",
                            sessionServer.port(),
                            Secret.secret("session-secret")
                        ),
                        new SessionEndpoint(
                            "127.0.0.1",
                            sessionServer.port(),
                            Secret.secret("session-secret")
                        )
                    )
                )
                .build();
        });
    }

    private static TcpEndpointAdapter<CommandEndpoint> commandAdapter(
        String event,
        List<InetSocketAddress> listenerAddresses,
        List<String> lifecycle
    ) {
        return endpoint(
            value -> new InetSocketAddress(value.host(), value.port()),
            (value, host, port) -> {
                recordListener(event, host, port, listenerAddresses, lifecycle);
                return new CommandEndpoint(host, port);
            }
        );
    }

    private static TcpEndpointAdapter<SessionEndpoint> sessionAdapter(
        String event,
        List<InetSocketAddress> listenerAddresses,
        List<String> lifecycle
    ) {
        return endpoint(
            value -> new InetSocketAddress(value.host(), value.port()),
            (value, host, port) -> {
                recordListener(event, host, port, listenerAddresses, lifecycle);
                return new SessionEndpoint(host, port, value.password());
            }
        );
    }

    private static void recordListener(
        String event,
        String host,
        int port,
        List<InetSocketAddress> listenerAddresses,
        List<String> lifecycle
    ) {
        if (InteractionGateway.TEST_HOST.equals(host)) {
            listenerAddresses.add(new InetSocketAddress(host, port));
            lifecycle.add(event + ":" + port);
        }
    }

    private static String exchange(InetSocketAddress address, String request)
        throws IOException {
        try (Socket socket = connect(address)) {
            return exchange(socket, request);
        }
    }

    private static String exchangeUnchecked(InetSocketAddress address, String request) {
        try {
            return exchange(address, request);
        } catch (IOException failure) {
            throw new AssertionError(
                "Gateway listener was unavailable during consumer startup",
                failure
            );
        }
    }

    private static Socket connect(InetSocketAddress address) throws IOException {
        Socket socket = new Socket();
        socket.connect(address, 2_000);
        socket.setSoTimeout(2_000);
        return socket;
    }

    private static String exchange(Socket socket, String request) throws IOException {
        BufferedWriter output = new BufferedWriter(
            new OutputStreamWriter(socket.getOutputStream(), UTF_8)
        );
        output.write(request);
        output.newLine();
        output.flush();
        return new BufferedReader(
            new InputStreamReader(socket.getInputStream(), UTF_8)
        ).readLine();
    }

    private static void assertPeerClosed(Socket socket) throws IOException {
        try {
            assertThat(socket.getInputStream().read()).isEqualTo(-1);
        } catch (SocketException closedByPeer) {
            assertThat(closedByPeer).hasMessageNotContaining("timed out");
        }
    }

    private static void assertCompletedExceptionally(
        java.util.concurrent.CompletableFuture<?> future
    ) {
        assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
            .isInstanceOf(java.util.concurrent.ExecutionException.class)
            .hasCauseInstanceOf(IllegalStateException.class);
    }

    private static void assertPortCanBeRebound(InetSocketAddress address) {
        assertPortCanBeRebound(address.getPort());
    }

    private static void assertPortCanBeRebound(int port) {
        try (ServerSocket rebound = new ServerSocket()) {
            rebound.setReuseAddress(true);
            rebound.bind(new InetSocketAddress(
                InetAddress.getByAddress(new byte[] {127, 0, 0, 1}),
                port
            ));
            assertThat(rebound.isBound()).isTrue();
        } catch (IOException failure) {
            throw new AssertionError("Listener port was not released", failure);
        }
    }

    record CommandEndpoint(String host, int port) {}

    private record SessionEndpoint(
        String host,
        int port,
        Secret<String> password
    ) {}

    private record ResolvedRoutes(
        CommandEndpoint command,
        SessionEndpoint session
    ) {}

    private record SubjectGatewayFixture(
        RoutedEnvironment environment,
        List<InetSocketAddress> listenerAddresses,
        AtomicReference<FrameServer> frameServer,
        ConnectionId connectionId
    ) {}

    private record SemanticGuardGateway(
        RoutedEnvironment environment,
        List<InetSocketAddress> listenerAddresses,
        AtomicReference<FrameSequenceServer> server,
        ConnectionId connectionId
    ) {}

    private record EmptyConfig() implements RuntimeConfig {}

    private enum Invocation implements InteractionSpec {
        INSTANCE;

        @Override
        public String id() {
            return "invocation";
        }
    }

    private enum Session implements InteractionSpec {
        INSTANCE;

        @Override
        public String id() {
            return "session";
        }
    }

    private enum Http implements ProtocolSpec {
        INSTANCE;

        @Override
        public String id() {
            return "http";
        }

        @Override
        public String scheme() {
            return "http";
        }
    }

    private enum Smpp implements ProtocolSpec {
        INSTANCE;

        @Override
        public String id() {
            return "smpp";
        }

        @Override
        public String scheme() {
            return "smpp";
        }
    }

    private static final class Client
        extends AbstractComponent<EmptyConfig, ResolvedRoutes> {

        private final RequiredPort<CommandEndpoint> command;
        private final RequiredPort<SessionEndpoint> session;

        private Client(ComponentDriver<EmptyConfig, ResolvedRoutes> driver) {
            super(
                ComponentId.component(CLIENT),
                new EmptyConfig(),
                ResolvedRoutes.class,
                driver
            );
            command = requiresAtStartup(this,
                "command",
                COMMAND,
                Invocation.INSTANCE,
                Http.INSTANCE
            );
            session = requiresAtStartup(this,
                "session",
                SESSION,
                Session.INSTANCE,
                Smpp.INSTANCE
            );
        }
    }

    private static final class Server extends AbstractComponent<EmptyConfig, Void> {
        private final ProvidedPort<CommandEndpoint> command;
        private final ProvidedPort<SessionEndpoint> session;

        private Server(ComponentDriver<EmptyConfig, Void> driver) {
            super(ComponentId.component(SERVER), new EmptyConfig(), Void.class, driver);
            command = provides(this, "command", COMMAND, Invocation.INSTANCE, Http.INSTANCE);
            session = provides(this, "session", SESSION, Session.INSTANCE, Smpp.INSTANCE);
        }
    }

    private static RoutedEnvironment routedEnvironment(EnvironmentBuilder builder, ConnectionRouting routing) {
        return builder.build((topology, logging) ->
            new RoutedEnvironment(topology, logging, routing)
        );
    }

    private static final class RoutedEnvironment extends Environment {
        private RoutedEnvironment(EnvironmentTopology topology, EnvironmentLogging logging, ConnectionRouting routing) {
            super(topology, logging, routing);
        }

        private ResolvedRoutes routes(Client client) {
            return operations(client);
        }
    }

    private interface TestServer extends AutoCloseable {
        int port();
    }

    private static final class FrameServer implements TestServer {
        private final ServerSocket listener;
        private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
        private final CountDownLatch received = new CountDownLatch(1);
        private final CountDownLatch accepted = new CountDownLatch(1);
        private final CountDownLatch allowRead = new CountDownLatch(1);
        private final AtomicReference<Socket> connectedClient = new AtomicReference<>();
        private final AtomicReference<byte[]> payload = new AtomicReference<>();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        private FrameServer(ServerSocket listener) {
            this.listener = listener;
            tasks.submit(this::accept);
        }

        private static FrameServer open() {
            try {
                ServerSocket listener = new ServerSocket();
                listener.bind(new InetSocketAddress("127.0.0.1", 0));
                return new FrameServer(listener);
            } catch (IOException failure) {
                throw new IllegalStateException(
                    "Could not open test frame server",
                    failure
                );
            }
        }

        @Override
        public int port() {
            return listener.getLocalPort();
        }

        private byte[] awaitPayload() {
            allowRead.countDown();
            try {
                if (!received.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError(
                        "Gateway did not forward the correlated frame"
                    );
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError(
                    "Interrupted while waiting for the correlated frame",
                    interrupted
                );
            }
            if (failure.get() != null) {
                throw new AssertionError(
                    "Frame server could not receive the correlated frame",
                    failure.get()
                );
            }
            return payload.get().clone();
        }

        private void assertNoBytes() throws Exception {
            assertThat(accepted.await(5, TimeUnit.SECONDS))
                .as("frame server accepted the gateway connection")
                .isTrue();
            Socket client = connectedClient.get();
            client.setSoTimeout(150);
            try {
                assertThat(catchThrowable(() -> client.getInputStream().read()))
                    .isInstanceOf(java.net.SocketTimeoutException.class);
            } finally {
                client.setSoTimeout(0);
            }
        }

        private void assertClosedWithoutPayload() throws Exception {
            Socket client = connectedClient.get();
            client.setSoTimeout(5_000);
            try {
                assertThat(client.getInputStream().read())
                    .as("held upstream session closed without forwarding a byte")
                    .isEqualTo(-1);
                assertThat(payload.get()).isNull();
            } finally {
                client.setSoTimeout(0);
            }
        }

        private boolean hasPayload() {
            return payload.get() != null;
        }

        private void accept() {
            try (
                Socket client = listener.accept();
                DataInputStream input = new DataInputStream(client.getInputStream())
            ) {
                connectedClient.set(client);
                accepted.countDown();
                if (!allowRead.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("Frame server read was not released");
                }
                int payloadBytes = input.readInt();
                byte[] receivedPayload = input.readNBytes(payloadBytes);
                if (receivedPayload.length != payloadBytes) {
                    throw new EOFException(
                        "Frame ended before its declared payload length"
                    );
                }
                payload.set(receivedPayload);
            } catch (Throwable receiveFailure) {
                failure.set(receiveFailure);
            } finally {
                received.countDown();
            }
        }

        @Override
        public void close() throws Exception {
            allowRead.countDown();
            listener.close();
            tasks.close();
        }
    }

    private static final class FrameSequenceServer implements TestServer {
        private final ServerSocket listener;
        private final int expectedFrames;
        private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
        private final List<byte[]> frames = new ArrayList<>();
        private final List<CountDownLatch> milestones;
        private final CountDownLatch terminated = new CountDownLatch(1);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        private FrameSequenceServer(ServerSocket listener, int expectedFrames) {
            this.listener = listener;
            this.expectedFrames = expectedFrames;
            milestones = java.util.stream.IntStream.range(0, expectedFrames)
                .mapToObj(ignored -> new CountDownLatch(1))
                .toList();
            tasks.submit(this::accept);
        }

        private static FrameSequenceServer open(int expectedFrames) {
            if (expectedFrames < 1) {
                throw new IllegalArgumentException("expectedFrames must be positive");
            }
            try {
                ServerSocket listener = new ServerSocket();
                listener.bind(new InetSocketAddress("127.0.0.1", 0));
                return new FrameSequenceServer(listener, expectedFrames);
            } catch (IOException failure) {
                throw new IllegalStateException(
                    "Could not open test frame sequence server",
                    failure
                );
            }
        }

        @Override
        public int port() {
            return listener.getLocalPort();
        }

        private List<byte[]> awaitFrames(int count) {
            if (count < 1 || count > expectedFrames) {
                throw new IllegalArgumentException("count is outside the expected frame range");
            }
            try {
                if (!milestones.get(count - 1).await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("Gateway did not forward the expected frames");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for frames", interrupted);
            }
            if (failure.get() != null) {
                throw new AssertionError("Frame sequence server failed", failure.get());
            }
            synchronized (frames) {
                return frames.stream().map(byte[]::clone).toList();
            }
        }

        private void assertClosedWithoutFrames() throws Exception {
            assertClosedWithFrames();
        }

        private void assertClosedWithFrames(byte[]... expected) throws Exception {
            assertThat(terminated.await(5, TimeUnit.SECONDS))
                .as("guarded upstream session closed")
                .isTrue();
            assertThat(failure.get()).isNull();
            synchronized (frames) {
                assertThat(frames).containsExactly(expected);
            }
        }

        private void accept() {
            try (
                Socket client = listener.accept();
                DataInputStream input = new DataInputStream(client.getInputStream())
            ) {
                for (int index = 0; index < expectedFrames; index++) {
                    int payloadBytes;
                    try {
                        payloadBytes = input.readInt();
                    } catch (EOFException closed) {
                        break;
                    }
                    byte[] payload = input.readNBytes(payloadBytes);
                    if (payload.length != payloadBytes) {
                        throw new EOFException(
                            "Frame ended before its declared payload length"
                        );
                    }
                    byte[] frame = ByteBuffer.allocate(Integer.BYTES + payloadBytes)
                        .putInt(payloadBytes)
                        .put(payload)
                        .array();
                    synchronized (frames) {
                        frames.add(frame);
                    }
                    milestones.get(index).countDown();
                }
            } catch (Throwable receiveFailure) {
                failure.set(receiveFailure);
            } finally {
                terminated.countDown();
            }
        }

        @Override
        public void close() throws Exception {
            listener.close();
            tasks.close();
        }
    }

    private enum ForwardingFailurePoint {
        WRITE,
        FLUSH
    }

    private static final class FailingForwardingOutput extends OutputStream {
        private final OutputStream delegate;
        private final ForwardingFailurePoint failurePoint;
        private final AtomicInteger writeAttempts = new AtomicInteger();
        private final AtomicInteger flushAttempts = new AtomicInteger();

        private FailingForwardingOutput(
            OutputStream delegate,
            ForwardingFailurePoint failurePoint
        ) {
            this.delegate = delegate;
            this.failurePoint = failurePoint;
        }

        @Override
        public void write(int value) throws IOException {
            write(new byte[] {(byte) value});
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            writeAttempts.incrementAndGet();
            if (failurePoint == ForwardingFailurePoint.WRITE) {
                throw new IOException("Injected gateway write failure");
            }
            delegate.write(bytes, offset, length);
        }

        @Override
        public void flush() throws IOException {
            flushAttempts.incrementAndGet();
            if (failurePoint == ForwardingFailurePoint.FLUSH) {
                throw new IOException("Injected gateway flush failure");
            }
            delegate.flush();
        }

        private int writeAttempts() {
            return writeAttempts.get();
        }

        private int flushAttempts() {
            return flushAttempts.get();
        }
    }

    private static final class LineServer implements TestServer {
        private final String prefix;
        private final ServerSocket listener;
        private final ExecutorService tasks = Executors.newVirtualThreadPerTaskExecutor();
        private final Set<Socket> clients = ConcurrentHashMap.newKeySet();

        private LineServer(String prefix, ServerSocket listener) {
            this.prefix = prefix;
            this.listener = listener;
            tasks.submit(this::accept);
        }

        private static LineServer open(String prefix) {
            try {
                ServerSocket listener = new ServerSocket();
                listener.bind(new InetSocketAddress("127.0.0.1", 0));
                return new LineServer(prefix, listener);
            } catch (IOException failure) {
                throw new IllegalStateException("Could not open test line server", failure);
            }
        }

        @Override
        public int port() {
            return listener.getLocalPort();
        }

        private void accept() {
            try {
                while (!listener.isClosed()) {
                    Socket client = listener.accept();
                    clients.add(client);
                    tasks.submit(() -> serve(client));
                }
            } catch (IOException ignored) {
                // Closing the listener terminates the test server.
            }
        }

        private void serve(Socket client) {
            try (
                client;
                BufferedReader input = new BufferedReader(
                    new InputStreamReader(client.getInputStream(), UTF_8)
                );
                BufferedWriter output = new BufferedWriter(
                    new OutputStreamWriter(client.getOutputStream(), UTF_8)
                )
            ) {
                String line;
                while ((line = input.readLine()) != null) {
                    output.write(prefix);
                    output.write(line);
                    output.newLine();
                    output.flush();
                }
            } catch (IOException ignored) {
                // Route cleanup closes active test connections.
            } finally {
                clients.remove(client);
            }
        }

        @Override
        public void close() throws Exception {
            listener.close();
            clients.forEach(client -> {
                try {
                    client.close();
                } catch (IOException ignored) {
                    // The client is already closed.
                }
            });
            tasks.close();
        }
    }
}
