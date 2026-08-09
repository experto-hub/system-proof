package io.github.jacekkardys.systemproof.environment;

import static io.github.jacekkardys.systemproof.control.SemanticInteractionSelector.matching;
import static io.github.jacekkardys.systemproof.control.SemanticPredecessorRequirement.confirmed;
import static io.github.jacekkardys.systemproof.endpoint.EndpointBinding.binding;
import static io.github.jacekkardys.systemproof.environment.ComponentPortFactory.provides;
import static io.github.jacekkardys.systemproof.environment.ComponentPortFactory.requiresAtStartup;
import static io.github.jacekkardys.systemproof.observation.RequiredObservationProfile.Capability.CORRELATION_CONTRIBUTIONS;
import static io.github.jacekkardys.systemproof.observation.RequiredObservationProfile.Capability.SEMANTIC_CONTROL;
import static io.github.jacekkardys.systemproof.topology.Contract.contract;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import io.github.jacekkardys.systemproof.component.AbstractComponent;
import io.github.jacekkardys.systemproof.component.ComponentId;
import io.github.jacekkardys.systemproof.component.ComponentType;
import io.github.jacekkardys.systemproof.configuration.RuntimeConfig;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticInteractionSelector;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardSpec;
import io.github.jacekkardys.systemproof.driver.ComponentRuntime;
import io.github.jacekkardys.systemproof.observation.EffectiveObservationStatus;
import io.github.jacekkardys.systemproof.observation.EvidenceCodec;
import io.github.jacekkardys.systemproof.observation.EvidenceSchemaId;
import io.github.jacekkardys.systemproof.observation.FlowDirection;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.observation.InteractionDecisionCoordinator;
import io.github.jacekkardys.systemproof.observation.RecordedInteraction;
import io.github.jacekkardys.systemproof.observation.RequiredObservationProfile;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.CorrelationKeySchema;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.topology.ConnectionId;
import io.github.jacekkardys.systemproof.topology.Contract;
import io.github.jacekkardys.systemproof.topology.InteractionSpec;
import io.github.jacekkardys.systemproof.topology.ProtocolSpec;
import io.github.jacekkardys.systemproof.topology.ProvidedPort;
import io.github.jacekkardys.systemproof.topology.RequiredPort;

final class ProofTestFixture implements AutoCloseable {
    static final EvidenceSchemaId EVIDENCE_SCHEMA =
        new EvidenceSchemaId("system-proof-test", "proof-evidence", 1);
    static final EvidenceSchemaId NATIVE_SCHEMA =
        new EvidenceSchemaId("system-proof-test", "proof-native-reference", 1);
    static final RequiredObservationProfile PROFILE = new RequiredObservationProfile(
        EVIDENCE_SCHEMA,
        Optional.of(NATIVE_SCHEMA),
        Set.of(CORRELATION_CONTRIBUTIONS, SEMANTIC_CONTROL),
        Set.of()
    );

    final TestEnvironment environment;
    final RouteProvider route;
    final ConnectionId connectionId;
    final ProofSubjectRef subject;
    final CorrelationKey key;
    final CorrelationKey successorKey;

    private ProofTestFixture(TestEnvironment environment, RouteProvider route) {
        this.environment = environment;
        this.route = route;
        environment.start();
        connectionId = environment.connections().getFirst().id();
        subject = environment.proofSubjects().create();
        key = correlationKey(1);
        successorKey = correlationKey(2);
        environment.proofSubjects().arm(subject, key);
        environment.proofSubjects().arm(subject, successorKey);
    }

    static ProofTestFixture start() {
        RouteProvider route = new RouteProvider();
        Server server = new Server();
        Client client = new Client();
        TestEnvironment environment = new EnvironmentBuilder()
            .components(client, server)
            .connect(client.api, server.api)
            .build((topology, logging) -> new TestEnvironment(
                topology,
                logging,
                ConnectionRouting.routed(API, PROFILE, route)
            ));
        return new ProofTestFixture(environment, route);
    }

    SemanticPredecessorGuard declareGuard(Duration maximumDuration) {
        return environment.controls().declareGuard(SemanticPredecessorGuardSpec.requiring(
            subject,
            confirmed(selector("predecessor")),
            selector("successor"),
            maximumDuration
        ));
    }

    SemanticHold declareHold(String value, Duration maximumDuration) {
        return environment.controls().declareHold(selector(value), maximumDuration);
    }

    SemanticInteractionSelector<String> selector(String expected) {
        return matching(
            connectionId,
            FlowDirection.CONSUMER_TO_PROVIDER,
            TextCodec.INSTANCE,
            expected::equals
        ).forSubject(subject);
    }

    RecordedInteraction correlated(String value) {
        InteractionSession session = route.observations().openSession();
        RecordedInteraction interaction = session.record(
            FlowDirection.CONSUMER_TO_PROVIDER,
            TextCodec.INSTANCE,
            value
        );
        session.correlate(
            interaction.interactionRef(),
            CorrelationContribution.capture(
                "successor".equals(value) ? successorKey : key,
                NativeCodec.INSTANCE,
                value
            )
        );
        return interaction;
    }

    ForwardingPermit permit(RecordedInteraction interaction) {
        return route.coordinator().permit(interaction);
    }

    ProofSubjectRef addSubjectForSameKey() {
        ProofSubjectRef other = environment.proofSubjects().create();
        environment.proofSubjects().arm(other, key);
        return other;
    }

    void observationStatus(EffectiveObservationStatus status) {
        route.status.set(status);
        environment.runtimeConnection(connectionId);
    }

    @Override
    public void close() {
        environment.close();
    }

    private static CorrelationKey correlationKey(int seed) {
        byte[] digest = new byte[16];
        Arrays.fill(digest, (byte) seed);
        return CorrelationKey.ofDigest(
            new CorrelationKeySchema("system-proof-test", "proof-execution", 1),
            digest
        );
    }

    static final Contract<String> API = contract("proof-api", String.class);

    private enum Invocation implements InteractionSpec {
        INSTANCE;

        @Override
        public String id() {
            return "invocation";
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

    enum TextCodec implements EvidenceCodec<String> {
        INSTANCE;

        @Override
        public EvidenceSchemaId schemaId() {
            return EVIDENCE_SCHEMA;
        }

        @Override
        public byte[] encode(String evidence) {
            return evidence.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] encodedEvidence) {
            return new String(encodedEvidence, StandardCharsets.UTF_8);
        }
    }

    enum NativeCodec implements EvidenceCodec<String> {
        INSTANCE;

        @Override
        public EvidenceSchemaId schemaId() {
            return NATIVE_SCHEMA;
        }

        @Override
        public byte[] encode(String evidence) {
            return evidence.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] encodedEvidence) {
            return new String(encodedEvidence, StandardCharsets.UTF_8);
        }
    }

    record EmptyConfig() implements RuntimeConfig {}

    static final class Client extends AbstractComponent<EmptyConfig, Void> {
        final RequiredPort<String> api;

        Client() {
            super(
                ComponentId.component(ComponentType.of("proof-client")),
                new EmptyConfig(),
                Void.class,
                (component, context) -> ComponentRuntime.<Void>runtime().build()
            );
            api = requiresAtStartup(this, "api", API, Invocation.INSTANCE, Http.INSTANCE);
        }
    }

    static final class Server extends AbstractComponent<EmptyConfig, Void> {
        final ProvidedPort<String> api;

        Server() {
            super(
                ComponentId.component(ComponentType.of("proof-server")),
                new EmptyConfig(),
                Void.class,
                (component, context) -> ComponentRuntime.<Void>runtime()
                    .provides(
                        ((Server) component).api,
                        binding("proof-direct", "proof-direct-external")
                    )
                    .build()
            );
            api = provides(this, "api", API, Invocation.INSTANCE, Http.INSTANCE);
        }
    }

    static final class RouteProvider
        implements ConnectionRouteProvider<String>, SemanticControlRouteCapability {
        private final AtomicReference<EffectiveObservationStatus> status =
            new AtomicReference<>(EffectiveObservationStatus.ACTIVE);
        private final AtomicReference<RuntimeException> samplingFailure =
            new AtomicReference<>();
        private final AtomicReference<ConnectionObservations> observations =
            new AtomicReference<>();
        private final AtomicReference<InteractionDecisionCoordinator> coordinator =
            new AtomicReference<>();

        @Override
        public ConnectionRoute<String> prepare(ConnectionRouteContext<String> context) {
            observations.set(context.observations());
            coordinator.set(context.coordinator());
            return ConnectionRoute.routed(
                binding("proof-routed", "proof-routed-external"),
                this::sample,
                new RouteResource()
            );
        }

        void failSampling(RuntimeException failure) {
            samplingFailure.set(java.util.Objects.requireNonNull(
                failure,
                "failure must not be null"
            ));
        }

        private EffectiveObservationStatus sample() {
            RuntimeException failure = samplingFailure.get();
            if (failure != null) {
                throw failure;
            }
            return status.get();
        }

        ConnectionObservations observations() {
            return java.util.Objects.requireNonNull(
                observations.get(),
                "observations were not prepared"
            );
        }

        InteractionDecisionCoordinator coordinator() {
            return java.util.Objects.requireNonNull(
                coordinator.get(),
                "coordinator was not prepared"
            );
        }
    }

    private static final class RouteResource
        implements AutoCloseable, SemanticControlRouteCapability {
        @Override
        public void close() {}
    }

    static final class TestEnvironment extends Environment {
        private TestEnvironment(
            EnvironmentTopology topology,
            EnvironmentLogging logging,
            ConnectionRouting routing
        ) {
            super(topology, logging, routing);
        }
    }
}
