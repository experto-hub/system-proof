package io.github.jacekkardys.systemproof.examples.sms.environment.proof;

import static io.github.jacekkardys.systemproof.testcontainers.gateway.TcpEndpointAdapter.endpoint;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import io.github.jacekkardys.systemproof.configuration.EnvironmentConfiguration;
import io.github.jacekkardys.systemproof.endpoint.JdbcEndpoint;
import io.github.jacekkardys.systemproof.endpoint.SmppEndpoint;
import io.github.jacekkardys.systemproof.environment.ConnectionRouting;
import io.github.jacekkardys.systemproof.environment.Environment;
import io.github.jacekkardys.systemproof.environment.EnvironmentBuilder;
import io.github.jacekkardys.systemproof.environment.EnvironmentLogging;
import io.github.jacekkardys.systemproof.environment.EnvironmentTopology;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.ingestion.SmsIngestionComponent;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.ingestion.SmsIngestionConfig;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.ingestion.SmsIngestionTestcontainersDriver;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.ingestion.SmsIngestionTestcontainersDriver.AcknowledgementMode;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.jasmin.JasminComponent;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.postgres.PostgresComponent;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.postgres.SmsDatabaseOperations;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.rabbitmq.RabbitMqComponent;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.redis.RedisComponent;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.smsc.SmscComponent;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.smsc.UkarimSmscOperations;
import io.github.jacekkardys.systemproof.examples.sms.environment.domain.SmsMessageFingerprint;
import io.github.jacekkardys.systemproof.http.HttpExchangeRef;
import io.github.jacekkardys.systemproof.http.HttpProtocolAdapter;
import io.github.jacekkardys.systemproof.journal.LogLevel;
import io.github.jacekkardys.systemproof.junit.annotation.EnvironmentDefinition;
import io.github.jacekkardys.systemproof.observation.EvidenceCodec;
import io.github.jacekkardys.systemproof.observation.RequiredObservationProfile;
import io.github.jacekkardys.systemproof.observation.RequiredObservationProfile.Capability;
import io.github.jacekkardys.systemproof.postgresql.PostgresqlProtocolAdapter;
import io.github.jacekkardys.systemproof.postgresql.TransactionRef;
import io.github.jacekkardys.systemproof.smpp.SmppExchangeRef;
import io.github.jacekkardys.systemproof.smpp.SmppProtocolAdapter;
import io.github.jacekkardys.systemproof.testcontainers.gateway.InteractionGateway;
import io.github.jacekkardys.systemproof.testcontainers.gateway.ProtocolLimits;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/** Real AML reference topology with required routed observation on every T1 proof connection. */
public final class AmlT1Environment extends Environment {
    private static final ProtocolLimits POSTGRESQL_LIMITS = new ProtocolLimits(
        1024 * 1024,
        2 * 1024 * 1024
    );
    private static final ProtocolLimits HTTP_LIMITS = new ProtocolLimits(
        1024 * 1024,
        2 * 1024 * 1024
    );
    private static final ProtocolLimits SMPP_LIMITS = new ProtocolLimits(
        64 * 1024,
        128 * 1024
    );

    private final SmscComponent smsc;
    private final JasminComponent jasmin;
    private final SmsIngestionComponent ingestion;
    private final PostgresComponent database;
    private final RabbitMqComponent broker;
    private final SmppProtocolAdapter smppAdapter;
    private final HttpProtocolAdapter httpAdapter;
    private final PostgresqlProtocolAdapter postgresqlAdapter;
    private final RequiredObservationProfile smppProfile;
    private final RequiredObservationProfile httpProfile;
    private final RequiredObservationProfile postgresqlProfile;

    private AmlT1Environment(
        EnvironmentTopology topology,
        EnvironmentLogging logging,
        ConnectionRouting routing,
        SmscComponent smsc,
        JasminComponent jasmin,
        SmsIngestionComponent ingestion,
        PostgresComponent database,
        RabbitMqComponent broker,
        SmppProtocolAdapter smppAdapter,
        HttpProtocolAdapter httpAdapter,
        PostgresqlProtocolAdapter postgresqlAdapter,
        RequiredObservationProfile smppProfile,
        RequiredObservationProfile httpProfile,
        RequiredObservationProfile postgresqlProfile
    ) {
        super(topology, logging, routing);
        this.smsc = smsc;
        this.jasmin = jasmin;
        this.ingestion = ingestion;
        this.database = database;
        this.broker = broker;
        this.smppAdapter = smppAdapter;
        this.httpAdapter = httpAdapter;
        this.postgresqlAdapter = postgresqlAdapter;
        this.smppProfile = smppProfile;
        this.httpProfile = httpProfile;
        this.postgresqlProfile = postgresqlProfile;
    }

    /** Defines the canonical commit-before-acknowledgement reference application. */
    @EnvironmentDefinition
    public static AmlT1Environment define() {
        return build(
            new PostgresqlProtocolAdapter(SmsMessageFingerprint.rawWriteCorrelation()),
            AcknowledgementMode.AFTER_COMMIT
        );
    }

    /** Defines the same observed topology with a caller-owned PostgreSQL adapter. */
    public static AmlT1Environment observed(PostgresqlProtocolAdapter postgresqlAdapter) {
        return build(postgresqlAdapter, AcknowledgementMode.AFTER_COMMIT);
    }

    /** Defines the explicit test-only counterexample that acknowledges before commit. */
    public static AmlT1Environment earlyAcknowledging() {
        return build(
            new PostgresqlProtocolAdapter(SmsMessageFingerprint.rawWriteCorrelation()),
            AcknowledgementMode.BEFORE_COMMIT
        );
    }

    private static AmlT1Environment build(
        PostgresqlProtocolAdapter postgresqlAdapter,
        AcknowledgementMode acknowledgementMode
    ) {
        EnvironmentConfiguration configuration = EnvironmentConfiguration.system();
        EnvironmentBuilder builder = new EnvironmentBuilder(configuration);
        SmscComponent smsc = builder.component(SmscComponent.class);
        JasminComponent jasmin = builder.component(JasminComponent.class);
        SmsIngestionConfig ingestionConfiguration = configuration.bind(
            SmsIngestionConfig.class
        );
        SmsIngestionConfig.Driver ingestionDriverConfiguration = configuration.bind(
            SmsIngestionConfig.Driver.class
        );
        SmsIngestionComponent ingestion = builder.component(
            SmsIngestionComponent.class,
            ingestionConfiguration,
            new SmsIngestionTestcontainersDriver(
                ingestionDriverConfiguration,
                acknowledgementMode
            )
        );
        PostgresComponent database = builder.component(PostgresComponent.class);
        RabbitMqComponent broker = builder.component(RabbitMqComponent.class);
        RedisComponent state = builder.component(RedisComponent.class);
        builder
            .logging(EnvironmentLogging.logs().defaultComponentLevel(LogLevel.OFF))
            .connect(jasmin.smpp(), smsc.smpp())
            .connect(jasmin.sms(), ingestion.sms())
            .connect(ingestion.jdbc(), database.jdbc())
            .connect(jasmin.amqp(), broker.amqp())
            .connect(jasmin.redis(), state.redis());

        SmppProtocolAdapter smppAdapter = new SmppProtocolAdapter(
            SmsMessageFingerprint.smppDeliverCorrelation()
        );
        HttpProtocolAdapter httpAdapter = new HttpProtocolAdapter(
            SmsMessageFingerprint.httpCallbackCorrelation()
        );
        RequiredObservationProfile smppProfile = requiredProfile(
            smppAdapter.evidenceCodec(),
            SmppExchangeRef.codec()
        );
        RequiredObservationProfile httpProfile = requiredProfile(
            httpAdapter.evidenceCodec(),
            HttpExchangeRef.codec()
        );
        RequiredObservationProfile postgresqlProfile = requiredProfile(
            postgresqlAdapter.evidenceCodec(),
            TransactionRef.codec()
        );
        InteractionGateway gateway = new InteractionGateway();
        ConnectionRouting routing = ConnectionRouting.routed(
            jasmin.smpp().contract(),
            smppProfile,
            gateway.tcp(
                endpoint(AmlT1Environment::smppAddress, AmlT1Environment::replaceSmppAddress),
                smppAdapter,
                SMPP_LIMITS
            )
        ).withRoute(
            jasmin.sms().contract(),
            httpProfile,
            gateway.tcp(
                endpoint(AmlT1Environment::httpAddress, AmlT1Environment::replaceHttpAddress),
                httpAdapter,
                HTTP_LIMITS
            )
        ).withRoute(
            ingestion.jdbc().contract(),
            postgresqlProfile,
            gateway.tcp(
                endpoint(
                    AmlT1Environment::postgresqlAddress,
                    AmlT1Environment::replacePostgresqlAddress
                ),
                postgresqlAdapter,
                POSTGRESQL_LIMITS
            )
        );
        return builder.build((topology, logging) -> new AmlT1Environment(
            topology,
            logging,
            routing,
            smsc,
            jasmin,
            ingestion,
            database,
            broker,
            smppAdapter,
            httpAdapter,
            postgresqlAdapter,
            smppProfile,
            httpProfile,
            postgresqlProfile
        ));
    }

    public UkarimSmscOperations smsc() {
        return operations(smsc);
    }

    public SmsDatabaseOperations database() {
        return operations(database);
    }

    public ConnectionId smppConnectionId() {
        return connectionFrom(jasmin.smpp()).id();
    }

    public ConnectionId httpConnectionId() {
        return connectionFrom(jasmin.sms()).id();
    }

    public ConnectionId databaseConnectionId() {
        return connectionFrom(ingestion.jdbc()).id();
    }

    public SmppProtocolAdapter smppAdapter() {
        return smppAdapter;
    }

    public HttpProtocolAdapter httpAdapter() {
        return httpAdapter;
    }

    public PostgresqlProtocolAdapter postgresqlAdapter() {
        return postgresqlAdapter;
    }

    public RequiredObservationProfile smppProfile() {
        return smppProfile;
    }

    public RequiredObservationProfile httpProfile() {
        return httpProfile;
    }

    public RequiredObservationProfile postgresqlProfile() {
        return postgresqlProfile;
    }

    public List<String> credentials() {
        return List.of(
            smsc.configuration().password().reveal(),
            jasmin.configuration().adminPassword().reveal(),
            database.configuration().password().reveal(),
            broker.configuration().password().reveal()
        );
    }

    private static RequiredObservationProfile requiredProfile(
        EvidenceCodec<?> evidenceCodec,
        EvidenceCodec<?> referenceCodec
    ) {
        return new RequiredObservationProfile(
            evidenceCodec.schemaId(),
            Optional.of(referenceCodec.schemaId()),
            Set.of(
                Capability.CORRELATION_CONTRIBUTIONS,
                Capability.SEMANTIC_CONTROL
            ),
            Set.of()
        );
    }

    private static InetSocketAddress postgresqlAddress(JdbcEndpoint endpoint) {
        URI uri = URI.create(endpoint.url().substring("jdbc:".length()));
        return new InetSocketAddress(uri.getHost(), uri.getPort());
    }

    private static JdbcEndpoint replacePostgresqlAddress(
        JdbcEndpoint endpoint,
        String host,
        int port
    ) {
        URI uri = URI.create(endpoint.url().substring("jdbc:".length()));
        String query = uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery();
        return new JdbcEndpoint(
            "jdbc:postgresql://" + host + ":" + port + uri.getRawPath() + query,
            endpoint.username(),
            endpoint.password()
        );
    }

    private static InetSocketAddress httpAddress(URI endpoint) {
        int port = endpoint.getPort() < 0 ? 80 : endpoint.getPort();
        return new InetSocketAddress(endpoint.getHost(), port);
    }

    private static URI replaceHttpAddress(URI endpoint, String host, int port) {
        return replaceAddress(endpoint, host, port);
    }

    private static URI replaceAddress(URI endpoint, String host, int port) {
        try {
            return new URI(
                endpoint.getScheme(),
                endpoint.getUserInfo(),
                host,
                port,
                endpoint.getPath(),
                endpoint.getQuery(),
                endpoint.getFragment()
            );
        } catch (URISyntaxException failure) {
            throw new IllegalArgumentException("Cannot replace endpoint address", failure);
        }
    }

    private static InetSocketAddress smppAddress(SmppEndpoint endpoint) {
        return new InetSocketAddress(endpoint.host(), endpoint.port());
    }

    private static SmppEndpoint replaceSmppAddress(
        SmppEndpoint endpoint,
        String host,
        int port
    ) {
        return new SmppEndpoint(host, port, endpoint.systemId(), endpoint.password());
    }
}
