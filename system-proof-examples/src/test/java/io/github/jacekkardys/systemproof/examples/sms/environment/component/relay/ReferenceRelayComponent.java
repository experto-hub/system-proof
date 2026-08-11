package io.github.jacekkardys.systemproof.examples.sms.environment.component.relay;

import static io.github.jacekkardys.systemproof.environment.ComponentPortFactory.requiresAtStartup;
import static io.github.jacekkardys.systemproof.examples.sms.environment.SmsContractIds.SMSC_SMPP;
import static io.github.jacekkardys.systemproof.examples.sms.environment.SmsContractIds.SMS_INGESTION;
import static io.github.jacekkardys.systemproof.topology.Contract.contract;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import io.github.jacekkardys.systemproof.component.AbstractComponent;
import io.github.jacekkardys.systemproof.component.ComponentId;
import io.github.jacekkardys.systemproof.component.ComponentType;
import io.github.jacekkardys.systemproof.configuration.RuntimeConfig;
import io.github.jacekkardys.systemproof.driver.ComponentDriver;
import io.github.jacekkardys.systemproof.driver.ComponentRuntime;
import io.github.jacekkardys.systemproof.driver.DriverContext;
import io.github.jacekkardys.systemproof.endpoint.SmppEndpoint;
import io.github.jacekkardys.systemproof.topology.Contract;
import io.github.jacekkardys.systemproof.topology.InteractionSpec;
import io.github.jacekkardys.systemproof.topology.ProtocolSpec;
import io.github.jacekkardys.systemproof.topology.RequiredPort;

/** JVM reference relay that synchronously maps one SMPP delivery to one HTTP callback. */
public final class ReferenceRelayComponent
    extends AbstractComponent<ReferenceRelayComponent.Config, ReferenceRelayOperations> {
    private static final Contract<SmppEndpoint> SMPP = contract(
        SMSC_SMPP,
        SmppEndpoint.class
    );
    private static final Contract<URI> HTTP = contract(SMS_INGESTION, URI.class);

    private final RequiredPort<SmppEndpoint> smpp;
    private final RequiredPort<URI> sms;

    private ReferenceRelayComponent() {
        super(
            ComponentId.component(ComponentType.of("reference-smpp-http-relay")),
            new Config(),
            ReferenceRelayOperations.class,
            new Driver()
        );
        smpp = requiresAtStartup(
            this,
            SMSC_SMPP,
            SMPP,
            Session.INSTANCE,
            Smpp.INSTANCE
        );
        sms = requiresAtStartup(
            this,
            SMS_INGESTION,
            HTTP,
            Invocation.INSTANCE,
            Http.INSTANCE
        );
    }

    public static ReferenceRelayComponent create() {
        return new ReferenceRelayComponent();
    }

    public RequiredPort<SmppEndpoint> smpp() {
        return smpp;
    }

    public RequiredPort<URI> sms() {
        return sms;
    }

    public record Config() implements RuntimeConfig {}

    private static final class Driver
        implements ComponentDriver<Config, ReferenceRelayOperations> {
        @Override
        public ComponentRuntime<ReferenceRelayOperations> start(
            AbstractComponent<Config, ReferenceRelayOperations> component,
            DriverContext context
        ) {
            ReferenceRelayComponent relay = (ReferenceRelayComponent) component;
            SmppEndpoint resolvedSmsc = context.resolve(relay.smpp);
            SmppEndpoint smsc = new SmppEndpoint(
                InetAddress.getLoopbackAddress().getHostAddress(),
                resolvedSmsc.port(),
                resolvedSmsc.systemId(),
                resolvedSmsc.password()
            );
            URI resolvedCallback = context.resolve(relay.sms);
            URI callback = replaceHost(
                resolvedCallback,
                InetAddress.getLoopbackAddress().getHostAddress()
            );
            try {
                ReferenceRelayOperations operations = ReferenceRelayOperations.open(
                    smsc,
                    callback
                );
                return ComponentRuntime.<ReferenceRelayOperations>runtime(operations)
                    .operations(operations)
                    .build();
            } catch (IOException failure) {
                throw new IllegalStateException("Cannot start the reference relay", failure);
            }
        }

        private static URI replaceHost(URI endpoint, String host) {
            try {
                return new URI(
                    endpoint.getScheme(),
                    endpoint.getUserInfo(),
                    host,
                    endpoint.getPort(),
                    endpoint.getPath(),
                    endpoint.getQuery(),
                    endpoint.getFragment()
                );
            } catch (Exception failure) {
                throw new IllegalArgumentException(
                    "Cannot replace the reference relay HTTP address",
                    failure
                );
            }
        }
    }

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
}
