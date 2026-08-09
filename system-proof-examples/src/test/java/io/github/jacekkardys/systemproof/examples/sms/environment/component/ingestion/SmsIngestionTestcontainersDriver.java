package io.github.jacekkardys.systemproof.examples.sms.environment.component.ingestion;

import static io.github.jacekkardys.systemproof.testcontainers.component.PortBinding.port;

import java.net.URI;
import lombok.NonNull;
import org.testcontainers.utility.DockerImageName;
import io.github.jacekkardys.systemproof.examples.sms.environment.ReferenceImages;
import io.github.jacekkardys.systemproof.examples.sms.environment.component.ingestion.SmsIngestionConfig.Driver;
import io.github.jacekkardys.systemproof.endpoint.JdbcEndpoint;
import io.github.jacekkardys.systemproof.driver.DriverContext;
import io.github.jacekkardys.systemproof.testcontainers.component.ContainerPlan;
import io.github.jacekkardys.systemproof.testcontainers.component.PortBinding;
import io.github.jacekkardys.systemproof.testcontainers.component.TestcontainersDriver;

public final class SmsIngestionTestcontainersDriver
    extends TestcontainersDriver<SmsIngestionConfig, Void, SmsIngestionComponent> {
    private final Driver configuration;
    private final AcknowledgementMode acknowledgementMode;

    public SmsIngestionTestcontainersDriver(@NonNull Driver configuration) {
        this(configuration, AcknowledgementMode.AFTER_COMMIT);
    }

    public SmsIngestionTestcontainersDriver(
        @NonNull Driver configuration,
        @NonNull AcknowledgementMode acknowledgementMode
    ) {
        super(SmsIngestionComponent.class);
        this.configuration = configuration;
        this.acknowledgementMode = acknowledgementMode;
    }

    @Override
    protected ContainerPlan create(SmsIngestionComponent component, DriverContext context) {
        JdbcEndpoint database = context.resolve(component.jdbc());
        PortBinding httpPort = port(configuration.httpPort());
        return referenceContainer()
            .environment(configuration.databaseUrlVariable(), database.url())
            .environment(configuration.databaseUsernameVariable(), database.username())
            .environment(
                configuration.databasePasswordVariable(),
                database.password().reveal()
            )
            .environment("SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE", "2")
            .environment("SPRING_DATASOURCE_HIKARI_MINIMUM_IDLE", "1")
            .environment(
                "SYSTEM_PROOF_INGESTION_ACKNOWLEDGEMENT_MODE",
                acknowledgementMode.configurationValue
            )
            .waitForHttp(
                httpPort,
                configuration.readinessPath(),
                configuration.readinessStatus()
            )
            .readinessTimeout(configuration.startupTimeout())
            .provides(
                component.sms(),
                httpPort,
                component.configuration().smsPath(),
                address -> URI.create(address.value())
            )
            .build();
    }

    private ContainerPlan.Builder referenceContainer() {
        if (ReferenceImages.INGESTION.equals(configuration.image())) {
            return ContainerPlan.container(ReferenceImages.ingestion());
        }
        return ContainerPlan.container(DockerImageName.parse(configuration.image()));
    }

    /** Explicit reference-application behavior selected by the owning test environment. */
    public enum AcknowledgementMode {
        AFTER_COMMIT("after-commit"),
        BEFORE_COMMIT("before-commit");

        private final String configurationValue;

        AcknowledgementMode(String configurationValue) {
            this.configurationValue = configurationValue;
        }
    }
}
