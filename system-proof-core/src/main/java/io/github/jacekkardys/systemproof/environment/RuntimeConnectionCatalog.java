package io.github.jacekkardys.systemproof.environment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import io.github.jacekkardys.systemproof.component.Component;
import io.github.jacekkardys.systemproof.topology.Connection;
import io.github.jacekkardys.systemproof.topology.ConnectionId;
import io.github.jacekkardys.systemproof.topology.ConnectionRef;
import io.github.jacekkardys.systemproof.topology.ProvidedPort;
import io.github.jacekkardys.systemproof.topology.RequiredPort;
import io.github.jacekkardys.systemproof.observation.InteractionDecisionCoordinator;

/** Immutable one-to-one materialization and identity indexes for topology connections. */
final class RuntimeConnectionCatalog {
    private final List<RuntimeConnection<?>> connections;
    private final Map<ConnectionId, RuntimeConnection<?>> connectionsById;
    private final IdentityHashMap<RequiredPort<?>, RuntimeConnection<?>> connectionsByRequired =
        new IdentityHashMap<>();
    private final IdentityHashMap<Component, List<RuntimeConnection<?>>> connectionsByProvider =
        new IdentityHashMap<>();
    private final IdentityHashMap<ProvidedPort<?>, List<RuntimeConnection<?>>> connectionsByProvided =
        new IdentityHashMap<>();

    RuntimeConnectionCatalog(
        List<ConnectionRef> declarations,
        EnvironmentEventPublisher events,
        ConnectionRouting routing,
        InteractionDecisionCoordinator coordinator,
        ProofSubjectRegistry proofSubjects,
        ProofEvidenceWindowTracker evidenceWindows
    ) {
        Objects.requireNonNull(declarations, "declarations must not be null");
        events = Objects.requireNonNull(events, "events must not be null");
        routing = Objects.requireNonNull(routing, "routing must not be null");
        coordinator = Objects.requireNonNull(coordinator, "coordinator must not be null");
        proofSubjects = Objects.requireNonNull(proofSubjects, "proofSubjects must not be null");
        evidenceWindows = Objects.requireNonNull(evidenceWindows, "evidenceWindows must not be null");

        List<RuntimeConnection<?>> materialized = new ArrayList<>(declarations.size());
        Map<ConnectionId, RuntimeConnection<?>> byId = new LinkedHashMap<>();
        for (ConnectionRef declaration : declarations) {
            RuntimeConnection<?> connection = materialize(
                Objects.requireNonNull(declaration, "declaration must not be null"),
                events,
                routing,
                coordinator,
                proofSubjects,
                evidenceWindows
            );
            RuntimeConnection<?> duplicateId = byId.putIfAbsent(connection.id(), connection);
            if (duplicateId != null) {
                throw new IllegalStateException(
                    "Runtime connection '" + connection.id() + "' was materialized more than once"
                );
            }
            RuntimeConnection<?> duplicateRequired = connectionsByRequired.putIfAbsent(
                connection.declaration().from(),
                connection
            );
            if (duplicateRequired != null) {
                throw new IllegalStateException(
                    "Required port '" + connection.declaration().from().qualifiedName()
                        + "' was materialized by more than one runtime connection"
                );
            }
            connectionsByProvider.computeIfAbsent(
                connection.declaration().to().owner(),
                ignored -> new ArrayList<>()
            ).add(connection);
            connectionsByProvided.computeIfAbsent(
                connection.declaration().to(),
                ignored -> new ArrayList<>()
            ).add(connection);
            materialized.add(connection);
        }
        connections = List.copyOf(materialized);
        connectionsById = Collections.unmodifiableMap(byId);
        freezeIndex(connectionsByProvider);
        freezeIndex(connectionsByProvided);
        if (connections.size() != declarations.size()
            || connectionsById.size() != declarations.size()
            || connectionsByRequired.size() != declarations.size()) {
            throw new IllegalStateException(
                "Runtime connection materialization is not one-to-one with topology declarations"
            );
        }
    }

    List<RuntimeConnection<?>> all() {
        return connections;
    }

    RuntimeConnection<?> connection(ConnectionId id) {
        Objects.requireNonNull(id, "id must not be null");
        RuntimeConnection<?> connection = connectionsById.get(id);
        if (connection == null) {
            throw new IllegalArgumentException("Connection '" + id + "' is outside the environment");
        }
        return connection;
    }

    RuntimeConnection<?> connection(RequiredPort<?> required) {
        Objects.requireNonNull(required, "required must not be null");
        RuntimeConnection<?> connection = connectionsByRequired.get(required);
        if (connection == null) {
            throw new IllegalArgumentException(
                "Required port '" + required.qualifiedName()
                    + "' has no runtime connection in this environment"
            );
        }
        return connection;
    }

    boolean owns(RuntimeConnection<?> connection) {
        return connection != null && connectionsById.get(connection.id()) == connection;
    }

    List<RuntimeConnection<?>> targeting(Component provider) {
        Objects.requireNonNull(provider, "provider must not be null");
        return connectionsByProvider.getOrDefault(provider, List.of());
    }

    List<RuntimeConnection<?>> targeting(ProvidedPort<?> provided) {
        Objects.requireNonNull(provided, "provided must not be null");
        return connectionsByProvided.getOrDefault(provided, List.of());
    }

    private static RuntimeConnection<?> materialize(
        ConnectionRef declaration,
        EnvironmentEventPublisher events,
        ConnectionRouting routing,
        InteractionDecisionCoordinator coordinator,
        ProofSubjectRegistry proofSubjects,
        ProofEvidenceWindowTracker evidenceWindows
    ) {
        return switch (declaration) {
            case Connection<?> connection -> materializeTyped(
                connection,
                events,
                routing,
                coordinator,
                proofSubjects,
                evidenceWindows
            );
        };
    }

    private static <C> RuntimeConnection<C> materializeTyped(
        Connection<C> declaration,
        EnvironmentEventPublisher events,
        ConnectionRouting routing,
        InteractionDecisionCoordinator coordinator,
        ProofSubjectRegistry proofSubjects,
        ProofEvidenceWindowTracker evidenceWindows
    ) {
        return new RuntimeConnection<>(
            declaration,
            routing.select(declaration),
            new ConnectionObservationPublisher(
                declaration,
                events,
                proofSubjects,
                evidenceWindows
            ),
            coordinator
        );
    }

    private static <K> void freezeIndex(IdentityHashMap<K, List<RuntimeConnection<?>>> index) {
        index.replaceAll((ignored, connections) -> List.copyOf(connections));
    }
}
