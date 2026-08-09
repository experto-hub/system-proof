package io.github.jacekkardys.systemproof.environment;

import io.github.jacekkardys.systemproof.environment.state.RuntimeConnectionSnapshot;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/** Receives committed typed observation current state without retaining a second history. */
interface ProofObservationListener {
    ProofObservationListener NONE = snapshot -> {};

    void observationChanged(RuntimeConnectionSnapshot snapshot);

    default void requiredObservationFailed(ConnectionId connectionId) {}

    default void finalizePending() {}
}
