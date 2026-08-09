package io.github.jacekkardys.systemproof.environment;

import java.util.List;
import java.util.Objects;
import io.github.jacekkardys.systemproof.environment.state.EnvironmentState;
import io.github.jacekkardys.systemproof.proof.ProofSubjects;
import io.github.jacekkardys.systemproof.journal.ScenarioJournalSnapshot;
import io.github.jacekkardys.systemproof.component.AbstractComponent;
import io.github.jacekkardys.systemproof.component.Component;
import io.github.jacekkardys.systemproof.component.ComponentState;
import io.github.jacekkardys.systemproof.configuration.RuntimeConfig;
import io.github.jacekkardys.systemproof.environment.state.RuntimeConnectionSnapshot;
import io.github.jacekkardys.systemproof.topology.ConnectionId;
import io.github.jacekkardys.systemproof.topology.ConnectionRef;
import io.github.jacekkardys.systemproof.topology.RequiredPort;
import io.github.jacekkardys.systemproof.control.SemanticControls;
import io.github.jacekkardys.systemproof.proof.Proofs;

/** Small public facade over an immutable topology and one internal runtime execution. */
public class Environment implements AutoCloseable {
    private final EnvironmentTopology topology;
    private final EnvironmentLogging logging;
    private final EnvironmentRuntime runtime;

    /** Creates a runtime facade from a structurally validated topology and logging configuration. */
    public Environment(EnvironmentTopology topology, EnvironmentLogging logging) {
        this(topology, logging, ConnectionRouting.direct());
    }

    /**
     * Runtime extension seam for framework-owned connection routing without changing topology DSL.
     */
    protected Environment(EnvironmentTopology topology, EnvironmentLogging logging, ConnectionRouting routing) {
        this.topology = Objects.requireNonNull(topology, "topology must not be null");
        this.logging = Objects.requireNonNull(logging, "logging must not be null");
        runtime = EnvironmentRuntime.of(this.topology, this.logging,
            Objects.requireNonNull(routing, "routing must not be null"));
    }

    public final List<Component> components() {
        return topology.components();
    }

    public final List<ConnectionRef> connections() {
        return topology.connections();
    }

    public final EnvironmentLogging logging() {
        return logging;
    }

    public final EnvironmentState state() {
        return runtime.state();
    }

    public final boolean contains(Component component) {
        return topology.contains(component);
    }

    public final ConnectionRef connectionFrom(RequiredPort<?> port) {
        return topology.connectionFrom(port);
    }

    public final ConnectionRef connection(ConnectionId id) {
        return topology.connection(id);
    }

    public final Environment start() {
        runtime.start();
        return this;
    }

    public final boolean isRunning() {
        return state() == EnvironmentState.RUNNING;
    }

    public final EnvironmentDiagnostics diagnostics() {
        return runtime.diagnostics();
    }

    /** Captures a detached immutable snapshot of the scenario's structured journal. */
    public final ScenarioJournalSnapshot journalSnapshot() {
        return runtime.journalSnapshot();
    }

    /** Returns this environment execution's narrow proof-subject correlation facade. */
    public final ProofSubjects proofSubjects() {
        return runtime.proofSubjects();
    }

    /** Returns this environment execution's frozen-plan proof-evaluation facade. */
    public final Proofs proofs() {
        return runtime.proofs();
    }

    /** Returns this environment execution's one-shot semantic traffic-control facade. */
    public final SemanticControls controls() {
        return runtime.controls();
    }

    /** Captures detached immutable runtime-connection state in topology declaration order. */
    public final List<RuntimeConnectionSnapshot> runtimeConnections() {
        return runtime.connectionSnapshots();
    }

    /** Captures one detached immutable runtime-connection state by semantic identity. */
    public final RuntimeConnectionSnapshot runtimeConnection(ConnectionId id) {
        return runtime.connectionSnapshot(id);
    }

    protected final <C extends RuntimeConfig, O> O operations(AbstractComponent<C, O> component) {
        if (!contains(component)) {
            throw new IllegalArgumentException(
                "Component '" + component.id() + "' is outside the environment"
            );
        }
        return runtime.operations(component);
    }

    public final ComponentState componentState(Component component) {
        return runtime.componentState(component);
    }

    @Override
    public final void close() {
        runtime.close();
    }

}
