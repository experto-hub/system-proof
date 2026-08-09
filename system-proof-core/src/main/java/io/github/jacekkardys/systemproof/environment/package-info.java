/**
 * Defines environment declaration, assembly, lifecycle, routing, execution, and inspection.
 *
 * <p>The supported surface consists of the environment facade and builder, immutable topology,
 * logging thresholds, declared routing policy, and the narrow routing/session extension SPI.
 * Detached execution state lives in {@code environment.state}.
 * {@link io.github.jacekkardys.systemproof.environment.EnvironmentDiagnostics} is created only by
 * an environment and is the bounded secret-safe-by-policy default report. Raw and sensitive
 * capture is unsupported.
 * Package-private types own all mutable construction, lifecycle, component, connection, proof
 * execution/current-state evaluation, journal, classified diagnostics, logging-emission, and
 * cleanup state. The proof current-state index is not a second event history.
 * Proof evidence-window membership is owned by observation-allocated interaction watermarks,
 * while explicit evaluation, deadline, required-observation failure, and correlation revalidation
 * cross the same internal control boundary.
 *
 * <p>Environment execution depends on stable component, configuration, diagnostics, endpoint,
 * journal, observation, proof, and topology contracts. Those contracts never depend back on the
 * mutable environment implementation.
 */
package io.github.jacekkardys.systemproof.environment;
