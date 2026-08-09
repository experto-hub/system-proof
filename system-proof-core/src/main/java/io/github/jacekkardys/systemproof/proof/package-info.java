/**
 * Defines environment-scoped proof subjects, protocol-neutral correlation, frozen proof plans,
 * and detached fail-closed results.
 *
 * <p>The package contains public immutable contracts that depend on observation values. Adapter
 * publication capabilities and all linearizable mutable registries remain owned by one
 * environment execution. A plan contains only bounded typed declarations; it owns no adapter,
 * predicate, payload, throwable, or event history. Results separate stimulus, explicit evaluation,
 * and obligation resolution, and validate their complete typed provenance matrix before exposing
 * a detached report.
 */
package io.github.jacekkardys.systemproof.proof;
