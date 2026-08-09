/**
 * Defines environment-scoped, protocol-neutral semantic traffic controls.
 *
 * <p>The supported API can declare controls for later all-or-nothing proof activation or arm them
 * directly before a stimulus. It exposes only immutable selector metadata, opaque identities,
 * lifecycle state, and completion signals. Outcome evaluation, original bytes, sockets, mutable
 * buffers, executors, and protocol-specific policy remain outside this package.
 */
package io.github.jacekkardys.systemproof.control;
