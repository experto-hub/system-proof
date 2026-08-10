# System Proof Examples

This module contains executable examples of System Proof's public API and protocol evidence.

## PostgreSQL example

`PostgresExampleIT` defines an environment with one PostgreSQL component, starts it through the
Testcontainers adapter, injects the environment through JUnit 5, and verifies behavior through
typed database operations.

The example uses `postgres:17.6-alpine` by default. Override it with
`SYSTEM_PROOF_EXAMPLE_POSTGRES_IMAGE`.

## Complete SMS ingestion example

`SmsIngestionSmokeIT` retains the complete multi-component system scenario as a baseline smoke
test:

```text
system-proof-smsc-simulator (logical component)
  -> adapted ukarim/smscsim fixture
  -> SMPP deliver_sm
  -> Jasmin 0.11
  -> POST /v1/ingestion/sms
  -> SMS ingestion service
  -> PostgreSQL transaction
       raw_sms_event + outbox_event
```

Its environment contains the SMSC simulator, Jasmin, ingestion service, PostgreSQL, RabbitMQ, and
Redis components together with their drivers, configuration, bootstrap, operations, and typed
connections.

The scenario waits until the real `ukarim/smscsim` control page lists Jasmin's bound system ID,
submits one uniquely identifiable MO through its form, and verifies one matching RAW row, one
matching Outbox row, equal RAW and aggregate IDs, and the normalized message fields.

This is not proof of T1. It does not establish ordering between the PostgreSQL commit and the SMPP
acknowledgement. Upstream logs that `deliver_sm_resp` arrived, but its stable API exposes neither
the response status nor sequence number. The smoke does not parse container logs or treat control
plane success as an SMPP acknowledgement. The accepted T1 evidence and success boundary are
defined in
[`docs/adr/0001-t1-proof-contract.md`](../docs/adr/0001-t1-proof-contract.md).

`PostgresqlCorrelatedCommitIT` routes the ingestion JDBC connection through REQUIRED PostgreSQL
observation together with REQUIRED SMPP and HTTP routes in one real topology. It arms each subject
before traffic, resolves the canonical SMS fingerprint to exactly one `SmppExchangeRef`,
`HttpExchangeRef`, and `TransactionRef`, selects the commit only through that
subject/key/transaction chain, releases it, and confirms the matching `CommitSucceeded` plus
atomic RAW/Outbox rows. Deterministic controls cover an earlier unrelated commit, two concurrent
subjects whose two commit holds are simultaneously reached on different PostgreSQL physical
sessions, separately verified sequential pool reuse, rollback, retry ambiguity, reconnect, and
secret-safe REQUIRED policy failure. Shared-key ambiguity remains fail-closed for public lookup,
subject-only holds, and native-flow holds even when the second subject arms the key after the first
candidate was published. The exact carrier and fail-closed boundaries are recorded in
[`ADR 0008`](../docs/adr/0008-aml-subject-transaction-attribution.md).

`HttpCallbackEvidenceIT` routes the Jasmin callback through REQUIRED HTTP observation. It binds the
same canonical fingerprint to one `HttpExchangeRef`, proves the exact status-200 plus
`ACK/Jasmin` response classification, and uses the generic subject-bound semantic hold to stop the
complete positive response before its first forwarded byte. The scenario repeats five times and
also proves that missing and ambiguous correlation do not select unrelated responses. This remains
HTTP evidence only, not a final cross-connection T1 proof.

`SmppEvidenceIT` routes the Jasmin/SMSCsim session through REQUIRED SMPP observation. It binds the
same canonical fingerprint to one `SmppExchangeRef`, verifies the exact `deliver_sm` and positive
`deliver_sm_resp` association, and uses the generic subject-bound semantic hold to stop the
complete response before its first forwarded byte. The scenario repeats five times and also proves
that unrelated and ambiguous correlation do not select a response. This remains SMPP evidence
only, not a final cross-connection T1 proof.

`SemanticPredecessorGuardAmlIT` composes REQUIRED PostgreSQL, HTTP, and SMPP routes with the real
protocol adapters and a real PostgreSQL container. Its controlled examples-owned protocol peers
provide deterministic handshakes without sleeps or log polling. The happy path proves, for one
exact subject, `CommitSucceeded CONFIRMED -> positive HTTP response` and `positive HTTP response
FORWARDED -> positive SMPP deliver_sm_resp`, two explicit relations, exactly-once successor bytes,
and atomic RAW/Outbox persistence. Deliberately invalid modes emit HTTP before commit confirmation
or SMPP after HTTP authorization but before its `forwarded()` callback; each records an explicit
violation, forwards zero target successor bytes, and remains terminal after the late boundary.
This enforces ordering only; proof outcomes and the final T1 proof remain outside the example.
See [`ADR 0009`](../docs/adr/0009-semantic-predecessor-guards.md).

`JasminHttpSmppCharacterizationIT` and `AmlT1ProofIT` keep three distinct claims separate. The
first holds the exact positive HTTP response and observes that the correlated positive
`deliver_sm_resp` has already arrived, falsifying only the architectural HTTP-to-SMPP hypothesis.
The second captures the positive SMPP response while the exact PostgreSQL commit is held and RAW
and Outbox remain invisible, then evaluates the authoritative direct
`CommitSucceeded -> deliver_sm_resp` obligation. Stock pinned Jasmin violates that direct T1
obligation; the canonical contract therefore remains intentionally red. The deliberately early
application is evaluated by a third focused `CommitSucceeded -> HTTP positive` plan. Source-level
control flow, decisive provenance, and repeated results are recorded in the
[`AML T1 investigation`](../docs/investigations/aml-t1-jasmin-0.11.0.md).

Default dependency images:

- `postgres:17.6-alpine`
- `rabbitmq:4.1.2-management-alpine`
- `redis:8.0.3-alpine`
- `jookies/jasmin:0.11.0@sha256:3f049692d22fd66ab08a55073f79db96fe442473ede9615e8ac085ac505a1064`

The Jasmin manifest labels map this digest to version `0.11.0` and source revision
`8455c1b875d5f22069759e8fbefcb7437c47db4b`. The default bootstrap diagnostic reports only safe
connector identifiers, method, callback-configured state, and SMPP bind state; it never lists the
configured callback URL.

The reference SUT lives under `apps/`:

- `system-proof-ingestion-service`: Spring Boot HTTP ingress with Flyway-managed
  `raw_sms_event` and `outbox_event` tables written in one transaction. It decodes Jasmin's UCS2
  `binary` form field at the HTTP boundary and returns `ACK/Jasmin` only after the transactional
  service call completes.

The logical `system-proof-smsc-simulator` component is implemented at runtime by a minimally
adapted [`ukarim/smscsim`](https://github.com/ukarim/smscsim) fixture. Its Docker build is under
`fixtures/ukarim-smscsim` and pins upstream commit
`4975a569f7be11a89f9c381494f42ccf55fd49d3`. The separate
`patches/0001-empty-deliver-sm-service-type.patch` changes only the invalid
`deliver_sm.service_type`: it emits an empty C-Octet String, selecting the SMSC default service.
The build runs `service_type_test.go` against the generated PDU before compiling the image.

During the root reactor's `verify` phase, the ingestion JAR is packaged first and the drivers build
`system-proof-ingestion-service:local` from that artifact and
`system-proof-ukarim-smscsim:local` from the pinned upstream source and patch. A clean checkout
therefore does not need either image in a registry or local Docker cache. Explicit image overrides
still use the supplied image without rebuilding it.

Image overrides:

- `SYSTEM_PROOF_EXAMPLE_POSTGRES_IMAGE`
- `SYSTEM_PROOF_EXAMPLE_RABBITMQ_IMAGE`
- `SYSTEM_PROOF_EXAMPLE_REDIS_IMAGE`
- `SYSTEM_PROOF_EXAMPLE_INGESTION_IMAGE`
- `SYSTEM_PROOF_SMSC_SIMULATOR_IMAGE`
- `SYSTEM_PROOF_EXAMPLE_JASMIN_IMAGE`

The ingestion container's database environment-variable names are configurable through:

- `SYSTEM_PROOF_EXAMPLE_INGESTION_DATABASE_URL_VARIABLE`
- `SYSTEM_PROOF_EXAMPLE_INGESTION_DATABASE_USERNAME_VARIABLE`
- `SYSTEM_PROOF_EXAMPLE_INGESTION_DATABASE_PASSWORD_VARIABLE`

Their defaults are `DATABASE_URL`, `DATABASE_USERNAME`, and `DATABASE_PASSWORD`. The overrides
allow another service image with a different environment contract to run unchanged.

## Test-JVM interaction gateway spike

`InteractionGatewayIT` starts one provider container and one consumer container around a single
test-JVM gateway. Two distinct `RuntimeConnection` routes coexist: an HTTP request/response path
using `EndpointAddress` and an SMPP-representative long-lived path using `SmppEndpoint`. The
session carries three exchanges over one connection, and distinct provider response prefixes make
cross-wiring observable.

The scenario also injects consumer startup failure after both routed endpoints resolve and verifies
that provider, listener, and connection resources are released. The complete address, lifecycle,
supported-environment, and failure-mode decision is in
[`docs/adr/0002-test-jvm-interaction-gateway.md`](../docs/adr/0002-test-jvm-interaction-gateway.md).

## Running

Run unit tests without Docker:

```bash
./mvnw clean test
```

Run both examples with Docker:

```bash
./mvnw clean verify
```

The adapted fixture retains upstream's intentionally small SMPP 3.4 subset and does not validate
incoming PDUs. Its control plane exposes `GET /` and the `POST /` MO form on port `12775`; SMPP is
on port `2775`. The POST finishes after writing `deliver_sm`, not after a correlated response.
The simulator log remains diagnostic. The bounded SMPP adapter now contributes structured session,
exchange, sequence, status, and acknowledgement evidence. The gateway also supports typed
PostgreSQL and HTTP observation through their bounded protocol modules. See
[`docs/third-party.md`](../docs/third-party.md) for the MIT attribution, exact pin, patch, and
complete limitations.
