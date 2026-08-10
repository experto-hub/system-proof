# AML T1 falsification against pinned Jasmin 0.11.0

- Status: Reproduced
- Date: 2026-08-10
- Issue: [#13](https://github.com/JacekKardys/system-proof/issues/13)
- Pull request: [#68](https://github.com/JacekKardys/system-proof/pull/68)

## Verdict

Stock pinned Jasmin violates the direct AML T1 invariant:

```text
PostgreSQL CommitSucceeded.OBSERVED
    MUST HAPPEN BEFORE
SMPP positive deliver_sm_resp.OBSERVED
```

The direct proof result is `VIOLATED`. The controlled state witness observes the matching positive
`deliver_sm_resp` while the exact PostgreSQL commit attempt is `REACHED_HELD`, neither RAW nor
Outbox is visible, and no matching `CommitSucceeded` exists. Releasing the hold subsequently
produces one matching commit confirmation and atomically visible RAW and Outbox rows.

The stronger architectural hypothesis

```text
HTTP positive response.FORWARDED
    MUST HAPPEN BEFORE
SMPP positive deliver_sm_resp.OBSERVED
```

is independently `FALSIFIED`. It is not used as a proxy for the direct T1 verdict.

## Exact image identity

The topology configures:

```text
jookies/jasmin:0.11.0@sha256:3f049692d22fd66ab08a55073f79db96fe442473ede9615e8ac085ac505a1064
```

Inspection of the local image used by the tests returned:

- repository digest: `jookies/jasmin@sha256:3f049692d22fd66ab08a55073f79db96fe442473ede9615e8ac085ac505a1064`;
- image id: `sha256:421578837174a8850272933ca73539da7e88e9fa4d2be05a584b5682ae87745f`;
- version label: `0.11.0`;
- source revision label: `8455c1b875d5f22069759e8fbefcb7437c47db4b`;
- entrypoint: `/docker-entrypoint.sh`;
- command: `jasmind.py --enable-interceptor-client --enable-dlr-thrower
  --enable-dlr-lookup -u jcliadmin -p jclipwd`.

The source below was read directly from that image, not inferred from external documentation or a
possibly different Git tag.

## Source-level inbound MO path

### 1. SMPP request dispatch

`/usr/local/lib/python3.11/site-packages/smpp/twisted/protocol.py:247-315` dispatches an inbound PDU
through `PDURequestReceived`, `PDUDataRequestReceived`, and `doPDURequest`. `doPDURequest` wraps the
registered handler with `defer.maybeDeferred`. Only when that handler completes does
`PDURequestSucceeded` call `sendResponse`; for `deliver_sm`, this creates and sends the correlated
`deliver_sm_resp` with the handler's status, defaulting to `ESME_ROK`.

### 2. Jasmin MO handler and first AMQP publication

`/usr/local/lib/python3.11/site-packages/jasmin/managers/listeners.py:528-674` handles
`deliver_sm_event_interceptor` and `deliver_sm_event_post_interception`. For an MO it constructs
`DeliverSmContent`, then yields:

```python
amqpBroker.publish(
    exchange='messaging',
    routing_key='deliver.sm.<connector-id>',
    content=content
)
```

Completion of this handler returns to `smpp.twisted.protocol.PDURequestSucceeded`, which sends the
positive `deliver_sm_resp`. There is no application HTTP completion in this call chain.

### 3. Router queue and second AMQP publication

`/usr/local/lib/python3.11/site-packages/jasmin/routing/router.py:154-221` consumes the
`deliver.sm.<connector-id>` queue in `deliver_sm_callback`. After route selection it acknowledges
the consumed AMQP message and publishes `RoutedDeliverSmContent` with routing key
`deliver_sm_thrower.http` for an HTTP route.

### 4. Asynchronous HTTP thrower

`/usr/local/lib/python3.11/site-packages/jasmin/routing/throwers.py:228-328,492-498` binds the
`deliver_sm_thrower.*` queue and dispatches `deliver_sm_thrower.http` to
`http_deliver_sm_callback`. That callback constructs the HTTP parameters and performs a separate
Twisted `Agent`/`HTTPClient` request. Its response and retry handling happen in this later AMQP
consumer path; they do not participate in creating the upstream `deliver_sm_resp`.

### 5. Process and execution model

`/usr/local/bin/jasmind.py:215-257,380-455,526-546` starts the SMPP services, RouterPB, and
`deliverSmThrower`, attaches the thrower to its AMQP consumer, and runs one Twisted reactor. In the
pinned container these are asynchronous components in the same `jasmind.py` OS process, not a
synchronous HTTP call on the inbound SMPP handler and not a separate HTTP worker process.

The source-supported sequence is therefore:

```text
SMSCsim
  -> Jasmin SMPP client listener
  -> deliver_sm_event_post_interception
  -> publish DeliverSmContent to RabbitMQ
  -> handler Deferred completes
  -> positive deliver_sm_resp to SMSCsim

RabbitMQ deliver.sm.<connector-id>
  -> Router deliver_sm_callback
  -> publish RoutedDeliverSmContent to deliver_sm_thrower.http
  -> DeliverSmThrower HTTP callback
  -> ingestion application
  -> PostgreSQL transaction
  -> HTTP response/retry handling
```

Consequently, a positive `deliver_sm_resp` means that Jasmin accepted the inbound MO through its
handler and completed the first internal AMQP publication call. It does not mean that the HTTP
application received the callback, acknowledged it, or durably committed its transaction.

## Causally controlled experiments

All selectors are bound to one opaque proof subject, the canonical SMS fingerprint, and the native
protocol reference (`SmppExchangeRef`, `HttpExchangeRef`, or `TransactionRef`). No assertion uses
timestamps, journal sequence, polling order, or sleeps.

### HTTP/SMPP characterization

`JasminHttpSmppCharacterizationIT.observesPositiveSmppWhileHttpResponseIsHeld` declares and arms a
hold on the exact positive HTTP response before the stimulus. At the assertion boundary:

```text
HTTP response hold = REACHED_HELD
matching positive deliver_sm_resp = observed exactly once
```

The HTTP response has not been forwarded, so HTTP forwarding cannot be a predecessor of the SMPP
response. The separate `arch-http-before-smpp-*` guard and relation both resolve `VIOLATED` with
only the SMPP successor in decisive provenance.

### Direct T1 controlled state witness

`AmlT1ProofIT.capturesPositiveSmppWhileExactCommitIsHeld` holds the exact matching PostgreSQL
`CommitAttempt` before its bytes are forwarded. At the assertion boundary:

```text
commit hold = REACHED_HELD
matching CommitAttempt = observed exactly once
matching CommitSucceeded = absent
RAW rows = 0
Outbox rows = 0
matching positive deliver_sm_resp = observed exactly once
```

After release, the same `TransactionRef` obtains one `CommitSucceeded`, and independent database
reads observe one RAW row and one matching Outbox row atomically. The direct proof plan separately
requires `CommitSucceeded.CONFIRMED -> positive deliver_sm_resp.OBSERVED`; its guard and causal
relation both resolve `VIOLATED` with only the SMPP successor in decisive provenance.

### Early-HTTP negative control

The deliberately broken application uses a focused plan containing only the durability
prerequisite, PostgreSQL and HTTP observations/correlations, and the direct
`CommitSucceeded.CONFIRMED -> HTTP positive.OBSERVED` guard. It resolves `VIOLATED` because the
exact HTTP successor appears with no established commit predecessor and the decision is
`CLOSE_SESSION`. The unrelated HTTP-to-SMPP hypothesis cannot terminate this experiment.

## Orchestration and outcome precedence

The previous helper awaited a happy-path hold after the proof had already terminalized. Terminal
proof completion correctly cancelled proof-owned controls, so that wait raised an exception and
masked the authoritative `VIOLATED` result at the JUnit boundary.

The revised orchestration waits on the decisive guard completion, obtains and validates the frozen
`ProofResult`, and stops happy-path waits immediately. The canonical invariant assertion is made
only after the exact direct violation and provenance have been verified. It therefore fails as an
assertion (`Expected proof outcome PROVED but was VIOLATED`), not as a cancelled-hold error.
Framework tests already cover that terminal violation remains authoritative when control cleanup or
stimulus follow-up also fails; no core outcome-precedence change was required.

## Repetition result

Focused real-topology runs on 2026-08-10 produced:

| Experiment | Result |
| --- | --- |
| HTTP response held with positive SMPP observed | 5/5 reproduced |
| Exact commit held, RAW=0, Outbox=0, positive SMPP observed | 5/5 reproduced |
| Canonical direct T1 contract | 5/5 `VIOLATED` (intentional red assertion) |
| Deliberately early HTTP application negative control | 3/3 detected for Commit -> HTTP |

The observed classifications were deterministic. The canonical T1 test intentionally keeps the
build red because changing the expected invariant outcome to `VIOLATED` would turn a real system
counterexample into a green contract.

## Verification commands

The focused commands were each executed against a fresh environment. The first two were repeated
five times, the canonical direct command was repeated five times with the same intentional exit
code `1`, and the negative control was repeated three times:

```powershell
$env:JAVA_HOME='C:\Program Files\Amazon Corretto\jdk21.0.12_8'

.\mvnw.cmd -pl system-proof-examples `
  "-Dit.test=JasminHttpSmppCharacterizationIT#observesPositiveSmppWhileHttpResponseIsHeld" `
  verify

.\mvnw.cmd -pl system-proof-examples `
  "-Dit.test=AmlT1ProofIT#capturesPositiveSmppWhileExactCommitIsHeld" `
  verify

.\mvnw.cmd -pl system-proof-examples `
  "-Dit.test=AmlT1ProofIT#provesDirectCommitBeforePositiveSmppResponse" `
  verify

.\mvnw.cmd -pl system-proof-examples `
  "-Dit.test=AmlT1ProofIT#rejectsTheRealEarlyAcknowledgingApplicationForCommitBeforeHttp" `
  verify
```

Final class and reactor checks:

```powershell
.\mvnw.cmd -pl system-proof-examples "-Dit.test=JasminHttpSmppCharacterizationIT" verify
.\mvnw.cmd -pl system-proof-examples "-Dit.test=AmlT1ProofIT" verify
.\mvnw.cmd clean test
.\mvnw.cmd clean verify
```

The characterization class was `2 tests, 0 failures, 0 errors`. The T1 class was exactly `3 tests,
1 failure, 0 errors`: the held-commit witness and early-HTTP negative control passed, and the only
failure was the canonical direct invariant assertion. `clean test` passed. `clean verify` executed
1,868 tests from the generated Surefire/Failsafe reports and ended with exactly one failure and zero
errors: `AmlT1ProofIT.provesDirectCommitBeforePositiveSmppResponse`, whose decisive obligation was
`t1-direct-commit-before-smpp-guard/CAUSAL_RELATION_VIOLATED`.

## Recommendation

Use **Outcome A** for issue #13 and PR #68: stock pinned Jasmin genuinely violates direct AML T1.
Record the deterministic falsification as the issue result. Do not close issue #13, merge the pull
request, change Jasmin/application behavior, or weaken the direct obligation as part of this
investigation.
