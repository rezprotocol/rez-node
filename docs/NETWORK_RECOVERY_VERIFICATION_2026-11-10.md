# Network security and recovery verification

Verified locally on November 10, 2026. The fixes and focused audit are complete.
Changes remain uncommitted and undeployed. This verifies a candidate for an
invite-only rollout; production acceptance still requires deployment checks and
a sustained soak under the operating contract in `HOSTED_OPERATIONS.md`.

The iPhone, Swift and Chat work was left untouched. Network acceptance uses
Core, SDK and Node independently of those client releases. Existing storage and
client changes in the checkout were preserved; the tests cover the combined
working tree, not a clean commit or the currently deployed network.

## Fixes

- Outbound admission refuses new work at capacity instead of silently evicting
  accepted packets. Serialized mutations preserve capacity and completion
  invariants under concurrent calls and storage failures.
- Polling and route discovery share one delivery attempt through its durable
  update. Failed background updates are reported and do not crash the node or
  starve other packets. This does not promise exactly-once remote delivery.
- Filesystem inbox expiry and interrupted purges recover at node startup and
  on the existing mesh maintenance cadence. Concurrent sweeps coalesce, one
  failed purge does not starve other inboxes, and shutdown waits for cleanup.
- PostgreSQL acquisition, statement and query budgets bound outages. Failed
  transaction clients are discarded. Readiness fails closed and backend
  failures produce retryable, sanitized errors on existing client sockets.
- Redis absence permits degraded startup and bounded shutdown. Reconnection
  triggers durable drains for registered inboxes, recovering missed live
  notifications without requiring client reconnection.
- Filesystem readiness actually writes and syncs a private temporary probe.
  Saved node identity uses strict reads and validates the keypair before reuse;
  corrupt, incomplete or mismatched saved keys are never silently replaced.
- Touched routing, inbox, gateway, crypto and recovery diagnostics omit tenant
  identifiers, request data and backend details, including debug rejection
  paths. The SDK compatibility module now re-exports the canonical Core secure
  channel implementation instead of maintaining a duplicate.
- The image copies runtime paths explicitly, excluding repository deployment
  files and application sources. Removing unused global package managers
  eliminates their vulnerable bundled dependencies from the runtime.

## Verification

| Gate | Result |
| --- | --- |
| Core full suite | 615 passed, 0 failed, 0 skipped |
| SDK full suite | 538 passed, 0 failed, 0 skipped |
| Node full suite, real PostgreSQL and Redis, hosted TLS and live mesh enabled | 1,399 passed, 0 failed, 0 skipped |
| Final hosted recovery drill | 5 passed, 0 failed, 0 skipped: parent topology gate plus four failure scenarios |
| Node 22 / Linux identity, queue, retry and privacy regressions | 33 passed, 0 failed, 0 skipped |
| Final runtime image, Trivy 0.76.2 | 0 reported HIGH or CRITICAL vulnerabilities |
| Runtime source comparison | All 630 shipped runtime files exactly match the checkout |
| Core, SDK and Node whitespace checks | Passed |

The local full-suite host used Node 25; the shipped image used Node 24. The
separate compatibility regressions used Node 22. No new project dependency or
tooling configuration was introduced.

The recovery fixture used two separately identified nodes behind a non-sticky
TLS load balancer, real PostgreSQL and Redis, authenticated sessions and signed
claims. TLS verification remained enabled with a fixture CA. All disruptive
operations targeted disposable local services.

The four recovery scenarios verified:

1. SIGKILL of both node processes preserves exact retained ciphertext and the
   acknowledged device cursor after restart.
2. Redis interruption preserves durable deposits. Restoring Redis delivers
   notifications missed during the outage to the original client socket.
3. Pausing PostgreSQL fails readiness within the test's ten-second bound,
   refuses new client admission, and returns retryable `SERVICE_UNAVAILABLE`
   on an existing socket. Unpause and an actual PostgreSQL restart recover
   committed data and cursor state without restarting the nodes.
4. `pg_dump` and `pg_restore` into another database, followed by recreating
   nodes with their original identity volumes and storage key, preserve exact
   ciphertext, cursor state and tenant isolation.

Additional regressions kill a child process after an encrypted filesystem
queue commit, reopen the store and compare exact bytes and retry state. They
also interrupt a production-wired inbox purge and finish it on restart while
refusing reuse of the reclaimed generation. The previously skipped idle TCP
test now uses its existing injected clock; the live four-node relay fixture
uses self-certifying identities and the current typed custody result.

### Candidate and evidence

Final local image: `rez-network-proof:20261110`

Image ID:
`sha256:e6e50f0d8e29202010ee5bb1b0f2b89b6f3998b5b010e4feb960d2151ff70d22`

Git parents before these uncommitted changes:

| Package | HEAD |
| --- | --- |
| Core | `4b158bd93fba90579cd3af2f12e9515faced5a84` |
| SDK | `2e6b6b9b5945df3ee78a43d538113ae8bc0d4011` |
| Node | `8971c9a7d39bb3433bcf62a6e7a2bd4e89a316b6` |

Evidence directory: `/tmp/rez-network-proof-20261110/`.
Retained logs: `core-final.log`, `sdk-final.log`, `node-final-verified.log`,
`recovery-final-verified.log`, `node22-final-verified.log`,
`image-vulnerabilities-final.json`, `runtime-source-manifest.json` and
`runtime-image-manifest.json`. These temporary files are local evidence, not
published release artifacts. Disposable services and database volumes were
removed after verification; the final image and evidence were retained.

The final Node command was:

```sh
REZ_PG_TEST_URL=postgres://postgres:rez-network-test@127.0.0.1:54953/postgres \
REZ_REDIS_TEST_URL=redis://127.0.0.1:54957 \
RUN_LOCAL_MESH_E2E=1 REZ_HOSTED_E2E_URL=wss://localhost:58443/ws \
NODE_EXTRA_CA_CERTS=/tmp/rez-network-proof-20261110/cert.pem npm test
```

The separate failure drill added
`REZ_HOSTED_RECOVERY_ROOT=/tmp/rez-network-proof-20261110` and ran
`npm run test:hosted`. It requires recreating the disposable fixture and must
never target production.

Preservation checks: the Chat diff retained SHA-256
`495890ca3878a4cb52434a963d5ff75b54d158e1b8ae344477a360e401e283b9`;
the SDK native/IndexedDB source diff retained SHA-256
`c118f23037b130363d922eca89dcb23cbfae03ce3a6b6057fbaba085373cec01`.

## Focused audit

### Files changed for this work

Paths below are relative to their owning repository. Other pre-existing changes
remain outside this change inventory.

- Core: `src/protocol/OutboundQueueEntryV1.js`,
  `src/e2ee/SecureChannelManager.js`, `test/outbound-queue.record.test.js`,
  `docs/RECEIPTS_AND_DELIVERY_STATES.md`.
- SDK: `src/e2ee/SecureChannelManager.js` and the diagnostic branches of
  `src/peer-link/PeerLinkService.js`.
- Node bootstrap: `src/app/startRezNode.js`, `src/app/bootstrapRelay.js`,
  `src/app/createStorageBackend.js`.
- Node queue and lifecycle: `src/gateway/PersistentOutboundQueue.js`,
  `src/gateway/RetryScheduler.js`, `src/gateway/GatewayLoop.js`,
  `src/gateway/MeshBootstrap.js`, `src/gateway/MeshCoordinator.js`,
  `src/inbox/InboxLifecycleSweeper.js`, `src/identity/NodeIdentity.js`.
- Node transport and backends: `src/storage/pg/PgConnection.js`,
  `src/relay/LivenessBus.js`, `src/relay/createLivenessBus.js`,
  `src/relay/InboxRouter.js`, `src/network/RelayConnectionPool.js`,
  `src/ws/WsGatewayServer.js`, `src/protocol/GatewaySession.js`,
  `src/protocol/RelayDepositRouter.js`,
  `src/protocol/handlers/MailboxHandler.js`,
  `src/protocol/handlers/InboxClaimHandler.js`,
  `src/routing/GossipRouteResolver.js`, `src/routing/dht/DhtProtocol.js`,
  `src/routing/dht/DhtRouteAnnouncer.js`,
  `src/routing/dht/DhtRouteResolver.js`.
- New Node tests: `test/gateway.retry-scheduler.test.js`,
  `test/gateway.queue-recovery.test.js`,
  `test/inbox.production-reclamation.test.js`,
  `test/node.backend-outage.test.js`, `test/network.log-privacy.test.js`.
- Updated Node tests: `test/gateway.outbound-queue.test.js`,
  `test/outbound-queue.status-pins.test.js`,
  `test/identity.node-identity.test.js`,
  `test/startRezNode.backend-select.test.js`,
  `test/storage.pg-connection.test.js`, `test/e2e.hosted-compose.test.js`,
  `test/e2e.local-mesh.durable-record.test.js`,
  `test/network.tcp.connection-manager.test.js`,
  `test/architecture.atlas-readiness.test.js`,
  `test/architecture.no-extension.compat.test.js`.
- Node delivery and operating artifacts: `deploy/Dockerfile`,
  `docs/HOSTED_NODE_DESIGN.md`, `docs/HOSTED_OPERATIONS.md` and this receipt.

### Audit results

Specs checked: repository `AGENTS.md`, Node `HOSTED_NODE_DESIGN.md` and
`HOSTED_OPERATIONS.md`, Core `RECEIPTS_AND_DELIVERY_STATES.md`, `STORAGE_MODEL.md`
and `adr/ADR-RELAY-IDENTITY.md`. Queue capacity and subscriber recovery prose
was updated to match behavior. The hosted design now names Core as the
canonical crypto implementation exposed through SDK.

Canonical owners confirmed: Core owns the queue record and shared crypto;
Node owns queue scheduling, durable server state, backend orchestration,
identity enforcement and RPC validation; SDK remains the application facade.
No protocol or server recovery responsibility was added to Chat, UI or native
clients. Existing record formats and generic transport dispatch were retained.

Duplicate/split-brain findings: removed the duplicate SDK secure-channel
implementation. Mesh maintenance has independent subscriptions rather than a
single hook that one subsystem could replace. Queue admission and mutations
have one process-local serialization owner; PostgreSQL remains the cluster
durable owner and Redis remains soft state.

Boundary/SSOT findings: no new cross-package import violation or second wire
schema was found in changed code and its direct dependencies. The queue record
now obeys the sealed RRecord contract with defensive ciphertext reads while
preserving its persisted JSON schema. Node key IDs still derive through Core.

God-class findings: existing `GatewaySession`, `InboxRouter` and
`PeerLinkService` remain large. This change adds no new responsibility to those
classes; their touched branches cover error handling or diagnostics. No new
god class was introduced. Broader decomposition was not attempted.

Dead code removed: SDK crypto duplicate, queue oldest-entry eviction helpers
and unused index prefix, obsolete single mesh-hook path and stale diagnostics.
The reserved `receiptInboxId` field remains for wire compatibility, as required
by the delivery-state spec.

Tests added/updated: new queue-record, retry-race, crash/storage-recovery,
production reclamation, backend-outage and privacy regressions; expanded TLS
recovery gate; updated capacity/status, identity, readiness, PostgreSQL client,
mesh, idle TCP and architecture guard tests. Tests exercise actual crypto,
durable files and real backend/process failures where those guarantees matter.

Pass/fail: all final listed gates passed. Earlier fixture failures from an
incorrect database name and stale test API/identity assumptions were corrected;
the final suites did not disable or skip readiness gates.

## Remaining operating limits

- The deployed revision, real credentials, TLS renewal, known-relay bootstrap,
  alerts, production backups and sustained load/soak still need live validation
  before invitations. No deployment or release publication occurred here.
- The queue lock coordinates one node process, not multiple processes sharing
  its filesystem. TTL and retry exhaustion remain explicit expiry conditions.
  Ambiguous acknowledgements may retry delivery; live outbound status is still
  best effort and is not proof that a recipient decrypted a message.
- Node mesh identity bootstrap remains protected by filesystem/volume access,
  not an independently encrypted credential vault. Losing the identity or
  configured at-rest key can make retained ciphertext unrecoverable. The
  existing storage-key custody and online rotation limitations remain.
- This is focused implementation and recovery verification, not a fresh
  whole-protocol cryptographic audit, exhaustive log inventory, DDoS/capacity
  certification, long-duration soak or third-party security review. Contract,
  chain, mobile and application-release gates were outside this change.
