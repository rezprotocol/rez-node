# Hosted Rez operations

This is the minimum operating contract for a public shared home. The reference Compose is a
release-candidate topology; production may replace its Postgres, Redis, TLS, and scheduler layers
with managed equivalents while preserving the same trust boundaries.

## Release gate

Do not deploy a node revision unless all of the following are green on the same coordinated
`core/sdk/node` release candidate. Network acceptance is independent of the
Chat, iPhone and native client release gates:

1. core, SDK and node unit suites;
2. node tests with reachable real Postgres and Redis (no integration opt-out);
3. the hosted Compose black-box gate;
4. real network recovery against an isolated TLS topology: node process death,
   Redis interruption, Postgres unavailability/restart and backup restoration;
5. contract tests and the EIP-170 size budget when chain settlement changes;
6. `docker compose config`, image build, and vulnerability scanning in the image registry.

Chat releases additionally run the web gate with a clean test origin/profile so
it cannot pass on stale browser state. This is a client acceptance gate:

```bash
REZ_HOSTED_CHAT_URL=https://chat.rezprotocol.io \
CHROME_PATH=/path/to/chrome \
npm -w rez-chat run test:e2e:hosted-web
```

It creates two independent browser accounts, verifies PWA registration, completes a direct invite,
disconnects the recipient, sends while offline, reconnects, and requires the exact plaintext to be
decrypted and rendered after durable catch-up.

For a public node, keep `REZ_REQUIRE_KNOWN_RELAYS=1`. An empty bootstrap set is an isolated home,
not a Rez network participant, and is rejected before listeners start.

Keep release candidates invite-only first. Promote after a soak that includes random reconnects,
node restarts, Redis interruption, and continuous deposits with no loss below the acknowledged
cursor.

The network-only hosted gate is `npm -w rez-node run test:hosted`, with
`REZ_HOSTED_E2E_URL` pointing to the isolated TLS load balancer. Test credentials
and a trusted test CA belong to that fixture, never to production. The extended
recovery mode (`REZ_HOSTED_RECOVERY_ROOT`) deliberately accepts only a
`/tmp/rez-network-proof-<digits>` Compose fixture on `wss://localhost`; it kills
test nodes and restores the test database, and must not target production.

Postgres connection acquisition is bounded at 3 seconds; ordinary statements
at 5 seconds server-side and 6 seconds client-side. Backend pool overrides may
increase these for controlled administrative work. Readiness uses a 2-second
query timeout and a 1-second cache. A failed transaction discards its connection
rather than returning ambiguous session state to the pool. Redis command and
connection budgets are 2 seconds. Redis loss degrades live wakeups while durable
delivery remains available; subscriber reconnection drains registered inboxes
from Postgres, including notifications missed during the outage.
Filesystem readiness creates, writes, syncs and removes a private temporary
probe on the node's storage volume; path, capacity and durability errors fail
readiness rather than advertising unconditional health.

Node identity bootstrap uses strict durable reads. Unreadable, malformed,
partially populated or mismatched saved mesh keys stop startup without
overwriting them. Restore the original identity volume and its protection;
generating replacement keys is not a recovery strategy for identity-derived
encrypted filesystem data. Legacy identities without any mesh key fields still
receive their one-time mesh key upgrade.

The runtime image copies only network package runtime sources. Workspace
manifests are present for the root lockfile install; application sources and
local secret files are not copied. Unused global package managers and their
bundled dependencies are removed from the shipped runtime.

The filesystem inbox lifecycle sweeper runs before listener startup and on the
existing mesh maintenance tick. It derives expiry from durable state, coalesces
overlapping sweeps, retries durable pending purges, and isolates per-inbox
failures. Shutdown unsubscribes it and waits for active cleanup before closing
storage. No mobile runtime drives server reclamation.

Outbound queue capacity refuses new work with retryable `RESOURCE_LIMIT` and
preserves accepted packets. Queue state, retry count and completion are updated
under one process-local mutation lock; storage failures never acknowledge new
custody. Queue TTL/attempt exhaustion and best-effort live status notifications
remain the documented transport contract, not recipient delivery proof.

## Alerts

Page immediately:

- any node returns `/ready` `503` for two consecutive checks;
- all nodes are absent from the load balancer;
- Postgres connection saturation, disk exhaustion, replication lag, or WAL archival failure;
- durable inbox append failures or a sustained increase in `rez_errors_total`;
- TLS certificate expiry under 14 days;
- both relay identities have zero healthy mesh peers for five minutes.

Ticket/degrade:

- `/ready` reports `redis: false` for five minutes;
- retry/outbox depth grows for fifteen minutes;
- persistent rate-limit saturation by one source or claimant;
- durable inbox pruning falls behind its retention/cap envelope.

Aggregate metrics contain no tenant or inbox labels. Do not add high-cardinality account IDs,
inbox IDs, public keys, lease tokens, or ciphertext metadata to metrics or logs.

## Postgres backup and restore

Use encrypted storage, daily base backups, continuous WAL archiving, and point-in-time recovery.
Backups and WAL must use a different failure domain and retention policy than the primary.

Run a restore drill at least quarterly:

1. restore the latest base backup plus WAL into an isolated network;
2. start one release-matched node with Redis disabled and public listeners blocked;
3. verify migration version, row counts, encrypted-row readability, authority epochs, device
   cursors, and propagation outbox leases;
4. run the hosted black-box test against the isolated restore;
5. destroy the drill environment and record recovery point/time achieved.

Redis is not restored. It is soft state and must rebuild from live sessions and Postgres.

## Secrets

Keep Postgres, Redis, TLS, and the shared storage key in a secret manager. Mount secrets read-only;
never put real values in `.env`, logs, images, backups without encryption, or support bundles.
The TLS private key must remain non-public: own it by the configured `TLS_READ_GID`, use mode
`0640`, and add that numeric group to the non-root node containers as the reference Compose does.
Automated renewal must also install the renewed files at the configured bind-mount paths, preserve
those permissions, and roll `node1`, `node2`, then `lb`, waiting for health after each replacement.
A Certbot timer without that deploy hook renews files on disk but can leave long-running containers
serving the old certificate.

Loss of the shared storage key makes cached rows unreadable. Compromise requires replacing the
cluster and treating the old encrypted cache as exposed ciphertext; online re-encryption is not yet
implemented. Account identity and E2EE keys remain client-owned, so this does not expose plaintext.

## Incident behavior

- **Postgres unavailable:** readiness fails; stop sending traffic to the affected node. Do not fall
  back to filesystem storage.
- **Redis unavailable:** keep serving. Live cross-node wakeups degrade, while reconnect drains the
  durable log. Alert and restore Redis.
- **One node compromised:** remove it, revoke its infrastructure credentials and TLS material,
  preserve logs, and replace its node identity volume. Other nodes retain the home state.
- **Storage key suspected compromised:** take a coordinated maintenance window and replace the
  cluster/key. Do not silently continue under a known-exposed key.
- **Mixed writer versions:** drain immediately. Do not rely on the startup schema gate to stop an
  already-running old process.

## Capacity and abuse

Open inbox registration is intentionally permissionless but bounded by cluster-wide claimant/IP
budgets, per-claimant inbox ceilings, per-inbox durable caps, per-publisher DHT quotas, frame/queue
caps, and per-source deposit limits. Monitor denial rates and database growth. Raising any ceiling
is a security change and requires load testing plus a touched-path audit.
