# Hosted Rez operations

This is the minimum operating contract for a public shared home. The reference Compose is a
release-candidate topology; production may replace its Postgres, Redis, TLS, and scheduler layers
with managed equivalents while preserving the same trust boundaries.

## Release gate

Do not deploy a node revision unless all of the following are green on the same coordinated
`core/sdk/node/chat` release candidate:

1. core, SDK, node, and chat unit suites;
2. node tests with reachable real Postgres and Redis (no integration opt-out);
3. the hosted Compose black-box gate;
4. contract tests and the EIP-170 size budget when chain settlement changes;
5. `docker compose config`, image build, and vulnerability scanning in the image registry.

For a public node, keep `REZ_REQUIRE_KNOWN_RELAYS=1`. An empty bootstrap set is an isolated home,
not a Rez network participant, and is rejected before listeners start.

Keep release candidates invite-only first. Promote after a soak that includes random reconnects,
node restarts, Redis interruption, and continuous deposits with no loss below the acknowledged
cursor.

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
