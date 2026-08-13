# Run a hosted Rez home cluster

Status: release-candidate implementation (`0.6.0-rc.1`).

A hosted home is two or more independently identified `rez-node` processes behind a non-sticky
TLS load balancer. Nodes share encrypted Postgres state and Redis liveness signals. Message bodies
remain end-to-end encrypted on clients; the cluster stores and routes ciphertext only.

## What is proven

The mandatory hosted gate builds the exact image and boots this topology, then proves:

- TLS client ingress reaches two distinct node identities through round-robin balancing;
- a deposit on one node wakes a socket held by another node through Redis;
- reconnecting to a different node drains the shared durable Postgres log;
- a cursor acknowledged on one node prevents redelivery on the other;
- a second tenant cannot read the first tenant's inbox;
- replacing both node processes without restarting nginx preserves two-node ingress;
- Postgres-backed claims, revocation, publication outbox, settlement, and migrations run in the
  default node suite against real backends.

## Quick start

Requirements: Docker Compose, a DNS name, a PEM certificate/private key valid for that name, and
host ports for the two relay identities. A public deployment also needs at least one authenticated
bootstrap relay from the published seed set; startup fails if that set is empty.

```bash
cd rez-node/deploy
cp .env.example .env
# Fill every secret/path in .env. Generate the storage key with:
# openssl rand -base64 32
set -a
source .env
set +a
docker compose up --build --detach --wait
curl --fail --cacert "$TLS_CERT_PATH" "https://$ADVERTISED_HOST:${LB_PORT:-8443}/ready"
```

The two relay ports are intentionally separate. A relay identity cannot sit behind an anonymous
TCP round-robin endpoint: peer authentication binds the advertised endpoint to that node's key.
Client WebSockets can be round-robin because every node shares the account home state.
The reference nginx resolves node service addresses continuously, so replacing a node container
does not pin the load balancer to its old address.

To stop without deleting durable data:

```bash
docker compose down
```

Deleting the `pgdata` volume destroys the hosted cache. Never use `down --volumes` outside an
intentional disposable environment.

## Configuration and secrets

| Variable | Requirement |
|---|---|
| `PG_PASSWORD` | Secret-manager value; no default. |
| `REDIS_PASSWORD` | Secret-manager value; Redis is private-network-only. |
| `REZ_STORAGE_ENCRYPTION_KEY` | Exactly 32 random bytes, base64. Shared by trusted cluster nodes. |
| `ADVERTISED_HOST` | Public DNS name used for WSS and relay descriptors. |
| `TLS_CERT_PATH` / `TLS_KEY_PATH` | Host paths mounted read-only into nginx and both relay listeners. |
| `LB_PORT` | Public WSS port, default `8443`. |
| `NODE1_RELAY_PORT` / `NODE2_RELAY_PORT` | Distinct public TLS relay ports. |
| `REZ_KNOWN_RELAYS_JSON` | JSON array of authenticated public bootstrap relays. |
| `REZ_REQUIRE_KNOWN_RELAYS` | Keep `1` in production; `0` is only for isolated tests. |

Each node persists its mesh identity under its own named volume. Never share `node1data` and
`node2data`. The storage encryption key is deliberately separate from those identities so every
node can read the same rows.

The gateway accepts `X-Forwarded-For` only from `172.30.0.0/24`, the explicitly allocated Compose
edge network. If the topology changes, update `trustedProxyCidrs` to the narrow proxy network; do
not add the public internet or use a blanket “trust proxy” switch.

## Health and metrics

- `GET /health` is process liveness.
- `GET /ready` checks Postgres and reports Redis independently. Postgres failure returns `503`.
  Redis failure returns `200` with `degraded: true` because reconnect catch-up remains lossless.
- `GET /metrics` is Prometheus text with aggregate process/node counters. Nginx blocks it at the
  public edge; scrape `node1:8787/metrics` and `node2:8787/metrics` from the private network.

See [HOSTED_OPERATIONS.md](./HOSTED_OPERATIONS.md) for alerts, backups, restore drills, incident
response, and release procedure.

## Upgrade and migration

Migrations are ordered, forward-only, advisory-locked, and version-gated. The `migrate` one-shot
runs before nodes. For additive migrations, replace nodes one at a time only after the coordinated
core/sdk/node release has passed CI.

Any migration that changes shared-writer semantics requires a full drain:

1. remove the load balancer from service and stop every node;
2. run `docker compose run --rm migrate` with the new image;
3. start only the new node version and wait for `/ready`;
4. run the hosted black-box proof before restoring public traffic.

Migration `0014_canonical_cert_ids` is one such drain-required migration. The startup schema gate
cannot fence an already-running old writer.

## Trust boundary

Postgres contains encrypted inbox bodies, opaque inbox ownership keys, device cursors, routing
delegations, and settlement/account-authority journals. Redis contains only rebuildable liveness,
presence, and rate-limit state. Neither holds account private keys, X3DH private material, ratchet
keys, or plaintext messages.
