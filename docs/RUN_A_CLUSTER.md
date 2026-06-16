# Run your own Rez home cluster

A hosted Rez "home" is N `rez-node` processes behind a **non-sticky** load
balancer, sharing one Postgres (durable inbox + registries + settlement) and one
Redis (liveness + presence + rate-limits). A client reconnecting to any node
never loses messages. A self-hosted node and a hosted cluster are the same
concept — the cluster is just a horizontally-scalable home.

> **Status (Job 1, in progress).** The storage backend (durable inbox,
> registries, settlement), the migration runner, the Redis LivenessBus, AND the
> node↔backend wiring (`startRezNode.js` backend-select seam) are implemented and
> verified against real Postgres + Redis — a node now boots on Postgres when
> configured (`storage.backend: "pg"` or `REZ_STORAGE_BACKEND=postgres`), runs
> migrations on boot, and persists all durable state to Pg. The remaining
> integration is the delivery rework (persist-then-notify + `mailbox.cursorAck`)
> and wiring the LivenessBus into the running node (Redis is not yet consumed at
> runtime). See the repo plan.

## Quick start (reference deployment)

```bash
cd rez-node/deploy
cp .env.example .env        # edit PG_PASSWORD, ADVERTISED_HOST
docker compose up -d postgres redis
docker compose run --rm migrate      # apply schema (advisory-locked, idempotent)
docker compose up -d                 # nodes boot on Postgres (REZ_STORAGE_BACKEND=postgres)
```

`docker compose run --rm migrate` runs `rez-node migrate`, which applies the
ordered, forward-only SQL in `src/storage/pg/migrations/`. It is safe to run from
many nodes at once: a Postgres advisory lock means exactly one applies the DDL
and the rest no-op. A node also refuses to start against a DB migrated *past* the
version it ships (the schema-version gate), so an old binary can't corrupt a
forward-migrated cluster.

## Configuration

| Env | Purpose |
|---|---|
| `REZ_PG_URL` | Postgres connection string (durable state). Overrides `storage.pg.connectionString` in the config file. |
| `REZ_REDIS_URL` | Redis connection string (liveness/presence/rate-limit). **Not yet consumed at runtime** — reserved for the LivenessBus wiring (S2). |
| `REZ_STORAGE_BACKEND` | `postgres` (alias for `pg`) to use the shared backend; `fs` for single-node. Overrides `storage.backend`. Default: `fs`. |
| `REZ_ADVERTISED_HOST` | DNS-pinned hostname clients use; nodes announce it to the WAN |
| `REZ_NODE_ID` | Per-node identifier (presence keys, logs) |

Each node still has its OWN node keypair; there is no shared cluster key
(claimant-rooted delegation — a node serves an inbox by the claimant's own signed
delegation, not a broker). Third-party operators run their own nodes the same way.

## Scale out

Add a node by adding another `node*` service (same env, distinct `REZ_NODE_ID`)
and listing it in `nginx.conf` `upstream`. It picks up shared state with zero
manual migration — the durable inbox, claims, and settlement all live in the
shared Postgres; liveness flows over Redis.

## Upgrade / migrate

1. Ship the new node version with new `NNNN_*.sql` migrations.
2. `docker compose run --rm migrate` (or rely on a node's migrate-on-boot).
3. Roll nodes one at a time. The schema-version gate keeps a not-yet-upgraded
   node from booting against the new schema.

Migrations are forward-only. A breaking schema change ships as a new numbered
migration plus a compatible code path, never an in-place edit of an applied file.

## Backup / PITR

Postgres is the only durable tier, and even it is a **disposable cache** in the
trust model: identity (BIP39 seed), delivered history (on each device), and the
sender's outbound retry queue all survive total home loss. Still, run the home as
HA Postgres with WAL archiving / PITR so "all nodes down AND DB unrecoverable AND
no backups" stays a vanishing corner. Redis holds only soft state (liveness,
presence, rate-limit counters, the revocation list) — it is rebuildable and does
not need PITR.

## What is NOT stored here

Plaintext, keys, ratchet state — none of it. The node is ciphertext-only
(Shape A). The shared store holds encrypted inbox bodies and routing pointers
(claimant pubkey → inbox), never anything that can decrypt a message, and never
an `accountId → inboxId` map (cross-inbox unlinkability is a privacy primitive).
