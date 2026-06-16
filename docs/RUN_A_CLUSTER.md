# Run your own Rez home cluster

A hosted Rez "home" is N `rez-node` processes behind a **non-sticky** load
balancer, sharing one Postgres (durable inbox + registries + settlement) and one
Redis (liveness + presence + rate-limits). A client reconnecting to any node
never loses messages. A self-hosted node and a hosted cluster are the same
concept — the cluster is just a horizontally-scalable home.

> **Status (Job 1, in progress).** Implemented and verified against real
> Postgres: the shared **storage** backend (KV/object/mailbox + durable inbox),
> the migration runner, the Redis `LivenessBus` *class*, and the `startRezNode`
> backend-select seam — a node boots on Postgres and persists durable **storage**
> there. **NOT yet wired into the running node:** the inbox-claim registry and
> settlement still use single-process implementations (multi-node would clobber
> claims / overdraft — run a **single** node for now); the `LivenessBus` is not
> consumed at runtime; the delivery rework (persist-then-notify +
> `mailbox.cursorAck`) is pending; and `rez-node start` still needs a config
> **file** the reference compose does not yet mount or generate (S5). See the plan.

## Quick start (reference deployment)

```bash
cd rez-node/deploy
cp .env.example .env        # set PG_PASSWORD, REDIS_PASSWORD, REZ_STORAGE_ENCRYPTION_KEY, ADVERTISED_HOST
docker compose up -d postgres redis
docker compose run --rm migrate      # apply schema (advisory-locked, idempotent)
# node services additionally need a mounted/generated config file (see "Node
# config" below) — that deploy glue is still S5. Today: run a single node from a
# config file you supply (storage.backend=pg + a node-local dataDir volume).
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
| `REZ_STORAGE_ENCRYPTION_KEY` | **Required in pg mode.** Base64 of 32 raw bytes — the at-rest encryption key **shared by every node** in the cluster (so nodes can read each other's encrypted rows). Generate with `openssl rand -base64 32`. **SECRET** — use a secret manager; never log it or commit it. fs mode derives its key from the node identity and needs none. |
| `REZ_ADVERTISED_HOST` | DNS-pinned hostname clients use; nodes announce it to the WAN. **No env bridge yet** (S5) — set `relay.advertisedHost` in the config file. |
| `REZ_NODE_ID` | Per-node identifier (presence keys, logs). **No env bridge yet** (S5). |

**Node identity is node-local.** Each node's mesh keypair (`substrate:nodeIdentity:v1`) is stored on that node's **local filesystem** (`storage.dataDir`), never in shared Postgres — otherwise two nodes would boot with the same identity. Give each node its own persistent `dataDir` volume, or supply a complete per-node `config.node.identity` (including `nodeKeyId`/`nodePublicKeyB64`/`nodePrivateKeyB64`) so the identity is stable across restarts. The at-rest storage key is **decoupled** from the node identity in pg mode, so a regenerated identity never makes shared storage unreadable.

Each node has its OWN node identity/signing keypair (node-local). There is no
shared node *signing* key — claimant-rooted delegation means a node serves an
inbox by the claimant's own signed delegation, not a broker or a cluster signing
key. (This is distinct from the shared at-rest **storage** key above, which every
cluster node *does* share so they can read the same encrypted rows.) Third-party
operators run their own nodes the same way.

## Scale out (target shape — NOT yet runtime-safe)

> **Current runtime is single-node.** The shared **storage** (durable inbox,
> KV/object) lives in Postgres, but the inbox-claim registry, settlement, and the
> `LivenessBus` are **not yet wired into the running node** (see the status block
> above and the startup warning). Adding a second `node*` today would clobber
> claims and overdraft shared wallets. Run **one** node until those bridges land.

The *intended* shape: add a node by adding another `node*` service (same env,
distinct `REZ_NODE_ID`) and listing it in `nginx.conf` `upstream`. Once the
cluster registries + LivenessBus are wired, it will pick up shared state with no
manual migration — durable inbox, claims, and settlement in shared Postgres,
liveness over Redis.

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
