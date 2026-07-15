# Run your own Rez home cluster

A hosted Rez "home" is N `rez-node` processes behind a **non-sticky** load
balancer, sharing one Postgres (durable inbox + registries + settlement) and one
Redis (liveness + presence + rate-limits). A client reconnecting to any node
never loses messages. A self-hosted node and a hosted cluster are the same
concept — the cluster is just a horizontally-scalable home.

> **Status (Job 1, in progress).** Implemented and verified against real
> Postgres: the shared **storage** backend (KV/object/mailbox + durable inbox),
> the migration runner, the `startRezNode` backend-select seam, and — wired into
> the running pg node — the **atomic inbox-claim registry** (`PgInboxClaimRegistry`)
> and **atomic settlement** (`PgSettlementProvider`). So claims and payments are
> cluster-correct. **NOT yet cluster-safe:** message **delivery** — the
> `LivenessBus` exists as a class but is not consumed at runtime, and delivery
> still pushes over the local socket rather than persist-then-notify against the
> durable home log (`mailbox.cursorAck`), so a client reconnecting to a *different*
> node can still miss buffered mail (S2). Also, `rez-node start` still needs a
> config **file** the reference compose does not yet mount or generate (S5). Run a
> **single** node until S2 lands. See the plan.

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

> **Current runtime is single-node for message delivery.** Shared storage,
> inbox claims (`PgInboxClaimRegistry`), and settlement (`PgSettlementProvider`)
> are now cluster-correct. But **delivery** is not: the `LivenessBus` is not
> consumed at runtime and delivery is socket-local, not persist-then-notify, so a
> second node would not reliably deliver mail buffered while a client was on
> another node. Run **one** node until S2 (LivenessBus + persist-then-notify) lands.

The *intended* shape: add a node by adding another `node*` service (same env,
distinct `REZ_NODE_ID`) and listing it in `nginx.conf` `upstream`. Once delivery
goes persist-then-notify over the durable home log + LivenessBus (S2), a new node
picks up shared state with no manual migration — durable inbox, claims, and
settlement in shared Postgres, liveness over Redis.

## Upgrade / migrate

1. Ship the new node version with new `NNNN_*.sql` migrations.
2. `docker compose run --rm migrate` (or rely on a node's migrate-on-boot).
3. Roll nodes one at a time. The schema-version gate keeps a not-yet-upgraded
   node from booting against the new schema.

Migrations are forward-only. A breaking schema change ships as a new numbered
migration plus a compatible code path, never an in-place edit of an applied file.

> **⚠️ REQUIRED for migration `0014_canonical_cert_ids` — DRAIN, do not roll.**
> Most migrations are additive and safe under the one-at-a-time roll above, because the
> schema-version gate blocks a *not-yet-upgraded* node from *booting*. Migration `0014`
> is different: it changes the **device.revoke semantics** (a revoke now touches only the
> target device's own bound cert, not an arbitrary caller-supplied cert). The version gate
> only fences a node at **startup** — it cannot stop an **already-running** old node from
> continuing to write under the old semantics against the shared DB. `0014`'s DB `CHECK`
> constraints fence bad *syntax*, but NOT the semantic change. Therefore, for `0014` (and
> any future migration that changes a shared-DB writer's semantics):
>
> 1. **Drain / stop ALL running node processes** (single-writer-version), OR fence writes.
> 2. Run the migration.
> 3. Bring nodes back up on the new version only.
>
> Do **not** apply `0014` against a live mixed-version cluster. Deployment tooling SHOULD
> enforce this drain step for semantic-change migrations rather than relying on this note.

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
