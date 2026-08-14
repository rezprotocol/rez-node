# Hosted multi-tenant rez-node

Status: release-candidate implementation (`0.6.0-rc.2`).

This document is the canonical hosted-node architecture. Operational instructions live in
[RUN_A_CLUSTER.md](./RUN_A_CLUSTER.md) and [HOSTED_OPERATIONS.md](./HOSTED_OPERATIONS.md).

## Trust model

The hosted node is untrusted ciphertext transport and storage. It may hold its own mesh identity,
encrypted mailbox bodies, opaque inbox ownership keys, routing state, durable device cursors, and
settlement/account-authority journals. It must never hold account signing private keys, X3DH
private material, ratchet keys, message keys, or plaintext application payloads.

Encryption/decryption and peer-link ratchets live in `rez-sdk`/`rez-chat`. The node accepts already
encrypted deposits and emits encrypted mailbox events. Location is only a `wsUrl` choice to the
application; local and hosted nodes use the same signed-challenge protocol.

The hosted web application runs that client-owned runtime directly in the browser. It is not a
server-rendered chat session and the cluster does not impersonate an account. Desktop and future
mobile shells are alternative clients over the same boundary; none is required for browser use.

## Authority and registration

Sessions prove the account root key or a root-authorized delegated device key. Delegated admission
is checked against the home account's current revocation state. Inbox registration is open and
self-authenticating: a claimant signs the random inbox ID and a claimant→node routing delegation.
The node never mints account authority.

Account-authority publication is root-signed only. Delegated devices can drain and publish prepared
records but cannot author their own validity state. Durable epoch floors prevent a holder that has
seen a newer authority state from accepting rollback.

## Multi-tenancy and privacy

Postgres keys every account-facing KV/object row by owner and every mailbox operation by an opaque,
random inbox ID. A session may claim multiple inboxes under unlinkable claimant keys. The node does
not maintain an account-ID→inbox map.

The shared-node gate proves two different tenants cannot read one another's inboxes. Cluster-wide
budgets bind open registration and outbox leases; durable inbox, DHT publisher, frame, queue, and
deposit caps bound storage and memory abuse.

## Cluster consistency

Postgres is the durable system of record for inbox events, device cursors, claims, account mutation,
publication obligations, rate budgets, and settlement. Nodes use atomic database operations rather
than process-local read/modify/write state.

Redis carries only sharded “inbox has new data” notifications and ephemeral presence. A depositing
node persists first, then notifies. A socket-holding node drains from Postgres. Lost Redis messages
therefore delay delivery until reconnect; they cannot lose an acknowledged event.

Each node has a distinct mesh key and public relay endpoint. Client WebSockets sit behind a
non-sticky TLS load balancer. Relay TCP identities do not sit behind anonymous round-robin because
the advertised endpoint is cryptographically bound to one node key.

## Lifecycle and failure behavior

- Postgres failure makes `/ready` fail closed.
- Redis failure is reported as degraded; reconnect catch-up remains available.
- Migrations are forward-only and advisory-locked. Semantic writer changes require a full drain.
- Revocation publication, pending ceremonies, inbox catch-up, and authority publication all have
  startup/reconnect/scheduled recovery readers; mutation triggers are only the fast path.
- Vault lock is a client concern and fails closed over the local chat runtime. Hosted nodes never
  receive the vault contents.

## Deliberate beta boundaries

- Core encrypted messaging is free. Paid services use off-chain service credits in beta; chain mode
  is testnet-only.
- Redis is a single logical soft-state tier in the reference deployment; production can use a
  managed HA equivalent.
- Online rotation of the shared at-rest encryption key is not implemented. Replacement is a
  coordinated maintenance operation; client-owned identity and plaintext remain outside the home.
- Cross-device ratchet-history synchronization is not provided. Each authorized device maintains
  its own device session and durable cursor.

These boundaries affect liveness and operations, not the rule that the shared node holds no
plaintext or account signing authority.
