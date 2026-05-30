# Hosted multi-tenant rez-node — design notes

Status: design draft / pre-implementation
Audience: rez maintainers
Date: 2026-05

## 0) Why

Today every user runs a rez-node in-process inside the desktop app. That gives strong privacy (the node never sees plaintext) and zero ops cost (no servers to run), but limits the product:

- **No sync across the user's own devices** without the desktop running on each of them.
- **No "hosted Rez" experience** for users who don't want to run the desktop locally (mobile, web).
- **No always-on inbox** — if your desktop is closed, your peers must keep retrying delivery (today that already works via SDK queueing, but it's brittle and expensive).

A hosted rez-node would mean: a server we (or a third party) runs that holds your **node identity**, hosts your **mailboxes**, participates in the **mesh routing fabric**, and acts as your **peer-link endpoint** — without ever holding plaintext or any key material that lets it decrypt.

This document scopes that out.

## 1) Trust model — non-negotiables

The goal is "hosted convenience without giving up E2EE." The hosted node:

- **MAY hold:**
  - The node's identity keypair (for being addressable on the mesh and signing routing announcements).
  - The owner's `accountId` and account public key.
  - **Encrypted** mailbox bodies (ciphertext only).
  - Peer-link metadata (peerLinkId, remoteAccountId, bindingTargetInboxId, state) — pointers, not plaintext.
  - Mesh routing state (peer node IDs, last-discovery timestamps, seed reachability).

- **MUST NOT hold:**
  - The owner's account-level signing private key. (That key authorizes `accountIdentity → nodeIdentity` binding; if compromised, the attacker can re-bind the account to a node they control. It stays on the user's device.)
  - **X3DH long-term identity private keys, signed prekey private keys, or one-time prekey private keys** for any peer-link the owner participates in. These give the holder the ability to derive ratchet keys and decrypt all past + future messages on any session that uses them.
  - **Double-ratchet root keys, chain keys, or message keys** for any peer-link.
  - Plaintext payloads of any chat message, group op, profile broadcast, or file chunk.
  - Avatar files in plaintext (these flow as encrypted file payloads today; the node sees ciphertext only).

The boundary is sharp: **the node is a transport + routing facility. Anything that could decrypt a peer-to-peer payload stays on the user's owned device.**

## 2) Where E2EE state lives in this model

Today, the SDK's `SecureChannelManager` runs in-process and reaches into local storage for:
- X3DH identity / signed prekey / one-time prekeys
- Ratchet sessions (root key, sending/receiving chain, header keys)
- Replay-protection windows

When the node is hosted, that state has to move out of the node's process. Two viable shapes:

### Shape A — "Thin client / fat hosted node, encrypted client state"

The user's client (desktop / mobile / web) is the only thing that ever holds plaintext keys. The hosted node runs the SDK's transport and routing layers but **not** the SecureChannelManager. The client connects via a WebSocket gateway, performs E2EE locally on every message, and the hosted node only sees and stores the encrypted envelopes.

**Pros:**
- Cleanest trust boundary. Nothing on the server can decrypt past messages even if seized.
- Same trust model as Signal-style designs.

**Cons:**
- Client must be online to encrypt outbound messages. (No "offline send" via the hosted node — the message sits on the client until next online.) This is a regression from current desktop behavior where the in-process node queues for later delivery.
- Receiving while offline is fine: the node holds the encrypted envelope; the client decrypts when it next syncs.
- Multi-device for the same account requires each device to derive its own ratchet sessions, OR the account adopts a multi-device key-share scheme (Signal's "linked devices" or sealed-sender style).

### Shape B — "Hosted node holds an HSM / TPM-equivalent that can encrypt/decrypt only"

The hosted node delegates all key operations to an isolated key custody service that:
- Receives plaintext peer-to-peer payloads transiently.
- Uses ratchet state stored encrypted-at-rest with a per-account KEK derived from the user's password (held only ephemerally during the user's online session).
- Returns ciphertext.

This is closer to the current desktop's behavior but exposes plaintext to the host during processing. **We reject Shape B** — it materially weakens the trust model and we have no way to make it auditable. Listed only to be explicit about the choice.

**Decision: Shape A.** The hosted node is a routing + storage facility; clients hold all keys.

## 3) WS gateway authentication (open question, leaning answer)

The hosted node speaks the existing rez WebSocket protocol. Today the local desktop app authenticates via `bridgeToken` (a random per-process token; both ends are the same machine). For a hosted node, the client is on a different machine and must prove "I am the account owner" without sending the account private key.

**Leaning answer: signed-challenge handshake.** On WS open, the node sends a random nonce. The client signs `(nonce || account_id || node_id || ts)` with the **account-level signing private key** (which lives on the client per §1) and replies. The node verifies the signature against the registered account public key, opens the session.

**Open questions:**
- **Multi-device.** If account-key signing is the credential, every device needs the account key. That's the same attack surface we already accept for the desktop's keystore (vault-encrypted at rest, decrypted with the password). Acceptable?
- **Signed-prekey-derived session credential** as an alternative — issue per-device session credentials signed by the account key, store only the per-device credential on each device. More complex; deferred.
- **Token revocation.** After a successful WS handshake, the node issues a session token (short-lived, e.g., 1 hour, refreshable). Lets us cut off compromised devices without rotating the account key. Required for hosted; not needed for in-process.

## 4) Multi-tenant data isolation

A single hosted rez-node process serves N accounts. The existing rez-node assumes single-tenant (one `ownerAccountId` for the whole process; the SDK's storage provider is keyed by owner internally but the node binary itself is owner-bound).

**Required changes (rez-node side):**
- The node identity becomes its own thing — one keypair for the *node*, not the owning account. Today these are conflated (the desktop derives its node identity from the account identity).
- Per-tenant inbox isolation: every mailbox is namespaced by accountId; queries cannot cross tenants.
- Per-tenant rate limits and quotas (storage size, message rate, peer-link count).
- Per-tenant key-rotation hooks (when the user rotates their account key, the node re-binds without disrupting active peer-links — peer-links carry their own identity bindings via X3DH).

**Required changes (rez-chat side):**
- `ChatServerApp.identity` becomes truly the *account* identity (which it already is). The `nodeRuntime` becomes a remote handle to the hosted node, replacing the in-process `startRezNode`.
- The desktop variant continues to embed an in-process node, OR can be configured to point at a hosted node (user choice).
- Cold-boot / sign-up: account creation either provisions a fresh node identity (single-tenant in-process) or registers the account against an existing hosted node (multi-tenant).

## 5) Cold-boot of a tenant on a shared node

When a user adds their account to a hosted node:

1. Client generates account identity (existing flow).
2. Client signs a `TenantRegistrationRequest` with the account key:
   ```
   { accountId, accountPublicKey, requestedQuota, ts }
   ```
3. Client opens WS to hosted node, sends request via the bridge.
4. Hosted node:
   - Verifies the signature.
   - Allocates per-tenant storage (mailbox table, peer-link table, etc.).
   - Issues a session token.
   - Returns the node identity and any seed/mesh bootstrap info.
5. Client begins normal operation — encrypted deposits flow through the node.

Subsequent device additions:
- Same handshake. The "tenant" already exists; the node's storage already holds the user's encrypted state. New device gets a fresh session token, syncs ratchet state from… where? **Open: cross-device ratchet state sync is its own design problem.** Without it, each new device starts fresh peer-links to existing peers. Acceptable for v1; nicer for v2.

## 6) Deletion / eviction

Two flavors:

- **User-initiated deletion** ("close my account"): client signs a deletion request; node removes all per-tenant data, invalidates session tokens, marks account ID as tombstoned for replay protection.
- **Operator eviction** (TOS violation, payment lapse): node retains a tombstone but data is purged after a grace period.

In both cases, the node never had plaintext, so deletion is "remove the encrypted blobs and the routing pointers."

## 7) Billing / quota hooks

Out of scope for the trust-model design. A pragmatic v1:

- Per-tenant monthly storage quota (mailbox bytes-at-rest).
- Per-tenant outbound message rate cap.
- Soft warnings via a node-side `quota.warning` event (re-emitted to client, surfaced in the SDK's runtime status).

Hard enforcement happens at the WS gateway (drop / 429 with `quota.exceeded`).

## 8) Open design questions

These are the things that need more thought before implementation:

1. **Multi-device session token model.** Per-account key vs per-device credential — covered briefly in §3, but the trade-offs (revocation, key compromise blast radius, UX on adding a device) need more analysis.
2. **Ratchet state sync between a user's own devices.** Current desktop assumption is one device per account. Hosted should not regress this; a "linked-device" scheme similar to Signal is the obvious answer but isn't designed yet.
3. **Cross-tenant peer-link traffic.** When tenant A on hosted-node-X messages tenant B on hosted-node-X, can the message stay inside the node (avoiding mesh egress) without weakening privacy? Probably yes (it's still encrypted), but worth being explicit about.
4. **Bootstrap of identity on a fresh device.** Account-key recovery model — today the desktop handles this via the keystore vault and password. For hosted-only users (mobile / web), there's no local keystore. The user's password becomes the only recovery path. Acceptable, but documents a hard constraint.
5. **Federated hosted nodes.** Can hosted-node-X talk to hosted-node-Y on behalf of their respective tenants without unwinding the trust model? Yes, via the existing mesh routing protocol. The interesting question is whether a tenant on node X can choose which hosted nodes their messages route through (e.g., for jurisdictional reasons).
6. **Audit + transparency.** Open-source the hosted node binary? Reproducible builds? Independent audit? Worth committing to early since it's load-bearing for the trust story.

## 8.5) Future split: peer-link protocol dispatcher

`ServerPeerLinkProtocolService` (currently in rez-chat) dispatches inbound
peer-link protocol messages by `kind`: `handshake`, `rehandshake`,
`handshake-ack`, `claim-req`, `claim-res`, `delivery-ack`, `user-message`.

Six of those seven branches are generic peer-link wire flow — nothing
chat-specific. Only `user-message` is chat-domain. The crypto primitives
themselves (`PeerLinkService`: X3DH, ratchet, peer-link records, invite
envelopes) already live in rez-sdk, so any second rez-app (rez-twitter
DMs, rez-reddit DMs, etc.) can instantiate one — but it would have to
re-implement the inbound dispatcher.

When a second app actually appears, lift the dispatcher up to rez-sdk
and let each app register its own `user-message` handler. Holding off
on the move until there's a real second consumer (no speculative
refactor).

## 9) What this doesn't touch

- The on-the-wire protocol vocabulary is mostly unchanged; one new wire type (`node.sendEncryptedDeposit`) lands as part of step 1 to carry the encrypted-deposit RPC over WS instead of via an in-process pointer.
- E2EE primitives (X3DH, double ratchet, envelope codec) are unchanged.
- The chat-domain layer (`ChatServerApp` and its services) doesn't know whether the node is local or remote — that's now purely a wsUrl decision.
- The renderer / UI is unchanged.

## 10) Implementation order

1. **Unified node access via `RezClient`** — **SHIPPED 2026-05**. The original framing (a "RemoteNodeRuntime" adapter that ChatServerApp would swap in for hosted vs in-process) was wrong because it codifies location-awareness. Replaced with: `ChatServerApp` no longer accepts a `nodeRuntime` parameter at all. `RezClient` (the SDK) gains the four methods chat-server used to reach via the in-process pointer — `sendEncryptedDeposit`, `getIdentity`, `refreshMesh`, `onMeshStatusChanged`. A new wire RPC `node.sendEncryptedDeposit` (handler in `rez-node/src/protocol/handlers/NodeRuntimeHandler.js`) carries the encrypted-deposit call when the node is on the other end of WS. Whether the node is on `127.0.0.1` or a VPS is purely a wsUrl decision. Auth was a red herring at this layer — the SDK already does signed-challenge handshake (`session.hello → session.challenge → session.authenticate`) and that's location-agnostic.

   **Trust-model state after step 1:** encryption still happens on the node (Shape B). A hosted node deployed today CAN see plaintext, because that's where `peerLinks.encryptDirectMessage` runs. Migrating encryption to the chat-server side (Shape A) is step 7. The `node.sendEncryptedDeposit` wire type is intentionally short-lived and gets deprecated when Shape A lands; at that point chat-server encrypts client-side and uses the existing `MAILBOX_DEPOSIT` directly.

2. **Multi-tenant storage isolation** in rez-node.
3. **Tenant registration** flow on the rez-chat side (account creation can target either local or hosted).
4. **Session token model** for handshake → session lifetime (refreshable short-lived tokens after auth).
5. **Quota / billing hooks.**
6. **Audit / transparency posture.**
7. **Shape A migration: client-side encryption.** Move `PeerLinkService.encryptDirectMessage` / `decryptDirectMessage` and ratchet session storage out of rez-node and into rez-sdk (or alongside `ChatServerApp`). Inbound flow: node delivers ciphertext via subscriptions; client decrypts. Outbound flow: client encrypts; node deposits ciphertext only. Once this lands, the `node.sendEncryptedDeposit` wire type is deleted and the trust model is fully Shape A.
8. **Multi-device + ratchet sync.** (separate design, gated on a working Shape A.)

Each step is independently testable.
