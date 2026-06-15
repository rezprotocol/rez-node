# Reznet Mesh Model (v1)

## Purpose
- Form a node-to-node mesh for routing using `rez-node` substrate primitives.
- Keep app authority client-side; node remains transport + optional encrypted backup substrate.
- Minimize operator risk via conservative defaults and bounded metadata.

## Defaults
- `mesh.enabled=true`
- `mesh.mode="seeded-gossip"`
- `mesh.participateInRouting=true` (gated by `node.network.participateInRouting`)
- `mesh.minPeers=3`, `mesh.maxPeers=32`
- `mesh.discoveryIntervalMs=30000`
- `mesh.gossipIntervalMs=60000`
- `mesh.policy={ rateLimit:120, payloadMaxBytes:1048576, failureThreshold:8 }`

## Lifecycle
1. Node starts, loads configured relays and mesh config.
2. `MeshCoordinator` bootstraps from `mesh.seeds`.
3. Discovery fetches relay descriptors from seed directory endpoints.
4. Admission applies policy:
- descriptor must validate
- relay key id must exist
- optional allowlist/denylist enforced
- unsupported meta version rejected
5. Accepted descriptors are merged into `RelayStore`.
6. In `seeded-gossip` mode, descriptors advertising HTTP transport are treated as additional discovery sources.
7. Periodic discovery/gossip refresh maintains peer set and updates mesh status.

## Operator Introspection
- `GET /health` includes mesh summary: enabled/mode/participation/peerCount/lastDiscoveryAtMs.
- WebSocket raw request `node.mesh.status` returns:
- node identity metadata
- mesh status
- sanitized peer list (`nodeId`, `transport`, `lastSeenAtMs`, `health`, `source`)

## Transport vs. Economic Recognition (two separate layers)
- **Transport / peer admission stays permissionless.** The mesh mechanics above (seeded-gossip discovery, descriptor validation, peer admission, routing) gate participation only on protocol correctness. Anyone may run a relay and carry traffic; admission is not an economic decision.
- **Economic recognition is a separate layer.** Earning real REZ does not flow from carrying traffic — it flows from being a *recognized* relay in the trust graph. Recognized relay identity keys are recorded in `RezRelayRegistry`, keyed by `recognizedRelayKey` (the same value that appears as `providerRelayId` in settlement records). Activity by a recognized relay counts toward real REZ; activity by an unrecognized one does not. The recognized-relay list is config-backed in beta and moves on-chain later.
- **`allowRelayKeyIds` becomes the economic recognition mechanism, not a transport gate.** The historically-unused allowlist (`allowRelayKeyIds` / the optional allowlist applied during admission in step 4) is repurposed: it expresses *economic recognition*, not transport permission. Transport admission remains open; the allowlist only governs whose activity is economically recognized.
- **Trust-graph recognition supersedes linear reputation.** An EigenTrust-style `TrustGraph`, seeded from the published, neutral, multi-operator seed-relay set (the existing `knownRelays` bootstrap trust root), determines rank; rank decays and saturates. The linear `ReputationScorer` is no longer the source of recognition — it becomes one input feature to the trust graph.
- **`networkId` binds the mesh to the official network.** An immutable, pre-genesis `networkId` is bound into the signed body of relay descriptors (and every other economic artifact: settlement receipts, peer attestations, escrow records, storage proofs). Only official-`networkId` descriptors earn or convert, isolating private forks even when they reuse identical mesh mechanics.

## Data Authority
- Chat/account state remains client authoritative.
- Relay/node mesh is for routing substrate availability, not plaintext chat data ownership.
- Backup remains encrypted and opaque to node operators.

## Failure Handling
- Source failures are tolerated; other sources continue.
- Expired descriptors are filtered out.
- Discovery errors do not crash node process.
- Health state marks peers `healthy`, `degraded`, or `stale`.
