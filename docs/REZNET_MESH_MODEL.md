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

## Data Authority
- Chat/account state remains client authoritative.
- Relay/node mesh is for routing substrate availability, not plaintext chat data ownership.
- Backup remains encrypted and opaque to node operators.

## Failure Handling
- Source failures are tolerated; other sources continue.
- Expired descriptors are filtered out.
- Discovery errors do not crash node process.
- Health state marks peers `healthy`, `degraded`, or `stale`.
