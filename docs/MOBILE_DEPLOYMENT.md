# Mobile provider deployment

The mobile client uses the shared Postgres account home for enrollment and a
separate claimant-only portable provider for ordinary delivery. F9 remains
enforced: neither a claimant session nor a portable lease belongs on the
Postgres home.

From `rez-node/deploy`, use the existing hosted secrets and relay bootstrap
configuration with:

```sh
docker compose -f docker-compose.yml -f docker-compose.mobile.yml config
docker compose -f docker-compose.yml -f docker-compose.mobile.yml up --build --detach --wait
```

The overlay adds `portable1`, with its own filesystem volume and node identity,
and two TLS edge routes:

| Purpose | Route |
|---|---|
| Account enrollment and activation | `wss://<host>/ws` |
| Permanent portable inbox | `wss://<host>/mobile/ws` |
| Portable provider readiness | `https://<host>/mobile/ready` |

`PORTABLE_NODE_IMAGE` may select an existing, release-matched node image.
Pin that image to an immutable digest or a preserved local tag and use
`--no-build` when reusing it. This does not authorize bypassing the operating
contract for a new node revision. The default image name is
`rez-portable-node:local` for an explicit build.

`PORTABLE_RELAY_PORT` defaults to 4203. Expose this TCP/TLS port at
`ADVERTISED_HOST`, preserve the TLS file permissions, and provide nonempty
`REZ_KNOWN_RELAYS_JSON` containing verified relay identities. The provider
refuses an empty bootstrap set. Use release-matched image inputs and the
existing TLS renewal procedure, also rolling `portable1` after certificate
replacement. Include the mobile overlay in every subsequent Compose operation.

## Persistence and availability

`portable1data` permanently stores claims, tombstones, lease generations and
buffered ciphertext. Never remove the volume to recover a failed container.
Snapshot it while the provider is stopped and encrypt backups in a different
failure domain; restore identity and mailbox state together. Prove claim/lease
rehydration and offline catch-up after every restore drill.

This first topology has one portable provider: persistence across container
restarts, not shared-Postgres failover. Do not put independent filesystem
providers behind random round-robin and assume they share mail. Monitor
`/mobile/ready` separately from the account home, together with storage capacity,
relay reachability and lease/reclamation failures. Provider disk loss requires
a restore; without a usable backup, provider-buffered ciphertext can be lost.

## Acceptance before release

Against a release-matched test TLS origin, run real mobile enrollment and
activation with both routes. Verify that the published device bundle names the
portable inbox and steady-state frames remain claimant only. Deposit while the
phone is offline, replace the portable container preserving its volume, and
require exact decrypted catch-up after reconnect. Stop the account home and
require ordinary delivery to continue. Verify foreign-claimant refusal,
lease expiry/grace/reclamation and terminal close.

The overlay does not supply APNs/FCM or declare the app released. The hosted
operating contract and physical-device/push gates still apply.
