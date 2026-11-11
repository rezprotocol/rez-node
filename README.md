# rez-node

`@rezprotocol/node` is the Rez node runtime and CLI.

## Install and distribution status

The npm developer cut is **0.6.0-rc.14**, under the `next` channel.

```sh
npm install -g @rezprotocol/node@0.6.0-rc.14
rez-node version
```

Public standalone Node CLI archives remain on the [release page](https://github.com/rezprotocol/rez-node/releases);
they are separate from Rez Chat desktop installers and may be an older version.
This npm publication does not publish a new container/CLI archive or change the
hosted deployment. Anonymous GHCR pulls still require separate package visibility
approval. See the [publishing plan](docs/PUBLISHING_PLAN_2026-11-11.md).

### From source

Use Node 24 and matching Core, SDK and Node revisions in a clean polyrepo workspace.
Install from the workspace root, using the coordinated workspace manifest and lock under
`rez-node/release/`. Do not install inside an individual package or overwrite an existing
workspace's lock. See the publishing plan for the clean-source preparation and validation gates.

### Docker

Build from the polyrepo root containing all three runtime repositories:

```sh
docker build --platform linux/amd64 --file rez-node/Dockerfile --tag rez-node:local .
docker run --rm rez-node:local node src/cli/index.js version
```

`rez-node/Dockerfile` is the sole image recipe. Its adjacent ignore file allows only
runtime inputs into the build context. Dependency installation happens in a build stage;
the final image contains the cleaned filesystem without that stage's caches or tooling.
Build/test scripts are excluded. LICENSE and NOTICE files are retained.

Starting a server additionally requires operator configuration, TLS material and persistent
storage. Use the hosted Compose templates and [operations guide](docs/HOSTED_OPERATIONS.md);
credentials and TLS keys belong in runtime environment/mounts, never build arguments or files
copied into the image. The normal and portable Compose templates use the same image recipe.

## CLI Commands

```bash
rez-node version
rez-node init [--config <path>] [--data-dir <path>] [--force]
rez-node doctor [--config <path>]
rez-node start [--config <path>]
```

Outputs are stable and grep-friendly:
- success lines begin with `OK` (or `DOCTOR_OK`)
- failures begin with `ERR` (or `DOCTOR_FAIL`)

## Config + Data Directory

Default config path:
- `./rez-node.config.json`

Default data dir:
- `./.local/rez-node-data`

`rez-node init` creates both. `rez-node start` loads config and starts the websocket gateway. `rez-node doctor` validates config parsing, data-dir read/write access, ws port availability, and control-socket binding/perms.

`rez-node start` also starts the local control IPC server by default and prints:

- `rez-node control=/path/to/control.sock` (macOS/Linux)
- `rez-node control=\\\\.\\pipe\\rez-node-control` (Windows)

Disable control server:

```bash
rez-node start --no-control
```

## Other Binaries

- `rez-relay` is a legacy compatibility shim. It prints the replacement command and exits; run relays with `rez-node start` and a relay-only configuration.

---

## Settlement + paid services

Carrying messages is free and permissionless — anyone may run a relay and route traffic. *Earning* REZ for paid services is gated on trust-graph recognition (EigenTrust, seeded from neutral published seed relays), not on any bond or stake; the `TrustGraph` supersedes the older linear `ReputationScorer`.

Paid services (claimed `@handles`, persistent storage, large media/files) are priced and metered through the settlement layer. A single `ServiceGate` enforces atomic, multi-leg `settleService` calls and returns `PAYMENT_REQUIRED` when the requester is underfunded; the canonical record is `SettlementEntryV1`, appended to the `SettlementJournal`. During beta this runs on off-chain Service Credits via the `LocalSettlementProvider`.

The on-chain `SettlementProvider` integration uses [`viem`](https://viem.sh) confined to `src/settlement/eth/` (testnet during beta); the REZ token and custody contracts it settles against live in [`rez-contracts`](https://github.com/rezprotocol/rez-contracts).

---

## Documentation

| Doc | Contents |
|---|---|
| [docs/running-a-node.md](./docs/running-a-node.md) | Self-hosting guide: CLI commands, config, Docker, Homebrew |
| [docs/HOSTED_NODE_DESIGN.md](./docs/HOSTED_NODE_DESIGN.md) | Relay node architecture, delivery guarantees, persistence |
| [docs/HOSTED_OPERATIONS.md](./docs/HOSTED_OPERATIONS.md) | Hosted release gates, alerts, backup/restore, incidents |
| [docs/PERSISTENCE.md](./docs/PERSISTENCE.md) | Storage guarantees and persistence model |
| [docs/REZNET_MESH_MODEL.md](./docs/REZNET_MESH_MODEL.md) | Relay mesh topology and routing |

For protocol-level documentation (capability model, wire contracts, identifiers), see [`rez-core/docs/`](https://github.com/rezprotocol/rez-core/tree/main/docs).

---

## Related projects

- [**rez-core**](https://github.com/rezprotocol/rez-core) — cryptographic primitives + protocol records
- [**rez-sdk**](https://github.com/rezprotocol/rez-sdk) — client SDK that connects to nodes
- [**rez-ui**](https://github.com/rezprotocol/rez-ui) — shared UI framework
- [**rez-chat**](https://github.com/rezprotocol/rez-chat) — reference desktop chat application; every install runs a local rez-node
- [**rez-contracts**](https://github.com/rezprotocol/rez-contracts) — Solidity contract suite (REZ token + custody/settlement) that the on-chain settlement provider targets

---

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md). Security disclosures: see [SECURITY.md](./SECURITY.md).

## License

Apache 2.0. See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).
