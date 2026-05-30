# rez-node

`@rezprotocol/node` is the Rez node runtime and CLI.

## Install

### From source

```bash
git clone https://github.com/rezprotocol/rez-node.git
cd rez-node
npm install
node bin/rez-node.js version
```

### Docker

A `Dockerfile` is included for containerized deployments:

```bash
docker build -t rez-node .
docker run --rm -p 8787:8787 -v "$PWD/rez-data:/data" rez-node
```

### Coming soon

The following distribution channels are planned but not yet available:

- **npm**: `npm install -g @rezprotocol/node`
- **Homebrew**: `brew tap rezprotocol/tap && brew install rez-node`
- **GHCR container image**: `ghcr.io/rezprotocol/node:<version>`
- **Standalone binaries** via [release artifacts](https://github.com/rezprotocol/rez-node/releases)

Track progress in [issues](https://github.com/rezprotocol/rez-node/issues).

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

- `rez-relay` -> `bin/rez-relay.js`

---

## Documentation

| Doc | Contents |
|---|---|
| [docs/running-a-node.md](./docs/running-a-node.md) | Self-hosting guide: CLI commands, config, Docker, Homebrew |
| [docs/HOSTED_NODE_DESIGN.md](./docs/HOSTED_NODE_DESIGN.md) | Relay node architecture, delivery guarantees, persistence |
| [docs/PERSISTENCE.md](./docs/PERSISTENCE.md) | Storage guarantees and persistence model |
| [docs/REZNET_MESH_MODEL.md](./docs/REZNET_MESH_MODEL.md) | Relay mesh topology and routing |

For protocol-level documentation (capability model, wire contracts, identifiers), see [`rez-core/docs/`](https://github.com/rezprotocol/rez-core/tree/main/docs).

---

## Related projects

- [**rez-core**](https://github.com/rezprotocol/rez-core) — cryptographic primitives + protocol records
- [**rez-sdk**](https://github.com/rezprotocol/rez-sdk) — client SDK that connects to nodes
- [**rez-ui**](https://github.com/rezprotocol/rez-ui) — shared UI framework
- [**rez-chat**](https://github.com/rezprotocol/rez-chat) — reference desktop chat application; every install runs a local rez-node

---

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md). Security disclosures: see [SECURITY.md](./SECURITY.md).

## License

Apache 2.0. See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).
