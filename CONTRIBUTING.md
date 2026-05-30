# Contributing to @rezprotocol/node

Thanks for considering a contribution. `@rezprotocol/node` is the relay node runtime — every account whose traffic transits a node trusts this code. Please read this before opening a PR.

## Getting started

```bash
git clone https://github.com/rezprotocol/rez-node.git
cd rez-node
npm install
npm test
```

## Code style

This codebase is **vanilla JavaScript, ESM only**.

- ES2022+: async/await, classes, native `import` / `export`
- `#privateField` / `#privateMethod()` for private members; `_protectedMethod()` convention for protected
- **No optional chaining (`?.`)** — use explicit `if` / `===` checks
- **No empty `catch` blocks** — every caught exception must be handled or re-thrown
- No TypeScript, no Babel/SWC, no transpilation
- Tests use Node's built-in `node:test` runner

## Architecture

`rez-node` is the **trust-minimized** layer: it sees ciphertext and routing headers, never plaintext or keys. Any change that would have the node decrypt user content, observe peer-link state, or persist anything beyond what the protocol mandates is rejected.

Key references:
- [`docs/HOSTED_NODE_DESIGN.md`](./docs/HOSTED_NODE_DESIGN.md) — node architecture
- [`docs/REZNET_MESH_MODEL.md`](./docs/REZNET_MESH_MODEL.md) — relay mesh topology
- [`docs/PERSISTENCE.md`](./docs/PERSISTENCE.md) — storage guarantees
- [`rez-core/docs/ARCHITECTURE_GUARANTEES.md`](https://github.com/rezprotocol/rez-core/blob/main/docs/ARCHITECTURE_GUARANTEES.md) — cross-package layer responsibilities

## Tests

```bash
npm test
```

Routing, capability-model, and DHT changes additionally require adversarial test coverage (peer-imposters, replay, downgrade, route stickiness).

## Pull request process

1. Fork → branch → push.
2. Open a PR against `main`.
3. Describe the change concretely (what + why; the *what* should match the diff).
4. CI runs tests.
5. Maintainer review. Routing / capability / DHT changes get extra scrutiny.

## Licensing

By submitting a contribution, you agree that your contribution will be licensed under the Apache License 2.0, the license of this repository (per Section 5 of the Apache License).

## Security disclosures

Please do **not** open public issues for security vulnerabilities. See [SECURITY.md](./SECURITY.md) for the disclosure process.
