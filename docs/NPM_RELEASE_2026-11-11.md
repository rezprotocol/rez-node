# npm developer release 0.6.0-rc.14 — 2026-11-11

**Release verification: PASS. Publication: PASS — all three packages public on `next`.**

The approved cut is `@rezprotocol/core`, `@rezprotocol/sdk` and
`@rezprotocol/node` at `0.6.0-rc.14`, on npm's public `next` channel.
No hosted node, relay, web client or desktop deployment was changed by this task.
No iPhone or Swift work was edited, staged or included in the source cut.
No new Docker registry image or GitHub release assets were published.

## Install

Use Node 24 or newer for the packaged connection example:

```sh
npm install @rezprotocol/sdk@next
node node_modules/@rezprotocol/sdk/examples/connect.mjs
```

Core is installed automatically at the exact matching version. To pin the
release, use `@rezprotocol/sdk@0.6.0-rc.14`. Standalone Node operators can use
`npm install -g @rezprotocol/node@0.6.0-rc.14` and `rez-node version`.
Applications consume SDK; Core owns protocol/crypto and Node owns server runtime.

## Exact cut and evidence

Final immutable source tags are **`npm/v0.6.0-rc.14`** in all three repositories:

| Owner | Source commit |
| --- | --- |
| Core | `a137c5c69344858d24d239e2e22cc5ead5e1d37e` |
| SDK | `d2d31c477eec73fd76fcbae1934885f747744788` |
| Node | `a554cebbbe8e0b7d7a06b3033f37a32d2df6f79f` |

[Coordinated CI run 43101260753](https://github.com/rezprotocol/rez-node/actions/runs/43101260753)
passed verification and all four platform jobs. The earlier `v0.6.0-rc.14`
preparation tags remain immutable; their initial queued run is not release
acceptance evidence. The npm tags capture the corrected documentation and
release checks without rewriting existing source tags.

The actual CI tarballs were downloaded once, independently checked against
every packaged file's Git blob in the tagged source, and installed in a fresh
external consumer. Their checksums also match the independently built local
archives. Publication promotes those exact CI bytes, without repacking.

The [receipt](verification/npm-rc14-2026-11-11/receipt.json) records source commits,
CI artifacts, SHA-256/SHA-512 values, registry readbacks, consumer proof and WIP
preservation. The [checksums](verification/npm-rc14-2026-11-11/SHA256SUMS.txt),
[image inspection](verification/npm-rc14-2026-11-11/image-hygiene.txt) and
[image inventory](verification/npm-rc14-2026-11-11/image-inventory.json) retain
bounded evidence without authentication links, tokens or user data.

## Verification

| Gate | Result |
| --- | --- |
| Core full suite | 582 passed; zero failures/skips |
| SDK full suite | 549 passed; zero failures/skips |
| Node full suite, real PostgreSQL/Redis, hosted TLS and local mesh | 1,413 passed; zero failures/skips |
| Exact-image destructive recovery in disposable topology | Five passed; zero failures/skips |
| Linux x64, Windows x64, macOS arm64/x64 | 23 authentication/version checks each; binary build and packaged version smoke passed |
| Package hygiene and tagged-source byte comparison | All three passed; 252/120/264 files, required notices present |
| Actual image, every archived layer and metadata | Passed; two layer blobs, 1,492 file occurrences, no forbidden helper/cache/credential paths or token/key patterns |
| Trivy HIGH/CRITICAL vulnerability and secret scan | Zero findings; 19 Alpine and 28 Node package inventory entries |
| Fresh CI-archive consumer | Public imports, installed CLI version and two authenticated SDK connect/disconnect cycles passed against the recovered isolated topology |
| Public registry archive/integrity/channel readbacks | PASS; public bytes equal CI, next=rc.14, latest=bootstrap placeholder |
| Fresh registry-only consumer and existing hosted-network compatibility | PASS; exact registry integrities, public imports, CLI rc.14 and two authenticated SDK connect/disconnect cycles |

The recovery drill proved both Node processes surviving SIGKILL with retained
ciphertext/cursors, Redis loss with durable replay/wakeup recovery, bounded
PostgreSQL outage/readiness recovery, and restoration into a separate database
with retained bytes, cursor and tenant isolation. No live fault injection ran.

The independent first Node run exposed an obsolete test reading the deleted
`deploy/Dockerfile`. A later emulated run hit a three-second PostgreSQL
connection timeout in settlement testing. Those runs are retained as failures,
not counted as passes. The focused settlement suite passed all 11 tests; the
local full suite with two workers and the default CI full suite passed. No
production timeout or settlement logic was changed. OpenSSL was installed only
in the disposable test environment to run the real TLS test without a skip.

Only this task's isolated local containers, networks and disposable volumes
were removed after verification. No shared Docker prune or live cleanup ran.

## Publication behavior

npm's interactive first-package bootstrap created public `0.0.0-stage`
placeholders. Although publication explicitly used `--tag next`, npm also put
rc.14 on `latest`. Deleting Core's `latest` tag returned E400. The correction
is to restore `latest` to the already-existing staging placeholder and retain
rc.14 on `next`; no extra real release or stable version is published.

Uploads returned HTTP 202 and were processed asynchronously. CLI acceptance
alone was not treated as public availability. Dependency order is Core, then
SDK, then Node; public archive bytes and channels must pass readback before the
dependent package proceeds. Ordinary fresh npm installation is also required.

This is a manual, 2FA-protected bootstrap with the isolated npm 11.21.2 tool.
No npm CI provenance was generated. Owning-repository trusted publishers,
stage-only automation and future stable promotion remain separate setup work.
No token was printed, created for automation, or placed in source/build inputs.

## Changes and focused audit

Files changed for this cut:

- Core: `package.json`, `README.md`.
- SDK: `package.json`, `README.md`.
- Node: `package.json`, `README.md`, `release/workspace-package-lock.json`,
  `.github/workflows/release.yml`, `scripts/ensure-tag-version.mjs`,
  `test/release.version-consistency.test.js`, `test/network.log-privacy.test.js`.
- Node docs: `docs/NPM_RELEASE_2026-11-11.md`,
  `docs/PUBLISHING_PLAN_2026-11-11.md`, `docs/running-a-node.md`.
- Node evidence under `docs/verification/npm-rc14-2026-11-11/`:
  `receipt.json`, `SHA256SUMS.txt`, `image-hygiene.txt`, `image-inventory.json`.
  No Chat/native source paths were part of these commits.

The release also includes the previously committed
[image/package cleanup](IMAGE_CLEANUP_2026-11-11.md); that report retains its
complete file inventory and audit.

Specs checked: repository `AGENTS.md`; package versions/exports/publishConfig;
the coordinated release lock and tag gate; the canonical Dockerfile and actual
archive policies; the dated cleanup report and publishing plan; npm's publishing,
2FA, staged bootstrap and distribution-tag documentation. Dated earlier reports
remain historical evidence.

Canonical owners confirmed: Core owns protocol/crypto/shared signed vocabulary;
SDK owns the app-facing facade; Node owns server runtime, packaging and the
single coordinated verification pipeline. No protocol/API implementation,
external runtime dependency, application/RPC record shape or transport dispatch changed.

Duplicate/split-brain findings: removed the superseded Dockerfile-text assertion
from network privacy tests and its unused `fs` import. Actual image/package
archive inspection remains the canonical security gate. Both source-tag forms
use the same version validator and workflow; no parallel publishing pipeline
was introduced. Runtime files were not transformed or copied from dirty WIP.

Boundary/SSOT findings: none introduced. Internal dependencies and the release
lock all require the same rc.14. Repository metadata continues to name each
real owning repository. No app/UI responsibilities crossed into Core/SDK/Node.

God-class/mixed-responsibility findings: none introduced; the source-tag change
is a small extension to the existing Node-owned release validator. Dead code
removed: the obsolete Dockerfile assertion and its unused import. Updated tests
cover npm source tags, version mismatch rejection and invalid tag prefixes;
the existing archive security regressions and complete suites passed.

## Limits and follow-ups

All original Core/SDK/Node tracked diffs and original untracked file hashes were
preserved. Chat had concurrent independent work and was excluded entirely; its
working tree was not reset, staged or committed by this task. Checks apply to
the clean immutable release cut, not uncommitted storage/native work.

The live network remains outside this release action. No new live soak, GUI
release, iPhone acceptance, Swift acceptance, contract release or production
rollout is claimed. The existing hosted network remains an early developer
network with its previously documented single-database-host durability limit.

The CI image and four CLI archives are verified CI artifacts only. Publishing
those assets, GHCR visibility, automated trusted publishing and deployment need
their own authorized follow-up. Existing action-version deprecation warnings
did not fail any gate; future workflow maintenance remains separate.
