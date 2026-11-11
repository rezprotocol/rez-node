# Image/package cleanup and publishing preparation — 2026-11-11

**Implementation, focused audit and local verification: PASS.**
**Publication and deployment: deliberately not executed.**

The [publishing plan](PUBLISHING_PLAN_2026-11-11.md) is ready for review. All
cleanup changes are committed locally on the maintainer’s request. No Git push/tag, npm staging/publication,
registry push, GitHub release edit, visibility change or live rollout occurred.
Previously published rc.13 artifacts are unchanged by this task.

## What changed

One canonical `rez-node/Dockerfile` now serves both hosted Compose templates.
Dependency installation occurs in a pinned Node build stage. After cleanup, a
fresh final stage copies the clean filesystem rather than inheriting layers
containing deleted npm/Yarn files or installation caches. Runtime sources,
production dependencies, the Node executable, shell/health tools and licenses
remain. Build/test helpers, global package managers and caches do not ship.
Runtime UID 1000, TLS/config mount expectations, storage volume and entry point
are preserved. No protocol/application source or runtime dependency changed.

The Dockerfile-specific context allowlist excludes unrelated repositories,
credentials and local state. The obsolete general ignore file and duplicate
`deploy/Dockerfile` are removed. Both normal and portable deployment templates
reference the canonical recipe; no iPhone/Swift code was changed.

Core, SDK and Node package manifests explicitly retain LICENSE/NOTICE, exclude
secret/state file patterns, declare their real source repositories and default
to the developer `next` publishing channel. Node's npm package no longer includes
build/test scripts. Source scripts remain in the repository for CI and CLI
building; they are not dead code and are not installed as runtime helpers.
Versions, exact internal dependencies, package exports and dependency locks are
unchanged. Local artifacts retain rc.13 metadata and cannot replace official
rc.13 artifacts. The plan proposes a new coordinated rc.14 after approval.

The local release workflow now prepares artifacts only. It retains complete
suite, backend/recovery, image and platform CLI gates, packs the real archives
once, checks them and uploads CI artifacts. Registry login/push, npm publication,
GitHub release upload and associated write permissions were removed. It is not
pushed: the remote existing workflow still needs the reviewed preparation-only
change before anyone creates the next release tags.

Image and package checks share one archive reader and credential-content policy.
The image checker inspects all archived layer payloads, including deleted lower
layers, and image metadata. It rejects helpers, caches, credential/state paths,
unexpected application roots, secret-looking text and parseable private keys.
It also checks required runtime files and the non-root image configuration.
The package checker allows only the appropriate package's runtime/example/CLI
surface and notices. Unsupported/corrupt archive input fails explicitly. Neither
checker extracts image-supplied paths or prints matched credential values.

## Files changed

- Core and SDK: `package.json` only.
- Node: `Dockerfile`, new `Dockerfile.dockerignore`, `package.json`, `README.md`,
  both deployment Compose files, and `.github/workflows/release.yml`.
- Node helpers: new `scripts/lib/ReleaseArchive.js`, `ReleaseContent.js`,
  `scripts/verify-release-image.mjs`, `verify-release-package.mjs`.
- Node tests: new `test/release.image-hygiene.test.js` and the focused recovery
  fixture fixes in `test/e2e.hosted-compose.test.js`.
- Node documentation: `HOSTED_OPERATIONS.md`, `running-a-node.md`, this report,
  the publishing plan and bounded verification receipts.
- Deleted Node paths: `.dockerignore`, `deploy/Dockerfile`.
- Chat: no files changed. All original storage/native/iPhone/Swift work preserved.

## Local verification

Candidate tag: `rez-network-cleanup:review-20261111` — **local only**.
Image ID: `sha256:2383b928737290de4b1a199919924bf556755cbdca10b493e71b5488345a0099`.
Exported archive SHA-256:
`d401b9d043d20ebe818447458d724ee56c18e5fcc96eef2d890da6d2af2a46ef`.

| Check | Result |
| --- | --- |
| Clean source context, original work excluded | PASS; source files came from committed repository archives plus explicit cleanup paths |
| Docker build and real runtime boot | PASS; Node v24.22.2, UID 1000, expected public imports/CLI and notices |
| Every archived layer/metadata payload | PASS; two tar payloads, 1,492 file occurrences; no forbidden helper/cache/credential paths |
| Trivy vulnerability + secret scan of that exact image ID | Zero HIGH/CRITICAL vulnerability or secret findings; Alpine and Node-package inventories detected |
| Focused Linux authentication, socket lifecycle, CLI, version and hygiene tests | 40 passed, zero failures/skips |
| Six new hygiene regressions | PASS on host and Linux Node 24; corruption/traversal, hidden lower-layer cache/token, secret metadata, package helper exclusion |
| Disposable TLS hosted topology | Healthy before recovery drill |
| Final-image destructive recovery | Five passed, zero failures/skips; process death, Redis replay, PostgreSQL outage/restart, backup restore, cursor/tenant/byte invariants |
| Actual Core/SDK/Node package archives | PASS; 252 / 120 / 264 files respectively, required notices present |
| Fresh installation from all three local archives | PASS; public imports and installed CLI version |
| Installed SDK against the isolated TLS topology | Authenticated node.status succeeded |
| Planted `.env` / `.key` files in all three source trees | Excluded; package archive integrities and Docker image ID unchanged |
| Preparation workflow static checks | YAML parses; no publisher commands/actions/write permissions; all platform gates retained |
| Original working changes at verification | All four HEADs matched the pre-cleanup baseline; original tracked diffs and ten original untracked file hashes matched |

Only task-owned disposable Docker projects/volumes were removed. The local
candidate and review evidence remain available; no shared Docker prune was run.

### Findings fixed while verifying

The first cleanup build exposed emulator-generated `/root/.cache/rosetta`
files. Cleanup was moved to the final operation of the build-stage filesystem
preparation; the exported image was rescanned successfully. OCI and legacy
Docker configurations can coexist in one archive, so runtime settings are
compared after normalizing legacy-only defaults rather than treating those
representations as different images.

The original recovery harness made eight immediate attempts to find the second
backend after process death. In this run all eight reached the same backend in
about 50 ms. That did not establish whether the second backend would recover
within the proxy's DNS/failed-backend recovery window. The helper now waits
within a bounded retry window, uses a bounded opening handshake, and retries
only connection/502/503 failures rather than authentication-policy failures.

The failed Redis phase previously let later phases continue, producing a
misleading restore sequence expectation because the Redis-phase packet had
never been deposited. Explicit prerequisite checks now stop dependent phases
at the real failure. The complete drill was rerun from fresh disposable storage
and all five checks passed. Both the original failure and corrected result are
retained; the failed run is not counted as a pass. No production server logic
or data-restoration behavior was changed to make the test pass.

The real npm pack initially included the deliberately planted secret-file
sentinels beneath `src/`. Explicit manifest exclusions now prevent that; the
independent package gate also rejects such files if they reappear. No actual
credential was involved in those fixtures.

## Focused audit

Specs checked: repository `AGENTS.md`; the canonical image/Compose/startup
contract; hosted operating gates and strict isolated recovery guard; package
exports/files/version declarations; current npm/GitHub publishing documentation
linked in the publishing plan. Prior dated deployment reports remain historical
evidence and were not rewritten to pretend the old Dockerfile never existed.

Canonical owners: Node owns runtime packaging and release verification; Core
owns shared protocol/crypto; SDK owns the app-facing facade; Chat and native/UI
work stay in their existing owners. Correct package repository metadata preserves
that ownership for future npm provenance. No new application or RPC record shape,
transport directive list, cross-layer import or protocol transform was added.

Duplicate/split-brain findings: the second image recipe and unused ignore policy
were removed; normal and portable builds share the same recipe. Tar parsing and
credential detection are shared by image/package gates. There is one coordinated
preparation flow. Future owning-repo staging wrappers in the plan must share one
pinned implementation rather than fork build/test/release logic.

Boundary/SSOT findings: none introduced. Publisher identity is per owning source
repository; the plan explicitly rejects mislabeling Core/SDK as Node to bypass
provenance checks. Runtime dependencies, signed vocabulary, storage semantics and
client APIs are unchanged. Builder-only dependencies never become runtime layers.

God-class/module findings: no new runtime class or mixed application service.
Archive parsing, content checks and image/package CLI policies are separate small
Node-owned tools. No protocol, networking or UI logic moved into them.

Hidden transforms/invariants: packages and images use declared inclusions and
exclusions; runtime code is not rewritten. TLS/authentication/identity and cursor
assertions are unchanged. The recovery test now respects actual bounded proxy
recovery rather than an immediate attempt count, and cannot continue dependent
proofs after a prerequisite fails. Direct credential values are never printed.

Dead/stale paths removed: duplicate deployment Dockerfile, ineffective old ignore
file, runtime/npm build-test helpers, installation-layer caches/tooling, automatic
registry/npm/release publishers, misleading nested install/bare Docker examples,
and the claim that standalone CLI downloads were only coming soon. The legacy
`rez-relay` shim is deliberately retained for compatibility and documented as
printing the replacement command; it is not another relay runtime.

Tests added/updated: six archive/image/package security regressions; actual
before/after image and package checks with planted sentinels; bounded recovery
routing and explicit phase prerequisites. Existing focused Linux cases and real
backend recovery proofs ran without opt-outs. No formatter/lint/dependency-tool
configuration, runtime dependency or root installation policy was changed.

## Commit follow-up

The maintainer subsequently authorized committing the cleanup, while withholding
release approval. Cleanup paths were committed separately in Core, SDK and Node;
Chat was not committed. No push, Git tag, registry staging/publication or live
rollout accompanied those commits. The machine-readable receipt retains the
original pre-commit verification state; it is historical test evidence, not a
claim that these later local commits never occurred.

## Remaining work and limits

The [machine-readable receipt](verification/image-cleanup-2026-11-11/acceptance.json)
and bounded evidence files record the candidate and results. Secret matching
cannot rule out arbitrary unknown/encoded secrets or exhaustively classify binary
contents. The clean source boundary, every-layer/path checks and independent
vulnerability/secret scan are complementary controls.

Full Core/SDK/Node suites and the four-platform remote build matrix were not rerun
here because no protocol/runtime implementation changed and nothing was pushed
or dispatched. They remain required preparation gates for the approved new
version. No new CLI binary, desktop GUI validation, signing/notarization or live
monitored soak was claimed for this local cleanup. No contract/chain tests were
needed for these packaging-only changes.

Publishing remains unexecuted: account ownership/2FA, a newer publishing CLI,
per-owning-repository trusted staging, the first-cut provenance decision, and
registry-wide historical-image review/visibility approval are in the plan.
Production deployment is separately gated. This report does not lift any desktop,
iPhone or Swift release hold or authorize modifying existing rc.13 artifacts.
