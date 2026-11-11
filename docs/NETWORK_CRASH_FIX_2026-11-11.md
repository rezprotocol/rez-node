# Peer-authentication crash fix and coordinated release cut

Implementation and focused audit: **PASS**. This is the network/Core/SDK
`0.6.0-rc.12` developer release. Deployment and registry publication must be
verified separately; an archive or source commit alone does not prove either.

## Failure and repair

At `2026-11-11T04:52:10Z`, the hosted node terminated with an unhandled
`relay connection closed during peer auth` rejection. `#ensurePeerAuthenticated`
awaited the hello write before observing its independent authentication promise.
A peer disconnect during that interval rejected an unobserved promise.

The regression reproduces the exact exception in an isolated process under
`--unhandled-rejections=strict`. It fails on the original source and survives
with the repair: the operation rejects normally and the process stays alive.
The fix observes write and authentication together and settles shared waiters
when either fails. Concurrent registration callers share one pending operation;
they cannot become ready merely because a socket exists. The authentication
deadline also covers a peer that sends a challenge but never accepts identify.
Shutdown rejects waiters and clears deadlines. Generation checks prevent old
failures from deleting a new handshake.

TCP teardown has one idempotent owner for error, close, idle expiry and shutdown.
A late close cannot evict a replacement, and a dial completing after shutdown
is destroyed. Signature, identity, client-nonce and explicit TLS checks remain.
No global rejection handler, ignored readiness failure or plaintext fallback
was added.

## Changed files and canonical owners

- Node `src/network/RelayConnectionPool.js`: signed peer authentication,
  shared registration readiness, deadlines and auth waiter lifecycle.
- Node `src/network/tcp/TcpConnectionManager.js`: connection/socket ownership,
  teardown and late-dial cleanup.
- Node connection-pool and TCP-manager tests: strict-process crash regression,
  concurrent rejection/retry, stalled accept, shutdown and replacement events.
- Node `scripts/ensure-tag-version.mjs` and its release consistency test:
  reject mismatched Core/SDK/Node versions or dependency ranges.
- Node `Dockerfile`: immutable Node 24 base, existing launchers, non-root
  runtime, deployment-compatible command/layout, runtime package-manager removal.
- Node `release/workspace-package-lock.json`: three-package graph using the
  previously tested runtime dependencies; no new dependency introduced.
- Node `.github/workflows/release.yml`: one coordinated pipeline replacing
  the three stale release workflows. Matching Core/SDK tags, a real backend/TLS
  fixture, complete tests, recovery and image scanning gate publication. The
  exact verified image is exported and published without rebuilding it.
- Node `scripts/test-support/prepare-release-proof.mjs`: disposable loopback
  test topology and test-only certificates. Destructive drills retain their
  strict `/tmp/rez-network-proof-N` and localhost protections.
- Node `docs/HOSTED_NODE_DESIGN.md`, this audit and package version metadata.
- Core package version only: cryptographic/protocol implementation unchanged.
- SDK package version, matched Core dependency, README and packaged connection
  example: existing public SDK facade only. A required device identity is
  included, and keys are not printed or persisted by the disposable example.
- Chat preview deployment image selection/package manifest mount: application
  source and web assets remain independent of the network rollout.

## Verification before rollout

| Check | Result |
|---|---|
| Original strict-process regression | Failed with the production exception |
| Focused connection/authentication checks | 22 passed, zero failed/skipped |
| Final Linux-image connection and release checks | 23 passed, zero failed/skipped |
| Clean Core suite | 582 passed, zero failed/skipped |
| Clean SDK suite | 549 passed, zero failed/skipped |
| Clean Node suite with real PostgreSQL, Redis, local mesh and hosted TLS | 1,408 passed, zero failed/skipped |
| Exact-image node-loss, Redis-loss, database-blackhole/restart and restore fixture | 5 passed, zero failed/skipped |
| Existing Chat compatibility | 1,118 passed, zero failed; 3 gated native/live integrations unrun |
| SDK installation/portability checks | 2 passed |
| Fresh app installed release Core/SDK archives and ran the packaged connection example | Authenticated hosted `node.status` passed |
| Release workflow syntax and coordinated-version mismatch regressions | Passed |

An initial full run failed because the disposable databases had no reachable
host ports. Another restore drill exposed missing fixture override metadata.
Those failures are retained in the local evidence; neither counts as a pass.
The corrected fixture and runs above are the acceptance evidence. No database
or Redis failure was deliberately induced on production.

## Focused audit

Specs checked: hosted-node trust/readiness requirements in
`HOSTED_NODE_DESIGN.md`; the canonical `PeerAuthShared.js` signed challenge and
accept payloads; relay identity binding and client-nonce replay checks;
`TcpFraming.js` framing/queue contracts; release package manifests and workspace
lock. The hosted spec now states complete registration readiness, bounded
authentication and teardown ownership. No wire schema or migration changed.

Canonical ownership stays in Node for networking and release orchestration,
Core for signed vocabulary/crypto, SDK for application-facing capabilities and
Chat for application semantics. No protocol logic moved into clients or UI.

Duplicate/split-brain findings: registration has one shared readiness promise;
TCP teardown has one owner; the old three release workflows are removed. There
is one verified source/image gate before publication. No second authentication,
preview, image-message or retry pipeline was introduced.

Boundary/SSOT findings: none introduced in the changed code and direct
dependencies. The release verifies matching immutable tags and dependency
versions; publication uses the tested image and clean tagged sources. The
fixture emits Docker configuration, not a new application or protocol payload.

God-class findings: no new mixed-responsibility class. The existing pool and
manager retain their roles; registration setup was split from coordination.
Dead paths removed: serial observation gap, premature registration-cache
success, duplicate error/close teardown, stale release jobs and the obsolete
directory-launcher reference. Tests align with actual asynchronous failures.

The release source is exported from committed trees with only reviewed changes.
Existing Core storage, SDK native/ownership, Node storage-provider and Chat WIP
are preserved and excluded. iPhone and Swift work is not part of this cut.

## Limits and subsequent gates

The release remains a developer preview on a shared hosted database machine.
The prior intermittent workstation connection failure was not established as
the same fault. Complete the rolling source/identity checks, packaged-client
checks and monitored live soak before declaring deployment accepted. Registry
publication requires an authenticated maintainer and must be checked by
read-back and a fresh consumer install. Desktop application custody/packaging
gates remain separate; Node CLI artifacts are not a desktop chat release.

## Release packaging correction

The rc.11 CI verification, image publication, Linux and both macOS CLI builds passed. Windows exposed an existing `npx` spawn failure: Windows command wrappers cannot be executed directly by `spawnSync` without a shell. The SEA builder now invokes the installed esbuild and postject JavaScript entry points through the current Node executable, preserving arguments without shell parsing. The npm publisher also uses an explicit `./packages/` tarball path rather than an ambiguous repository shorthand. Release rc.12 supersedes rc.11 without changing existing tags. These corrections affect release tooling, not protocol semantics. Required checks are the real platform builds and packaged version smoke tests.
