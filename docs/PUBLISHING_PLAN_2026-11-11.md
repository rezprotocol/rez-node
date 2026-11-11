# RezNet developer publishing plan — 2026-11-11

**Plan only. Nothing in this document authorizes publication.**

**Subsequent authorized action:** the maintainer approved the npm-only rc.14 cut.
Core, SDK and Node are now published on `next`; see the
[release report and verification receipt](NPM_RELEASE_2026-11-11.md).
The original proposal below remains a dated record. Container/CLI asset
publication, trusted-publisher automation and deployment are still separate.

The cleanup is committed locally on the maintainer’s request and has not been pushed. No package was staged or published, no
image was pushed, no GitHub release or Git tag was created or edited, no registry visibility
was changed, and no live node was redeployed during this cleanup. Existing
rc.13 artifacts and production instances retain their current contents.

## Proposed first cut

Use **0.6.0-rc.14**, subject to a final version-availability check, for the
coordinated Core/SDK/Node developer cut. Current local test artifacts retain
rc.13 metadata solely for compatibility checks; they must never replace the
existing rc.13 releases. Bump all three versions, exact internal dependencies
and the coordinated lock together only after this plan is approved.

| Artifact | Canonical source / owner | Proposed public channel |
| --- | --- | --- |
| `@rezprotocol/core` | `rezprotocol/rez-core` | npm public, `next` |
| `@rezprotocol/sdk` | `rezprotocol/rez-sdk` | npm public, `next` |
| `@rezprotocol/node` | `rezprotocol/rez-node` | npm public, `next` |
| Node image | `rezprotocol/rez-node`, canonical Dockerfile | Versioned GitHub archive; GHCR after separate visibility gate |
| Node CLI archives | `rezprotocol/rez-node`, shared verified source cut | GitHub developer prerelease, Linux/Windows x64, macOS arm64/x64 |

No `latest` promotion, Homebrew publication, new Chat desktop installer, iPhone,
Swift, app-store distribution, contract deployment or automatic production
rollout belongs to this first cut. Existing clients continue to use the hosted
network. The developer install advertised after acceptance is
`npm install @rezprotocol/sdk@next` or an explicit approved version.

## 1. Review the local cleanup before any external write

Review the Docker/package changes and the cleanup verification report. The
Node workflow now prepares artifacts only: it retains complete Core/SDK/Node,
isolated recovery, vulnerability/secret and platform binary gates, but contains
no registry pushes, npm publishers or GitHub release writes. It uploads only
CI artifacts. This change has not been pushed; the currently remote rc.13
workflow is still the previous automatically publishing workflow.

The local cleanup commits contain only the reviewed paths and preserve all
unrelated working changes. Before pushing any version tag, ensure the remote
workflow is the reviewed preparation-only version. Never use a tag push as an
implicit approval to publish or deploy. Do not force-update existing tags.

## 2. Establish publisher ownership and authentication

A maintainer must confirm ownership/publish permissions for the `@rezprotocol`
scope and the three package names, sign in interactively and enable 2FA. Do not
send credentials in chat or copy them into Docker/build contexts. The last
publication attempt failed with ENEEDAUTH; this cleanup does not assume access
has since been configured.

Prefer npm's staged approval flow for the initial bootstrap: a maintainer
uploads the inspected archive, reviews its digest and explicitly approves it
with 2FA. **Even staging a brand-new package creates a public `0.0.0-stage`
placeholder.** Do not run that command before approval. Staging needs Node
22.14+ and npm 11.15+; the inspected local npm is 11.10.1, so the publishing CLI
must be upgraded/pinned in its own approved environment first. The server's
Node runtime need not change. [npm staged publishing](https://docs.npmjs.com/staged-publishing/).

The initial interactive bootstrap does not automatically provide npm CI
provenance. Retain the approved source commits, CI run, archive checksums and
independent checks as the first-cut evidence; do not advertise a provenance
badge that was not actually generated. If npm provenance is required for the
very first real version, stop before approval and complete the owning-repo
trusted-publisher bootstrap instead of silently dropping that requirement.

## 3. Configure ongoing approval-based trusted publishing

After bootstrap, configure a GitHub-hosted, stage-only trusted publisher for
each owning repository, with an exact workflow filename and protected release
environment. Keep direct publication disabled for automation; the maintainer
approves staged versions. Avoid a permanent shared NPM_TOKEN. Enable tighter
token restrictions after the trusted flow is proved. New trust configurations
need their first successful publish within two days, so configure them during
the approved release session, not weeks ahead. [npm trusted publishers](https://docs.npmjs.com/trusted-publishers/).

**One coordinated preparation process; package ownership stays per repository.**
The Node coordinator builds/tests the entire source cut and produces all three
archives once. A small owning-repo publisher consumes only its approved archive
and delegates shared checks to one pinned Node-owned reusable implementation.
It must verify the preparation run, package name/version, its own source commit
and archive digest before staging. Register the calling workflow's identity and
permissions correctly when using reusable workflows. Do not recreate three
independent build/test/release pipelines.

Core must publish with the Core repository identity; SDK with SDK; Node with
Node. npm requires repository metadata to match the publishing repository for
provenance/trusted publication. Correct repository metadata is now present in
the local manifests. Do not point all three packages at Node just to make a
central OIDC publisher work. [npm provenance requirements](https://docs.npmjs.com/generating-provenance-statements/).

These owning-repo staging wrappers and the shared approval-based promoter are
**proposed next implementation**, not configured or deployed by this cleanup.
The preparation workflow has no credentials or permission to perform them.

## 4. Produce and freeze one review bundle

From clean reviewed commits, make the coordinated rc.14 version/dependency
change and source tags. The preparation pipeline checks matching versions,
uses the existing locked three-repository graph and installs at its root.
Build with the pinned base, no production configuration, no secret build args
and no live-container snapshots. Keep builder caches private/local.

Required bundle:

- Exact tested image archive and content digest; source and dependency-lock hashes.
- Actual Core, SDK and Node `.tgz` archives, their pack metadata and SHA-256/SHA-512 digests.
- Full-suite and isolated destructive recovery results; real CLI platform startup checks.
- Every-layer image hygiene and package hygiene results; HIGH/CRITICAL vulnerability and secret scan results.
- Dependency inventory/SBOM and source/build provenance where supported; exact evidence links.
- Release notes distinguishing developer-beta support and known native-client limitations.

Pack once. Scan and test those bytes, retain them, and promote those same bytes.
Never repack on the publishing machine or rebuild the image while promoting it.
Before release, exercise a fresh consumer from the archives: imports, CLI,
authenticated SDK connection/reconnect, and hosted delivery/recovery gates.

## 5. Approve and publish npm packages in dependency order

**Future authorized actions only**, after the review bundle and account setup:

1. Stage the inspected Core archive with public access and `--tag next`.
2. Review the staged name, version, source/digest and tag; approve with 2FA.
3. Read back Core's public `dist.integrity` and version. Then stage/approve SDK.
4. Read back SDK and prove a fresh registry-only SDK install. Then stage/approve Node.
5. Read back all three package integrities and `next` tags; compare to the frozen bundle.

The `next` tag is intentional. npm's default publishing tag is `latest`; the
local manifests specify `next` as an additional safeguard. Stable promotion
needs a separate decision. [npm distribution tags](https://docs.npmjs.com/adding-dist-tags-to-packages/).

Proposed command shape (not executed):

```sh
npm stage publish ./approved-package.tgz --access public --tag next
npm stage view <stage-id>
npm stage approve <stage-id>
```

Use the exact inspected filename/digest from the bundle; placeholders above are
not runnable release instructions. Staged tags are fixed for that stage, so
check `next` before submission. [npm stage command](https://docs.npmjs.com/cli/v11/commands/npm-stage/).

An interruption is not atomic rollback: Core may succeed before SDK/Node. Keep
the bundle and resume only missing steps. An existing matching integrity is
already complete; a mismatching existing version is a hard stop. Do not
unpublish, overwrite versions or move tags to conceal a partial release.
Announce the developer install only after all required packages pass readback.

## 6. Release the image and Node CLI assets

First publish the exact verified archives/checksums and acceptance notes as a
GitHub **developer prerelease**, after approval. The image archive supports
`docker load` without opening registry access. Check downloaded bytes from a
fresh consumer. Do not claim new desktop signing/notarization or GUI acceptance.

For GHCR, retain the intended name `ghcr.io/rezprotocol/node` only after a
package administrator inventories every existing version. **Visibility applies
to the package, not just the clean tag.** Approving rc.14 must not inadvertently
expose unaudited older versions. Audit retained versions before changing
visibility; if that is impossible, keep the current package private and propose
a fresh public package name for explicit approval. Do not delete old versions
or change package visibility as a side effect of the cleanup.

After the separate visibility approval, push/promote the exact verified digest
without rebuilding, verify anonymous pull in a clean unauthenticated client,
then publish the tested installation instructions. GitHub's public-visibility
change cannot simply be reversed. [GitHub package visibility](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility).

## 7. Treat deployment as another approval

Publishing an SDK/image is independent of changing the hosted network. If a
production update is later authorized, retain the current images and private
backups, roll one node at a time, compare identities, reload the validated
proxy and require public readiness before continuing. Update all three public
relays with the same verified source/dependency graph and compatible glibc
runtime. Run client acceptance and the monitored soak. No live fault injection.

A failed release readback stops announcement; a failed rollout invokes the
retained runtime rollback. Nothing in this plan revokes the existing iPhone,
Swift or desktop release boundaries.

## Review decisions and current blockers

| Decision or prerequisite | Proposed disposition |
| --- | --- |
| Version | rc.14 after availability check; no current version/tag changes |
| npm channel | public `next`; stable `latest` stays separate |
| Initial account bootstrap | maintainer interactive staging/2FA; public-placeholder creation requires approval |
| First-cut npm provenance | bootstrap may lack it; require owning-repo OIDC first if it is a release gate |
| Ongoing publishers | per-owning-repository stage-only OIDC, one shared pinned implementation |
| npm CLI | approved isolated version >=11.15 for staging; current local 11.10.1 is insufficient |
| Container registry | administrator inventory and separate visibility approval; public archive can ship first |
| Remote automation | replace the existing remote auto-publisher with reviewed preparation-only flow before tagging |
| Production | separate deployment approval; current rc.13 deployment stays unchanged |
| Native/mobile | outside this cut; preserve all work in progress |

No administrator settings, credentials, registry ownership or future platform
provenance flow was exercised here. The reviewed cleanup and concrete gates are
ready for a decision; publishing is deliberately unexecuted.
