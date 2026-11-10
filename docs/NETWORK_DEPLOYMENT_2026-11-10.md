# Network deployment and live verification

The network candidate is deployed to the hosted account nodes, portable
provider and all three DigitalOcean relays. The 30-minute monitored live soak
passed with 777 exact-byte deliveries, 777 durable acknowledgements, 156
recipient reconnects across both account nodes and 77 tenant-isolation checks.
The iPhone, Swift and Chat work remains untouched. A small, monitored
invite-only pilot is supported by these results; the operating limits below
still apply.

## Coordinated candidate

| Owner | Committed source |
| --- | --- |
| Core | `89c7d77769dba50060abaad0195404aff23f997d` |
| SDK | `af6fac3ecbe32a180a9b9d590d8da49aa3a8681b` |
| Node runtime | `c66cffb184c34da368243507a768c5a7123578ae` |
| Node live soak runner | `d935e194fca894d588c927da8468437846e4df7c` |

These commits are pushed to each repository's existing `feat/hosted-cluster`
branch. They are not merged to main or published to npm. All three package
manifests identify `0.6.0-rc.10`.

The release excludes the pre-existing Core storage work, SDK native/IndexedDB
and ownership changes, Node provider credential work, and Chat changes. It was
exported from committed source and verified separately from the dirty working
tree described in `NETWORK_RECOVERY_VERIFICATION_2026-11-10.md`.

Hosted image: `rez-network-release:20261110`.
The hosted Docker engine reports image ID
`sha256:edbaeffa2be438198ebfffad6a2778de76bdbc3d58d18b10002d3b27a47bb696`.
The transferred image archive was verified by SHA-256 before loading. The relay
runtime files were exported from this same image. All six running processes
were checked against the same 626-file source manifest and run Node 24.22.2;
the relay binary matched the official Node distribution checksum.

The source manifest SHA-256 is
`f30027a12c2397b735134e35a48c9357f46b2ab66ff70cca15726ad30f2bb916`.

## Deployed services

| DigitalOcean server | Runtime selection |
| --- | --- |
| `rez-chat-rc1`, `206.189.185.100` | `node1`, `node2`, `portable1` use the same candidate image |
| `r1.rezprotocol.io`, `134.209.119.210` | systemd selects the candidate app and Node binary |
| `r2.rezprotocol.io`, `147.182.162.134` | systemd selects the candidate app and Node binary |
| `r3.rezprotocol.io`, `157.230.213.181` | systemd selects the candidate app and Node binary |

Original mesh identities, data volumes, routing configuration and TLS endpoints
were preserved. The three relays were rolled one at a time, with public TLS
and pinned-identity checks between upgrades. No database migration was needed:
the live database and candidate already shared migrations 0001–0026.

The hosted cutover preserved rollback images, a consistent PostgreSQL dump,
private configuration and stopped-volume snapshots. The account nodes and
portable provider were briefly stopped for the coordinated cutover. The Redis
credential was rotated after an inspection inadvertently included the old
value in its output; the replacement stayed on the server. Readiness verified
the new authenticated Redis connections, and the retired credential was
separately verified as rejected.

The edge container, application assets, phone/native sources and client builds
were not replaced. Updating `portable1` changed its generic network server
runtime, not the iPhone application.

Hosted release artifacts and private rollback data are under
`/opt/rez/releases/network-20261110/`. The persisted Compose override selects
the candidate image and builds from that release's clean `context/`, rather
than rebuilding the older checkout. Default account image aliases and the
portable image setting also select the candidate for certificate renewal. The
existing renewal hook passed syntax validation; certificate issuance was not
forced. The hosted certificate expires January 10, 2027.

Each relay uses `/opt/rez-relay/releases/network-20261110/`, selected by the
service's `90-network-release.conf` drop-in. Original source, configuration and
service files remain available for rollback. No volume removal or identity
reset was performed on a live server.

## Verification

| Gate | Result |
| --- | --- |
| Clean committed Core suite | 581 passed, 0 failed, 0 skipped |
| Clean committed SDK suite | 533 passed, 0 failed, 0 skipped |
| Clean committed Node suite with PostgreSQL, Redis, live mesh and hosted TLS enabled | 1,397 passed, 0 failed, 0 skipped |
| Clean candidate destructive recovery fixture | 5 passed, 0 failed, 0 skipped |
| Final candidate image vulnerability scan | 0 reported HIGH or CRITICAL findings |
| All six deployed runtime graphs | All 626 files matched committed source; Node 24.22.2 |
| Live hosted gate before cutover | Passed |
| Public relay TLS and pinned peer authentication before/after each relay update | Passed for all three original identities |
| Signed record publication/custody/retrieval through different public entry relays | Passed; exact signature retained |
| Live portable-provider controlled restart | Passed; original identity, exact packet bytes and claimant-only access retained |
| Live portable-provider tenant isolation | Passed; foreign claimant received `FORBIDDEN` |
| Production logical backup restored into an isolated database | 19 tables, 3,852 rows; both encrypted KV rows decrypted with the retained key |
| Monitored 30-minute hosted soak | 2 tests passed, 0 failed, 0 skipped; 777/777 packets acknowledged; 156 reconnects; 77 tenant refusals; both nodes visited |

The destructive local fixture verified simultaneous node SIGKILL, Redis loss
and recovery without client reconnection, PostgreSQL blackhole/restart with
bounded readiness and retryable errors, and restoration into another database
with exact ciphertext, cursor and tenant isolation preserved. No destructive
database or Redis outage was induced on the live service.

The live hosted soak continuously deposits random opaque packet bytes, requires
the exact bytes to arrive, durably acknowledges every sequence, reconnects the
recipient every five packets, checks that acknowledged packets do not reappear,
requires recipient connections to visit both account nodes, and checks public
readiness and cross-tenant refusal every ten packets. Server snapshots monitor
readiness, restarts, memory, CPU, peers and deposit counters. Expected tenant
refusals increment the error counter; they are not backend failures.
The final soak recorded no missing or mismatched packet bytes and no replay
below the acknowledged cursor. Its worst measured deposit-to-ack round trip
was 625 ms. Sampled hosted processes remained healthy with zero unexpected
restarts or OOM kills and memory usage below 70 MiB each; these measurements
describe this low-volume pilot load, not maximum capacity.

The public mesh proof used two temporary nodes connected through different
production relays. A relay acknowledged signed-record custody, and the other
node retrieved the exact signature through the public mesh. These temporary
nodes were stopped and their local state removed.

The portable proof used the existing SDK claimant protocol with synthetic keys,
without account identity or device fields. It retained one packet across a
controlled provider restart, then verified the original provider identity and
the exact packet through fetch. No iPhone, Swift or application code ran in
these network checks.

## Backups and recovery copies

Daily DigitalOcean backups are enabled for hosted droplet `592168339`, with
seven-day retention. The user approved the expected additional $7.20/month:
the $24/month droplet plus the daily plan's 30% surcharge, described in
[DigitalOcean's backup pricing](https://docs.digitalocean.com/products/backups/details/pricing/).
Action `3535886787` completed successfully. The first scheduled window is
November 11, 2026, 08:00–12:00 UTC (02:00–06:00 Central); enabling the schedule
does not mean a first scheduled image has already been created.

Immediate encrypted off-server recovery copies are saved in
`/Users/noah/code/personal/rezprotocol/artifacts/network-release-20261110/`.
They cover the hosted database dumps, original node identities, configuration,
TLS material and clean build context, plus each relay's preserved identity,
state, service and proxy/TLS configuration. These files are encrypted to the
existing Rez GPG public key, fingerprint
`7F3C2DD28199C8E018849A40EF802999DB14B544`, and require its matching private key.
No plaintext backup was saved locally or committed to Git.

Database restoration/decryption was verified separately on the server. The
GPG-encrypted archives were checked for the intended recipient and stored with
restricted permissions; an end-to-end decrypt/restore of those archives was
not attempted. The next scheduled DigitalOcean backup must still complete.

## Focused audit and changes

Files changed in this deployment continuation: Node
`src/inbox/DepositRateLimitStore.js`,
`test/handler.deposit.ratelimit.low4.test.js`,
`test/e2e.hosted-compose.test.js`, `docs/HOSTED_OPERATIONS.md`, this receipt and
the link from the earlier local verification receipt. The earlier receipt
contains the original Core/SDK/Node fix inventory. Deployment also added the
versioned server runtime/context directories, Compose selection, relay service
drop-ins and protected backup files described above. No lint, formatting,
dependency or client tooling configuration changed.

The clean restore drill exposed one further recovery race: a node enumerates
an expired rate-limit row, another node prunes it, then the first reads absence
and reports corruption. Hydration now accepts only genuine absence in that
case. Present malformed counters and unreadable storage still fail closed;
retained live budgets remain enforced. Regression tests cover concurrent
pruning, preserved limits and private error messages. The fix and its direct
strict-read/expiry dependencies passed the focused audit and full clean suite.

Specs checked: repository `AGENTS.md`; Node `HOSTED_NODE_DESIGN.md`,
`HOSTED_OPERATIONS.md`, `RUN_A_CLUSTER.md`; Core
`RECEIPTS_AND_DELIVERY_STATES.md`, `STORAGE_MODEL.md` and
`adr/ADR-RELAY-IDENTITY.md`. The operations document now describes the
concurrent-pruning rule. No shared-writer schema change or identity migration
was introduced.

Canonical owners remain Core for shared records/crypto, Node for server
recovery, backend validation, deployment and relay runtime, and SDK for the
application facade. Neither Chat/UI nor a native runtime drives network
maintenance. Existing generic dispatch and record schemas are preserved.

Duplicate/split-brain findings: the initial mixed server images are replaced
by one coordinated runtime graph. The SDK crypto duplicate and queue eviction
helpers were removed in the earlier commits. No new duplicate storage,
recovery scheduler or protocol schema was introduced. Redis remains soft
state; PostgreSQL remains the hosted durable owner.

Boundary/SSOT findings: none introduced in the changed code or its direct
dependencies. The new soak uses existing authentication, claim, bind, list and
ack helpers. It tests the server independently of client work in progress.

God-class findings: no new mixed responsibility was introduced. Existing large
session, inbox and peer-link classes remain as reported in the earlier audit;
broader decomposition was not performed. Dead code removed remains the earlier
crypto duplicate, queue eviction helpers and obsolete mesh hook. Old deployed
images/source are retained explicitly for rollback, not selected as active
runtime paths.

Pass/fail: all listed gates passed, including the completed soak. Remaining
risks and follow-ups are below.

## Operating limits

- This is an invite-only candidate, with a bounded live soak rather than a
  long-duration availability or capacity certification. Application/mobile
  release acceptance remains separate.
- The hosted account nodes and PostgreSQL still share one physical droplet.
  Daily disk backups and logical dumps provide recovery, not cross-host
  PostgreSQL failover or continuous WAL/PITR. Neither was implemented here.
- Queue expiry, retry exhaustion, ambiguous-ack retries and best-effort live
  status retain their documented semantics. A transport acceptance is not a
  recipient's end-to-end delivery acknowledgement.
- Mesh identities and encryption keys remain critical recovery material.
  Online storage-key rotation and a separate node credential vault remain
  outside this change.
- No general OS, database/Redis/edge package upgrade, whole-protocol
  cryptographic review or third-party security audit was performed. Hosted
  edge/application assets and existing relay privilege settings were retained.
- Live probes leave small synthetic claim/cursor and packet records under the
  normal retention policies. No other tenant's records were purged.

Local evidence is under `/tmp/rez-live-release-20261110/`. Public verification
receipts are retained with the encrypted recovery copies; private server
configuration and key material are never included in the public receipt.
