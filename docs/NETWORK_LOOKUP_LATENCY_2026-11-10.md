# Hosted lookup latency repair — 2026-11-10

Status: committed, pushed and deployed on all six network processes; live
lookup checks and two independent ten-minute soaks pass. Earlier intermittent
workstation connection failures remain unattributed, as recorded below.

## Diagnosis and scope

The mailbox byte-budget repair restores login, but contact publication and
cold record reads still block startup. Direct synthetic SDK probes, inside
each hosted container, measured record writes at 3,115 / 3,128 / 3,188 ms
and absent-record reads at 3,105 / 3,099 / 3,097 ms. Every write received five
remote replica acknowledgements. An isolated client of all six public nodes
published in 136 ms with six acknowledgements, isolating the difference to
the hosted node's own lookup path rather than browser cryptography.

K-buckets already exclude this node's identity. Iterative lookup did not:
another peer can legitimately return our own canonical identity, which was
then admitted as a remote candidate. A self-connection can authenticate and
then close on the authoritative-self descriptor guard, leaving a query to
expire. New regression tests fail on the old implementation because both
node and value lookup dial and query themselves.

This repair excludes self references before candidate resolution, sharing
the local-identity predicate with the routing table. It does not change
timeouts, skip readiness, reduce replica requirements or weaken identity,
signature, same-socket, revocation or storage checks. Live before/after
timings must establish how much of the reported delay this removes.

## Files and required audit

- `src/routing/dht/KBucketTable.js`: own the self-node predicate and reuse it
  for routing-table admission.
- `src/routing/dht/DhtLookup.js`: reject self references after canonical
  identity/hash validation, before dialing, and count them as rejected.
- `test/routing.dht-lookup.test.js`: two regressions cover node/value lookup,
  zero self-dials, zero self-queries, returned peers and honest counters.
- This report records verification, deployment and remaining limits.

Verification: **64 focused tests pass**, zero failures/skips. Clean candidate
server suite with real PostgreSQL 17 and Redis, including the real local
four-node mesh: **1,400 pass**, zero failures, one opt-in hosted gate pending.
The two new regressions fail before the repair. `git diff --check` passes.

Specs checked: `rez-core/docs/ATLAS_PREREQUISITES.md` P3/P4, canonical
`ADR-RELAY-IDENTITY.md`, routing-table self exclusion, durable-record put
and recursive-read contracts, peer-bind authoritative-self admission and
query waiter's same-socket guard.

Canonical owners: Node owns lookup/routing and authenticated connection
resolution; Core retains cryptographic identity derivation and signed
records. Duplicate/split-brain findings: self comparison has one owner in
the routing table; no second identity, cache, resolver or lookup path.
Boundary/SSOT findings: no new record shape, dependency, transport dispatch
or app/protocol crossover. God-class findings: none in changed code or
direct dependencies. Dead code removed: none; existing invalid-reference
and budget paths remain required. Tests added: the two lookup regressions.
No unresolved finding in the touched-code audit.

Unrelated storage-provider changes, Chat, iPhone and Swift work are excluded.
Evidence is under `/tmp/rez-client-acceptance-20261110/`, principally
`hosted-record-profile.log`, `public-six-record-profile.log`,
`self-lookup-before.log`, `self-lookup-focused.log` and
`routing-clean-suite.log`.

## Remaining risks

Commit `e046034` is deployed as `rez-network-routing:20261110` (image
`sha256:2b24c25148c795460867d35fd046d6492f22a228cf3d7f2add3d78bcbb22a866`).
Hosted processes rolled at 22:17 UTC and the three DigitalOcean relays at
22:18 UTC. All six match the same 626-file runtime source manifest
(`45d53b1038febb38439d5668b676b81d480d68cbf4249bd5b5b11277333f8e29`)
and Node 24.22.2. Health checks pass with no unexpected restarts.

The same per-container probe now measures writes at **18 / 23 / 25 ms**,
still with five remote acknowledgements, and absent reads at **7 / 9 / 11 ms**.
Firefox existing-contact unlock was **1.592 s**, first message **321 ms**;
five-contact/avatar refresh was **3.216 s**. These are synthetic measurements,
not a promise for every user connection. Pinned TLS and cross-core signed
record checks pass.

One workstation ten-minute run delivered/acknowledged **257/257** messages
with **52 reconnects**, 25 tenant-isolation checks and worst delivery 403 ms.
An independent Node 24 run from relay r1 delivered/acknowledged **285/285**
with **57 reconnects**, 28 isolation checks and worst delivery 590 ms over
600 seconds. Both used two hosted nodes and passed.

Two other workstation runs failed: one waited for a hosted frame, another
timed out opening a new TCP connection to port 443. These are not counted as
passes or proven fixed by the lookup repair. Server health/listener/firewall
checks found no matching overload or restart. Subsequent fresh HTTPS probes
passed **89/89** from the workstation and **35/35** from r1. SYN metadata
alone did not establish packet-loss causality; that intermittent path remains
unattributed. Logs: `routing-live-soak-debug.log`, `remote-soak-status.log`,
`routing-live-soak.log`, `routing-media-load-soak.log`,
`edge-network-diagnostic.log`, `public-connection-probe.log`,
`relay-edge-probe.log` in the evidence directory above.

The serial per-contact refresh still exists, and this does not implement
on-demand media retrieval or image delivery receipts. Desktop packaging and
the previously recorded desktop blockers remain open. Hold new invitations
until the remaining client acceptance work is complete.
