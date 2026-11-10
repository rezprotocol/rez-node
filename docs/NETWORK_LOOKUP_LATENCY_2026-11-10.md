# Hosted lookup latency repair — 2026-11-10

Status: implementation and focused audit pass; live rollout pending.

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

Live rollout, browser startup/message checks and monitored soak are pending.
The serial per-contact refresh still exists, and this does not implement
on-demand media retrieval or image delivery receipts. Desktop packaging and
the previously recorded desktop blockers remain open. Hold new invitations
until the remaining client acceptance work is complete.
