# Session persistence and DT-302

## Canonical ownership

The SDK owns peer-link/session persistence and receive recovery through
`PeerLinkService`, `createKeyValueBackedPeerLinkStorage`, and
`DeliveryCommitStore`. Node provides filesystem/Postgres storage and runtime
ownership; core owns the crypto objects and strict-read storage contract. Chat
applies durable decrypted work, then acknowledges that application to the SDK.

The receive commit is one SDK-owned WAL record binding the next ratchet state,
replay identity, and decrypted work. Recovery converges each canonical key
independently. A corrupt or unreadable record is not treated as missing.

## Runtime and backend guarantees

- Use one active SDK delivery runtime per filesystem data directory or Postgres
  storage namespace. The runtime must retain its ownership grant until its
  dependency lanes have drained on shutdown.
- Filesystem ownership holds a SQLite rollback-journal EXCLUSIVE transaction
  through Node's built-in `node:sqlite` (desktop Node 22.15 or later). The OS
  releases ownership on process death. A suspended process keeps it; neither a
  heartbeat nor a PID probe may evict it. The lock database is never unlinked
  or renamed, including during release. Use a local filesystem with working
  SQLite locks. This database stores no application data or secrets.
- A live legacy lock refuses upgrade startup; a provably dead legacy lock is
  ignored without unlinking it. Concurrent use of older binaries is unsupported.
- Postgres ownership uses a nonblocking advisory lock and routes every protected
  KV operation through the same checked-out connection. A disconnected former
  owner cannot silently fall back to the pool. Failed acquisition unlocks first,
  or destroys the connection when unlock cannot be confirmed.
- IndexedDB ownership uses a nonblocking Web Lock; a competing tab receives
  `DELIVERY_RUNTIME_ALREADY_ACTIVE`. No browser lock stealing is used.
- Runtime epochs identify recovery generations; lifetime locks provide exclusion.
  Application acknowledgement and pending-work cleanup join the owner lane so
  shutdown drains these writes before releasing ownership.
- Filesystem key-value writes sync the temporary file before rename and the
  containing directory after rename. Deletes also sync the containing directory.
  Failure to prove durability rejects the operation.
- Configure at-rest encryption through the storage provider. The WAL contains
  ratchet state and decrypted work; atomic persistence is not encryption.

## Removed legacy API (breaking change)

DT-302 retires the separately implemented `PersistentSessionManager`,
`FsSessionStore`, `ratchetStateToJson`, and `ratchetStateFromJson` exports from
`@rezprotocol/node`. Their dedicated serializer and tests are removed as well.
No production constructor call sites existed in the Rez workspace.

External consumers of those exports must migrate to the SDK peer-link service
and its storage-provider contract. There is no automatic import of the old
`data/sessions/<sidHex>.json` layout into canonical peer-link storage; do not copy
those files into the new layout or assume this API removal migrates their data.
Existing data files are left untouched.

The governing decision is DT-006 Atomic Commit Feasibility, revision 4,
sections 3.5 and 12.7. This document describes the implemented persistence path,
not release approval or completion of all integration gates.

## Applied replay identity bounds

Applied markers are pruned on owner recovery and at the first/every 64th
application acknowledgement. The current ratchet-only receive implementation
keeps the newest 10,000 eligible applied markers per owner and removes markers
older than seven days at each maintenance pass (up to 63 new markers between
passes). An idle runtime does not run a timer; its next maintenance pass or
restart performs cleanup.
Pending work and remaining WAL records always prevent pruning their marker.
An expired/pruned marker is not evidence to reapply: a new receive intent must
extend the current session version, and old ciphertext must still pass the
ratchet. Full carrier-age/profile policy remains DT-301 scope.

## Portable inbox admission

The registry owns admission and reattestation under one write mutex. Remint
requires the stored original claimant lineage and a generation above the
monotonic reclaimed floor. It purges residual dead-generation mail only after
all checks pass and before publishing the new claim. The sweeper holds the
same mutex through its cleanup. Historical reclaimed tombstones lacking
lineage authority fail closed as terminal; authority is never guessed.
