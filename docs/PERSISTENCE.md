# rez-node Session Persistence (Phase 15)

This document describes filesystem-backed session persistence in `rez-node`.

## Scope
- Session records are stored on disk for process restart durability.
- Single-process safety only (no locks or multi-process guarantees).
- Core remains memory-only.

## Root Directory
By default, `FsSessionStore` writes to:

`<repo>/data/sessions/`

This can be overridden by passing `rootDir` to the store constructor.

## File Layout
Each session is stored as:

`rootDir/<sidHex>.json`

## Encoding
- JSON with `v: 1`.
- All `Uint8Array` fields are encoded as base64 strings.
- Ratchet state and skipped-key store entries are serialized deterministically.

## Atomic Writes
Writes use a temp file + rename for atomic replacement.

## Limitations
- No encryption at rest in Phase 15.
- No file locking.
- No multi-process coordination.
