# Mailbox recovery repair — 2026-11-10

Status: candidate verified; live rollout and same-account browser recovery
pending. Hold new tester invitations until the incident is resolved.

## Failure

The deployed browser SDK caps one WebSocket frame at 1,000,000 bytes.
`MailboxHandler` returned up to 50 durable events inline without a byte budget.
A production metadata-only query found 33 pending events totaling 6,191,633
stored bytes; the largest was 558,170 bytes. Base64 expands the response still
further. No message content or account secrets were read to obtain these counts.

The old client silently discards oversized responses, leaving `mailbox.list`
to time out. Startup awaits this drain, and periodic catch-up repeats the
failure. Ordinary health/readiness checks do not exercise it. A live isolated
Firefox test reproduced the exact startup error after queuing a 1,300,068-byte
image for an offline recipient. That same browser account is retained for the
post-rollout recovery proof.

File transfer already uses 64 KiB chunks; catch-up wrongly bundled many into
one oversized reply. Avatars also use a separate inline payload path. This
repair does not implement on-demand media fetching or certify avatar updates.

## Files changed

- `src/protocol/handlers/MailboxHandler.js`: cap durable lists at 50 items and
  700,000 raw body bytes, reserving base64/framing headroom below the client cap.
- `src/storage/pg/PgDurableInbox.js`: select a contiguous byte-bounded prefix
  before advancing `last_delivered`, within the existing transaction/lock.
- `src/storage/DurableInbox.js`: document the optional read-byte budget.
- `test/handler.mailbox-list-durable.test.js`: large backlog, exact bytes,
  replay, delivered-bound ack, retained events, oversize errors and count caps.
- `docs/HOSTED_NODE_DESIGN.md` and this report: contract and acceptance evidence.

No migration, payload rewrite, decryption, cursor-identity change or account
reset. Existing provider, iPhone, Swift and client work is excluded.

## Verification and focused audit

- Before repair: the real-PostgreSQL regression failed on the oversized reply.
- Focused PostgreSQL suites: **52 pass, zero fail or skipped**.
- Clean full server run: **1,397 pass, zero fail**, with two opt-in network
  gates invoked separately: **2 pass, zero fail or skipped**. Thus all 1,399
  tests ran across these commands; the focused counts overlap.
- `git diff --check`: pass.
- Specs checked: canonical `HOSTED_NODE_DESIGN.md`; `DurableInbox` read/ack
  contract; PostgreSQL delivered-watermark/revocation gates; shipped SDK
  `WsTransport` cap; `MailboxListResponse`; outer-packet codec.
- Canonical owners: Node owns mailbox RPC framing and durable storage;
  PostgreSQL remains event/cursor SSOT. SDK/Core retain client transport and
  cryptography. No app/media semantics move into Node.
- Duplicate/split-brain: one protocol budget, selected by the storage owner
  under its existing lock. No shadow queue, cursor or acknowledgement path.
- Boundaries/SSOT: no new wire shape or dependency, no server access to client
  secrets. Registration, revocation and delivered-bound ack gates stay intact.
- God-class findings: none; selection stays within the owning read method.
- Dead code removed: unbounded returned-row mapping replaced by bounded-prefix
  selection; no unrelated cleanup. No unresolved touched-code audit finding.

## Remaining limits

A single event above 700,000 bytes produces explicit `INBOX_CAP_EXCEEDED`,
without cursor advance or deletion. Such legacy inline events need a bounded
fetch/chunking design. Normal chunked files can span many pages. The observed
production backlog's largest event fits this repair.

Image receipts remain a separate defect: a live test received exact image
bytes while the sender stayed `sent` for 30 seconds. Serial contact publication
and cold record lookups remain startup/first-message latency work. The exact
30–60 second reconnect delay is not fully attributed. Desktop release blockers
also remain; this is not a declaration of general tester readiness.

Evidence: `/tmp/rez-client-acceptance-20261110/`, particularly
`mailbox-sizes.log`, `mailbox-before.log`, `mailbox-final.log`,
`node-clean-hotfix.log`, `explicit-network-gates.log` and
`offline-image-recovery-final.log`.
