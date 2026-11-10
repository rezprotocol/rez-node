# Mailbox recovery repair — 2026-11-10

Status: repair commit `9a2d596` pushed and deployed to all six network processes.
Same-account recovery and valid-image rendering pass; ten-minute live soak is
running. Hold new tester invitations while remaining client issues are open.

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

## Live deployment and recovery evidence

- Image: `rez-network-mailbox:20261110`, image ID
  `sha256:b9eef33e118926b8e79ae8afbb38a2b5948c189a192ca6fd070e277a24283e7b`.
  Only three committed runtime files changed; no dependency, migration or
  client asset update. All six network processes match the same 626-file
  manifest on Node 24.22.2. Manifest SHA-256:
  `b0193a888c6d24c6dc43df79a335c4cbbe87d95895469d439c3c8aac8221ff3b`.
- The first hosted attempt rolled back on public readiness 502. The mounted
  proxy config contained dynamic DNS, but running workers needed a reload.
  After validation and graceful reload, the retry refreshed edge routing
  after each replacement and passed `/ready` and `/mobile/ready`. Previous
  image selection remains at `/opt/rez/releases/mailbox-20261110/`.
- Relay 1's first check raced initialization and rolled back; a bounded
  PID/executable/working-directory check corrected the false failure. All
  three relays now run the repair source. Pinned TLS at each entry and signed
  record propagation across the public core passed.
- The SAME browser account that reproduced the timeout recovered its exact
  original 1,300,068-byte synthetic payload in 19.284 seconds after repair.
  No account reset, operator acknowledgement or deletion. This payload proves
  byte recovery; a separate valid PNG proves decoding and rendering.
- Firefox test runtime 150: a valid **1,702,301-byte PNG** queued while offline
  recovered with exact bytes and rendered at **700×700** in 25.552 seconds
  from reopening/unlocking. This is a recovery check, not a latency target.
  Automated tests did not use the user's Firefox profile.
- Permanent Chrome acceptance now includes offline text, an image larger than
  one frame, exact bytes, real 700×700 rendering and a reverse reply: passed in
  62.1 seconds for the complete multi-account workflow. Updated
  `rez-chat/test/e2e.hosted-web.test.js`; no media runtime or wire change.
- Metadata-only recheck: the original 33-event/6.2 MB production backlog now
  has **zero pending events/bytes**. No operator acknowledgement or deletion.
  Hosted processes remain healthy with zero restarts.

Additional evidence: `mailbox-hosted-rollout-final.log`, `edge-recovery.log`,
`mailbox-r1-rollout-final.log`, `mailbox-r2-rollout.log`,
`mailbox-r3-rollout.log`, `public-relay-hotfix-proof.log`,
`valid-image-recovery.log`, `web-live-media-final.log`,
`production-recovery-check.log`, and `mailbox-live-soak.log`.
