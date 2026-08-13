/**
 * Canonical readiness policy for per-device fan-out (E6). The SINGLE SSOT for whether
 * multi-device fan-out may open. EVERY public construction path — validateConfig,
 * createRelayRuntime / createNodeRuntime, bootstrapRelayInfrastructure — MUST consult
 * assertMultiDeviceFanoutReady, so the release-blocker interlock cannot be bypassed by
 * an embedding application that calls a runtime factory directly with
 * multiDeviceFanout:true / maxDevices>1 (audit R4 L2c review P1).
 *
 * Each readiness constant is flipped IN CODE only when its blocker actually ships:
 *   - FANOUT_SUITE_READY: the S12 multi-device E2EE suite is green.
 *   - LEGACY_CURSOR_MIGRATION_READY: audit R4 F2 — an unproven legacy claim cursor
 *     (device_cursors.device_public_key IS NULL) must fail read/drain/ack until a
 *     device.bind backfills its key.
 *   - DEVICE_ADMISSION_CONTROL_READY: audit R4 F3 — per-account active/lifetime device
 *     caps + never-enrolled-tombstone cap, EXACT canonical cert-id + bounded opId shape,
 *     no-op detection, journal retention. (There is NO revoked-cert quota: device.revoke
 *     auto-revokes only the target's own bound cert, lifetime-bounded — finding 1.)
 *   - LEGACY_REVOKE_SERIALIZATION_READY: audit R4 L4 — there must be NO second, un-serialized
 *     revoke writer. RESOLVED by consolidation: the legacy per-inbox device.revoke directive
 *     (DeviceHandler.handleRevoke + DeviceRevokeV1 + the DEVICE_REVOKE wire type) was RETIRED
 *     across rez-core/sdk/node. Revoke is now EXCLUSIVELY the serialized account.deviceMutation
 *     (device.revoke) path, which folds registry status + delivery cursor + tombstone + authority
 *     epoch atomically under the per-account lock (the L3 verifier SSOT). One writer, no split-brain.
 *   - DELEGATED_SESSION_FRESH_REVOCATION_READY: audit R4 L5 — a per-dispatch guard must
 *     refuse a delegated session's post-auth ops once its authorizing device OR any cert in
 *     its chain is revoked, reading ALWAYS-FRESH revocation state (no cache TTL) and closing
 *     the socket terminally, without waiting for reconnect.
 *   - DELEGATED_REVOCATION_COMPLETE_READY: audit R4 F3-remediation round-3 finding 2 — a
 *     delegated device added via device.add stores cert_id=NULL, so revoking it before a
 *     device.bind backfills its leaf cert leaves that leaf capability VALID (nothing to
 *     auto-revoke). Revocation is not complete until every non-primary device carries a
 *     revocable authority-cert binding (e.g. device.add carries + verifies the leaf cert,
 *     or the recheck/off-home state consumes registry-revoked status). This is a DISTINCT
 *     release blocker: L4 (routing) and L5 (fresh-revocation guard) do not supply the missing binding.
 * Fan-out opens only when ALL are true.
 *
 * STATUS 2026-08-13: every blocker is closed. Registration-before-release binds the leaf cert
 * before publication; revocation obligations are durably drained; AccountAuthorityState is
 * root-signed-only at every verification door; monotonic epoch floors prevent observed rollback;
 * record.put and read-repair consult home revocation state. Multi-device fan-out may therefore be
 * enabled explicitly by the operator. The default remains off for backward-compatible personal
 * nodes.
 *
 * NOTE (finding 4): the standalone `capability.revoke` operation
 * (AccountDeviceCapabilityRevokeV1) is deliberately NOT wired at depth-one launch and is
 * tracked as a separate re-delegation prerequisite, NOT folded into L4. It is not a
 * fan-out gate here — arbitrary cert revocation being unavailable is acceptable at launch,
 * whereas COMPLETE device revocation (above) is not.
 *
 * Audit 2026-07-14 (F3-remediation finding 1): L4 and L5 are separate RELEASE BLOCKERS
 * with no representation here before this change. Without their own constants, flipping
 * F2 (the legacy-cursor migration) would silently open fan-out over the still-unbuilt L4
 * and L5 holes — F2 must NOT be the sole remaining gate. Each is its own false constant.
 */
export const FANOUT_SUITE_READY = true; // S2.5 S12: multi-device E2EE suite green.
export const LEGACY_CURSOR_MIGRATION_READY = true; // audit R4 F2: SHIPPED — a legacy,
// UNPROVEN device cursor (device_cursors.device_public_key IS NULL, created by the
// single-device claim path) now FAILS EVERY durable read surface (readAfterCursor /
// readUndelivered / cursorAck AND the random-access mailbox.fetch path via
// PgDurableInbox.assertReadable) whenever the fan-out gate is OPEN (maxDevices > 1), until a
// device.bind backfills the proven device key. All surfaces route through the ONE
// #assertCursorReadable gate. The No-Go audit's P1#1 (mailbox.fetch bypassed the check via
// getEvent) is closed with a real-Pg regression. Gate CLOSED (maxDevices == 1) keeps the
// legacy single-device claim path byte-identical.
export const DEVICE_ADMISSION_CONTROL_READY = true; // audit R4 F3: SHIPPED (per-account
// active/lifetime device caps + never-enrolled-tombstone cap + canonical cert-id/opId
// shape guards + no-op detection + journal replay retention; F3-remediation closed the
// lifetime-cap-on-tombstone bypass + the device.revoke→arbitrary-cert-revoke escalation +
// the fail-close-blocking revoked-cert quota). Fan-out still gated by F2/L4/L5 + the
// finding-2 completeness blocker below.
export const LEGACY_REVOKE_SERIALIZATION_READY = true; // audit R4 L4: SHIPPED via consolidation —
// the legacy per-inbox device.revoke directive was retired (rez-core/sdk/node); revoke is now the
// SOLE serialized account.deviceMutation path (atomic registry+cursor+tombstone+epoch under the
// per-account lock). No second un-serialized writer remains. Fan-out still gated by F2 + the
// completeness blocker below.
export const DELEGATED_SESSION_FRESH_REVOCATION_READY = true; // audit R4 L5: SHIPPED — the
// per-dispatch delegated-authority guard uses the EPOCH FAST PATH (review finding 1): it reads the
// account's monotonic authority epoch (one cheap indexed int) on every delegated frame, and only
// when that epoch has ADVANCED since admission does it pay the heavy path — ONE coherent snapshot
// (terminal device status + revocation state + epoch, read in a single REPEATABLE READ transaction
// so they cannot be mixed across a concurrent commit) plus a full cert-chain re-verify — then
// advances its watermark to that snapshot's epoch. An epoch bumps on every add/revoke, so a
// device/cert revoked mid-session is enforced on the very next dispatch (the socket is then closed
// terminally), while the steady state stays ~1 round-trip with no per-frame crypto. A backend outage
// answers SERVICE_UNAVAILABLE (retryable, socket open), never a false "revoked".
export const DELEGATED_REVOCATION_COMPLETE_READY = true; // audit P0 + follow-ons: SHIPPED.
// AccountAuthorityState is structurally root-signed-only (signer===owner, empty delegation chain),
// so a revoked leaf cannot author the state that decides its validity. The generic record.put door
// and read-repair admission both resolve owner revocation state; a durable per-slot epoch floor
// refuses rollback after observing a newer root snapshot. Adversarial rewrite/rollback tests pin
// those properties independently of the honest outbox flow.
// The No-Go window is closed. The ceremony now REGISTERS before it RELEASES: DeviceLinkApprover
// builds + seals the response, PERSISTS it (P1#2a), submits device.add carrying the new device's own
// inbox binding AND the minted leaf cert, and only then publishes the response that releases the
// leaf. rez-chat ServerDeviceLinkService supplies that device.add (returning the HOME's committed
// registry row, which the approver validates against the leaf it minted) plus the durable
// pending-ceremony journal, so a crash between commit and publish is resumed by republishing the
// exact stored bytes rather than re-minting a cert the home never bound.
//
// PROVEN END-TO-END, not merely unit-green (the previous flip's mistake):
//   - test/e2e.pg.registration-before-release.test.js — real Pg + the REAL ceremony crypto. At the
//     instant the response record is published the home already has the leaf's certId bound; a
//     revoke in the PRE-ONLINE window (no cursor yet) auto-revokes THAT certId; and an OFF-HOME
//     verifyAccountAuthority — which accepts the chain with no revocation state — REJECTS it once
//     given the account's published state.
//   - test/e2e.pg.revoke-propagation.test.js — the other half: the revoke's obligation is enqueued
//     IN the fold transaction, drained under the cluster lease, published as a signed record,
//     stored, and then FETCHED BACK by a peer that never spoke to the home, which opens it and
//     rejects the leaf. "The home knows" and "peers can find out" are now both demonstrated.
//   - test/storage.pg.device-add-then-bind.test.js (L5) — the device.add row and the later
//     device.bind converge on ONE registry row + a proven cursor, while a bind that disagrees on
//     inbox or cert is still a conflict and a revoked device cannot bind at all.

export const MULTI_DEVICE_FANOUT_READY =
  FANOUT_SUITE_READY
  && LEGACY_CURSOR_MIGRATION_READY
  && DEVICE_ADMISSION_CONTROL_READY
  && LEGACY_REVOKE_SERIALIZATION_READY
  && DELEGATED_SESSION_FRESH_REVOCATION_READY
  && DELEGATED_REVOCATION_COMPLETE_READY;

/**
 * Enforce the fan-out interlock at a construction boundary. Throws (fail loud, naming
 * the unmet blockers) when fan-out is REQUESTED but not every blocker is ready — never
 * a silent downgrade, so a node cannot even boot / assemble a runtime advertising
 * fan-out while the revocation work is unbuilt. Returns the effective readiness so a
 * caller can compute its gate (requested AND ready).
 *
 * @param {boolean} requested caller/operator intent to open per-device fan-out
 * @returns {boolean} MULTI_DEVICE_FANOUT_READY
 */
export function assertMultiDeviceFanoutReady(requested) {
  if (requested === true && !MULTI_DEVICE_FANOUT_READY) {
    const unmet = [];
    if (!FANOUT_SUITE_READY) unmet.push("multi-device E2EE suite (S12)");
    if (!LEGACY_CURSOR_MIGRATION_READY) unmet.push("legacy-cursor migration (audit R4 F2)");
    if (!DEVICE_ADMISSION_CONTROL_READY) unmet.push("device admission control (audit R4 F3)");
    if (!LEGACY_REVOKE_SERIALIZATION_READY) unmet.push("delegated-revoke serialization (audit R4 L4)");
    if (!DELEGATED_SESSION_FRESH_REVOCATION_READY) unmet.push("delegated-session fresh-revocation dispatch guard (audit R4 L5)");
    if (!DELEGATED_REVOCATION_COMPLETE_READY) unmet.push("complete delegated-device revocation (audit R4 round-3 finding 2)");
    throw new Error(
      "rez-node per-device fan-out requires unmet release blockers: "
        + unmet.join(", ")
        + ". Refusing to open per-device fan-out (multiDeviceFanout / maxDevices > 1).",
    );
  }
  return MULTI_DEVICE_FANOUT_READY;
}
