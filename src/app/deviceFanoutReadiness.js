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
 * STATUS 2026-07-15 (post No-Go audit): F2 (legacy-cursor fail-close) is complete — the
 * mailbox.fetch bypass found by the audit (P1#1) is closed, so ALL durable read surfaces
 * now fail closed for unproven/revoked cursors. BUT completeness (below) was reverted to
 * false: the device-link CEREMONY (rez-sdk DeviceLinkApprover / rez-chat
 * ServerDeviceLinkService) mints + publishes a device's leaf cert BEFORE any home mutation
 * binds its certId, so a device can hold + use its leaf before the home can auto-revoke it —
 * off-home verifiers never learn which cert to reject (audit P1#2, registration-before-release
 * not implemented). So MULTI_DEVICE_FANOUT_READY is FALSE again. Additional app-layer
 * prerequisites the audit raised before an operator enables fan-out (NOT node readiness
 * constants, tracked separately): durable revocation-propagation outbox (P1#3) and the
 * open-gate no-silent-downgrade sender invariant (P1#4).
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
export const DELEGATED_REVOCATION_COMPLETE_READY = false; // audit R4 No-Go P1#2: REVERTED to false.
// The node-side MECHANISM exists (device.add carries + verifies the leaf cert and the home stores its
// certId; a delegated device.bind binds authority.leafCertId), BUT the actual device-link CEREMONY
// does not use it safely: rez-sdk DeviceLinkApprover.approve() mints + publishes the leaf cert to the
// new device (releasing it) BEFORE any home mutation binds its certId, and rez-chat
// ServerDeviceLinkService calls approve() without submitting device.add. So there is a WINDOW where
// the new device holds + can use its leaf cert while the home has NOT recorded its certId — if the
// account revokes in that window the home can tombstone the deviceId but off-home verifiers never
// learn WHICH cert to reject (the leaf stays valid to peers). Registration-before-release is not yet
// implemented. Re-flip only once the ceremony binds the certId at the home BEFORE the leaf is usable,
// proven end-to-end.

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
