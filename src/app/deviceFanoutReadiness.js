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
 *     caps, revoked-cert cap, bounded cert-id/opId formats, no-op detection, journal
 *     retention.
 * Fan-out opens only when ALL are true.
 */
export const FANOUT_SUITE_READY = true; // S2.5 S12: multi-device E2EE suite green.
export const LEGACY_CURSOR_MIGRATION_READY = false; // audit R4 F2: not yet built.
export const DEVICE_ADMISSION_CONTROL_READY = true; // audit R4 F3: SHIPPED (per-account
// active/lifetime/revoked-cert/tombstone caps + cert-id/opId shape guards + no-op
// detection + journal replay retention). Fan-out still gated by F2 below.

export const MULTI_DEVICE_FANOUT_READY =
  FANOUT_SUITE_READY && LEGACY_CURSOR_MIGRATION_READY && DEVICE_ADMISSION_CONTROL_READY;

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
    throw new Error(
      "rez-node per-device fan-out requires unmet release blockers: "
        + unmet.join(", ")
        + ". Refusing to open per-device fan-out (multiDeviceFanout / maxDevices > 1).",
    );
  }
  return MULTI_DEVICE_FANOUT_READY;
}
