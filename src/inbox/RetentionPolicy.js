/**
 * RetentionPolicy — fixed per-class retention semantics (portable inbox lease
 * L2, plans/PORTABLE_INBOX_LEASE_SPEC.md §5, frozen at spec approval).
 *
 * Grace is PROVIDER liability and abuse surface, so it belongs to the class,
 * never to the lease: classes can evolve/version, providers advertise which
 * they support, the claimant SELECTS a class, and the class determines grace.
 * No per-lease negotiation.
 *
 *   "transient"  — legacy-identical: lease expiry does NOT drive a retention
 *                  lifecycle (RMailbox retention + caps govern the mail,
 *                  exactly as shipped). Terminal close still works — the kill
 *                  switch is classless.
 *   "standard"   — durable retention: lease expiry opens a fixed grace window
 *                  (renewable), then reclamation; terminal close opens the
 *                  terminal grace window (drain-only), then reclamation.
 *
 * Windows are constructor-configurable so tests and the adversarial spike use
 * short real durations; PRODUCTION DEFAULTS are the constants below. This
 * object holds policy only — no clocks, no timers, no state.
 */

export const RETENTION_CLASSES = Object.freeze(["transient", "standard"]);

const DEFAULT_STANDARD_LEASE_GRACE_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_STANDARD_TERMINAL_GRACE_MS = 7 * 24 * 60 * 60 * 1000;
const DEFAULT_TRANSIENT_TERMINAL_GRACE_MS = 24 * 60 * 60 * 1000;

export class RetentionPolicy {
  #standardLeaseGraceMs;
  #standardTerminalGraceMs;
  #transientTerminalGraceMs;

  constructor({
    standardLeaseGraceMs = DEFAULT_STANDARD_LEASE_GRACE_MS,
    standardTerminalGraceMs = DEFAULT_STANDARD_TERMINAL_GRACE_MS,
    transientTerminalGraceMs = DEFAULT_TRANSIENT_TERMINAL_GRACE_MS,
  } = {}) {
    for (const [name, value] of [
      ["standardLeaseGraceMs", standardLeaseGraceMs],
      ["standardTerminalGraceMs", standardTerminalGraceMs],
      ["transientTerminalGraceMs", transientTerminalGraceMs],
    ]) {
      if (!Number.isFinite(value) || value <= 0) {
        throw new Error("RetentionPolicy requires positive " + name);
      }
    }
    this.#standardLeaseGraceMs = standardLeaseGraceMs;
    this.#standardTerminalGraceMs = standardTerminalGraceMs;
    this.#transientTerminalGraceMs = transientTerminalGraceMs;
  }

  isKnownClass(retentionClass) {
    return RETENTION_CLASSES.includes(retentionClass);
  }

  /** Does lease expiry drive the retention lifecycle for this class? */
  expiryLifecycleApplies(retentionClass) {
    return retentionClass === "standard";
  }

  /** Grace after lease expiry during which a valid renewal restores ACTIVE. */
  leaseGraceMs(retentionClass) {
    if (retentionClass === "standard") return this.#standardLeaseGraceMs;
    throw new Error("leaseGraceMs: expiry lifecycle does not apply to class " + retentionClass);
  }

  /** Drain-only grace after a terminal close, before reclamation. */
  terminalGraceMs(retentionClass) {
    if (retentionClass === "standard") return this.#standardTerminalGraceMs;
    return this.#transientTerminalGraceMs;
  }
}
