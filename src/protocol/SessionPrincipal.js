/**
 * SessionPrincipal — the single, immutable identity of an authenticated gateway
 * session (SESSION_AUTH_V5 slice 1; plans/SESSION_AUTH_V5_PHASE0.md §2).
 *
 * A session holds exactly ONE principal, committed atomically when
 * authentication COMPLETES. Kinds are orthogonal, never hierarchical:
 *
 *   ACCOUNT  — account-control authority. Carries the account identity key,
 *              the session device id, and the verified session authority
 *              (the S2.5 dual-mode direct/delegated result) as a sub-shape.
 *   CLAIMANT — mailbox capability authority rooted at one claimant key.
 *              No production constructor path exists in slice 1; the v5
 *              handshake (slice 2) is its first caller. It exists now so the
 *              dispatcher's deny paths are testable before any wire change.
 *
 * There is no principal upgrade and no dual principal. v4 compatibility allows
 * atomic REPLACEMENT (P → P′) when a full re-authentication completes on the
 * same socket — the session slot swaps one frozen principal for another; the
 * v5 handshake slice forbids replacement for v5 sessions.
 */

const KIND_ACCOUNT = "ACCOUNT";
const KIND_CLAIMANT = "CLAIMANT";

function requireNonEmptyString(value, message) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(message);
  }
  return value.trim();
}

export class SessionPrincipal {
  static KINDS = Object.freeze({ ACCOUNT: KIND_ACCOUNT, CLAIMANT: KIND_CLAIMANT });

  /**
   * One constructor; the invariant is intrinsic to it. Use the static
   * factories — they exist for call-site clarity, not to relax validation.
   *
   * @param {object} opts
   * @param {string} opts.kind — SessionPrincipal.KINDS.*
   * @param {string} [opts.accountPublicKeyB64] — ACCOUNT only, required there
   * @param {string} [opts.sessionDeviceId] — ACCOUNT only, required there
   * @param {object} [opts.authority] — ACCOUNT only, required there: the
   *   verified dual-mode session-auth result ({ mode: "direct" | "delegated",
   *   accountIdentityPublicKeyB64, signerPublicKeyB64, grantedCapabilities,
   *   leafCertId, ... }). Deep-frozen here so nothing can mutate the grant
   *   after admission (audit leaf-3c F2 — freezing moved from the commit site
   *   to construction, adjacent to where the object becomes authoritative).
   * @param {string} [opts.claimantPublicKeyB64] — CLAIMANT only, required there
   */
  constructor({ kind, accountPublicKeyB64, sessionDeviceId, authority, claimantPublicKeyB64 } = {}) {
    if (kind === KIND_ACCOUNT) {
      this.kind = KIND_ACCOUNT;
      this.accountPublicKeyB64 = requireNonEmptyString(
        accountPublicKeyB64,
        "SessionPrincipal ACCOUNT requires accountPublicKeyB64",
      );
      this.sessionDeviceId = requireNonEmptyString(
        sessionDeviceId,
        "SessionPrincipal ACCOUNT requires sessionDeviceId",
      );
      if (!authority || typeof authority !== "object") {
        throw new Error("SessionPrincipal ACCOUNT requires the verified authority object");
      }
      if (authority.mode !== "direct" && authority.mode !== "delegated") {
        throw new Error("SessionPrincipal ACCOUNT authority.mode must be direct or delegated");
      }
      if (claimantPublicKeyB64 !== undefined) {
        throw new Error("SessionPrincipal ACCOUNT must not carry a claimant key");
      }
      if (Array.isArray(authority.grantedCapabilities)) Object.freeze(authority.grantedCapabilities);
      if (Array.isArray(authority.certChain)) Object.freeze(authority.certChain);
      this.authority = Object.freeze(authority);
      this.claimantPublicKeyB64 = null;
    } else if (kind === KIND_CLAIMANT) {
      this.kind = KIND_CLAIMANT;
      this.claimantPublicKeyB64 = requireNonEmptyString(
        claimantPublicKeyB64,
        "SessionPrincipal CLAIMANT requires claimantPublicKeyB64",
      );
      if (accountPublicKeyB64 !== undefined || sessionDeviceId !== undefined || authority !== undefined) {
        throw new Error("SessionPrincipal CLAIMANT must not carry account identity fields");
      }
      this.accountPublicKeyB64 = null;
      this.sessionDeviceId = null;
      this.authority = null;
    } else {
      throw new Error("SessionPrincipal requires kind ACCOUNT or CLAIMANT");
    }
    Object.freeze(this);
  }

  static accountDirect({ accountPublicKeyB64, sessionDeviceId, authority }) {
    if (!authority || authority.mode !== "direct") {
      throw new Error("SessionPrincipal.accountDirect requires a direct-mode authority");
    }
    return new SessionPrincipal({ kind: KIND_ACCOUNT, accountPublicKeyB64, sessionDeviceId, authority });
  }

  static accountDelegated({ accountPublicKeyB64, sessionDeviceId, authority }) {
    if (!authority || authority.mode !== "delegated") {
      throw new Error("SessionPrincipal.accountDelegated requires a delegated-mode authority");
    }
    return new SessionPrincipal({ kind: KIND_ACCOUNT, accountPublicKeyB64, sessionDeviceId, authority });
  }

  static claimant({ claimantPublicKeyB64 }) {
    return new SessionPrincipal({ kind: KIND_CLAIMANT, claimantPublicKeyB64 });
  }

  isAccount() {
    return this.kind === KIND_ACCOUNT;
  }

  isClaimant() {
    return this.kind === KIND_CLAIMANT;
  }

  /**
   * The §3 claimant-cardinality invariant, intrinsic to the principal:
   * may this session bind (via inbox.claim) an inbox claimed by the given key?
   *
   * ACCOUNT: true — the legacy v4 multi-key claim path (CAPABILITY_MODEL §8)
   * is preserved for this slice; SESSION_AUTH_V5 slice 2 revisits it only for
   * v5 sessions.
   * CLAIMANT: only the principal's own trust root. One session, one claimant
   * root — an unrelated binding is rejected, which demotes the F1b co-residency
   * disclosure to plain traffic analysis.
   *
   * @param {string} claimantPublicKeyB64
   * @returns {boolean}
   */
  admitsClaimantBinding(claimantPublicKeyB64) {
    if (typeof claimantPublicKeyB64 !== "string" || claimantPublicKeyB64.trim().length === 0) {
      return false;
    }
    if (this.kind === KIND_ACCOUNT) {
      return true;
    }
    return claimantPublicKeyB64.trim() === this.claimantPublicKeyB64;
  }
}
