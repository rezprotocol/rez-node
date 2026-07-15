/**
 * Fresh reader over the home's account authority-state (S2.5 S11, F4; audit R4 L5).
 *
 * A delegated session is only as trustworthy as the account's CURRENT revocation state. The
 * authority home owns that state (PgAccountMutationSerializer); this class is the read façade the
 * session-auth + per-dispatch guard paths consult.
 *
 * NO CACHING (audit R4 L5 review): the L5 guard reads the account's monotonic authority epoch on
 * every delegated frame (currentEpoch) and, only when it advances, does ONE COHERENT snapshot read
 * (resolveDelegatedSnapshot) covering epoch + revocation state + the session device's terminal
 * status together. A TTL cache would only reintroduce a bounded-staleness window and an
 * overlapping-read regression race (review findings 1+2) for zero benefit, since nothing consults a
 * warm entry — so the warm resolve()/invalidate() path was removed. The class name is retained to
 * avoid churning the runtime wiring key `accountAuthorityRevocationCache`.
 *
 * BYTE-COMPATIBILITY INVARIANT: the projected `state` is `null` — never `{}` — when an account has
 * no revocations (no revoked certs AND no issued-at cutoff). The downstream verifiers
 * (`verifyAccountAuthority`, `verifyDurableRecordV2`) treat a null `revocationState` as the untouched
 * primary path, so a never-revoked account stays byte-for-byte identical to the pre-S11 behavior.
 */

export class AccountAuthorityRevocationCache {
  #serializer;

  constructor({ serializer } = {}) {
    if (!serializer || typeof serializer.getCurrentEpoch !== "function"
        || typeof serializer.getDelegatedAuthoritySnapshot !== "function") {
      throw new Error("AccountAuthorityRevocationCache requires a serializer with getCurrentEpoch + getDelegatedAuthoritySnapshot");
    }
    this.#serializer = serializer;
  }

  /**
   * The account's current authority epoch — a cheap, ALWAYS-FRESH single-row read (passthrough to
   * the serializer). The per-dispatch L5 guard calls this on every delegated frame as its fast
   * path: an epoch unchanged since admission proves authority is unchanged (the epoch is monotonic
   * and bumps on every add/revoke).
   * @returns {Promise<number>} epoch (0 for a blank account or one with no authority row)
   */
  async currentEpoch(accountIdentityPublicKeyB64) {
    const account = this.#normAccount(accountIdentityPublicKeyB64);
    if (!account) return 0;
    return this.#serializer.getCurrentEpoch(account);
  }

  /**
   * ONE COHERENT snapshot for a delegated (account, device): the projected revocation state, its
   * epoch, and the device's TERMINAL status — all read in a single REPEATABLE READ transaction on
   * the home (review finding 1). Because terminal + epoch + revoked-cert set come from the same
   * snapshot, a `cert_id = NULL` device revoked mid-read cannot leave the terminal read pre-revoke
   * while the epoch reads post-revoke (which previously poisoned the guard's epoch watermark). The
   * caller uses `terminal` to reject, `state` to re-verify the chain, and `epoch` to arm/advance its
   * fast-path watermark — always mutually consistent.
   *
   * The terminal predicate is read through the serializer's OWN canonical registry (audit R4 L5
   * review-3 finding P2) — this façade no longer knows about the in-transaction storage API nor
   * threads a per-call registry through it.
   * @param {string} accountIdentityPublicKeyB64
   * @param {string} deviceId
   * @returns {Promise<{state: {revokedCertIds: string[], minValidIssuedAtMs: number}|null, epoch: number, terminal: boolean}>}
   */
  async resolveDelegatedSnapshot(accountIdentityPublicKeyB64, deviceId) {
    const account = this.#normAccount(accountIdentityPublicKeyB64);
    if (!account) return { state: null, epoch: 0, terminal: false };
    const snap = await this.#serializer.getDelegatedAuthoritySnapshot({
      accountIdentityPublicKeyB64: account,
      deviceId,
    });
    // Audit R4 L5 review-4 finding P1 (+ review-5): the authority home MUST return a COMPLETE, STRICTLY
    // typed row — a boolean terminal, a safe nonnegative INTEGER epoch, a string[] revoked-cert set, and
    // a safe nonnegative INTEGER cutoff. Validate the RAW fields with NO Number(...) coercion: coercing
    // would silently accept null, "", numeric strings, fractions, or non-finite values (Number(null)===0,
    // Number("")===0) and normalize a malformed cutoff to 0 — which then projects `state` to null and
    // silently DROPS the issued-at revocation dimension, letting a downstream consumer fail OPEN. A
    // malformed field is a backend contract violation; fail LOUD — the guard/admission paths treat a
    // throw as REVOCATION_BACKEND_UNAVAILABLE (retryable), never a definitive "authorized". The projected
    // values below are the ALREADY-VALIDATED raw fields (no second, looser contract).
    if (!snap || typeof snap !== "object" || typeof snap.terminal !== "boolean"
        || !Number.isSafeInteger(snap.epoch) || snap.epoch < 0
        || !Array.isArray(snap.revokedCertIds)
        || !snap.revokedCertIds.every((c) => typeof c === "string")
        || !Number.isSafeInteger(snap.minValidIssuedAtMs) || snap.minValidIssuedAtMs < 0) {
      const err = new Error("authority home returned an incomplete delegated snapshot");
      err.code = "REVOCATION_BACKEND_UNAVAILABLE";
      throw err;
    }
    // null-when-empty (byte-compat): no revoked certs AND no issued-at cutoff ⇒ primary path. Both fields
    // are already validated safe nonnegative integers / a string[] — project them directly, no coercion.
    const state = (snap.revokedCertIds.length === 0 && snap.minValidIssuedAtMs === 0)
      ? null
      : { revokedCertIds: [...snap.revokedCertIds], minValidIssuedAtMs: snap.minValidIssuedAtMs };
    return { state, epoch: snap.epoch, terminal: snap.terminal };
  }

  #normAccount(accountIdentityPublicKeyB64) {
    return typeof accountIdentityPublicKeyB64 === "string" && accountIdentityPublicKeyB64.trim().length > 0
      ? accountIdentityPublicKeyB64.trim()
      : null;
  }
}
