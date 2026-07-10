import { verifyAccountAuthority } from "@rezprotocol/core";

/**
 * Per-op revalidation of a DELEGATED session's cert chain against the home's
 * CURRENT authority state (audit 2026-07-09 F2, centralized 2026-07-10 so no
 * handler can forget it).
 *
 * A session's `sessionAuthority` (chain + grantedCapabilities) is fixed at
 * connect time. A delegated device revoked WHILE its socket stays open would
 * otherwise keep exercising its capabilities until it reconnects. Every
 * privileged op on a delegated session must therefore re-check the chain
 * against the authoritative, un-cached revocation set — via the canonical
 * verifier (SSOT; never reimplement revocation semantics here).
 *
 * Direct (primary) sessions sign with the account root, which holds every
 * capability and cannot be revoked — callers skip this for them.
 *
 * @returns {Promise<{ok: true} | {ok: false, code: string, message: string, retryable: boolean}>}
 *   Failure objects map 1:1 onto `ctx.sendError` fields.
 */
export async function revalidateDelegatedAuthority({
  serializer,
  crypto,
  accountIdentityPublicKeyB64,
  requiredCapability = null,
  opSignerPublicKeyB64,
  certChain,
  nowMs,
}) {
  let authorityState;
  try {
    authorityState = await serializer.getAuthorityState(accountIdentityPublicKeyB64);
  } catch (err) {
    return {
      ok: false,
      code: "INTERNAL",
      message: "authority revalidation failed: " + (err && err.message ? err.message : "unknown"),
      retryable: true,
    };
  }
  const revocationState = {
    revokedCertIds: Array.isArray(authorityState.revokedCertIds) ? authorityState.revokedCertIds : [],
    minValidIssuedAtMs: Number.isFinite(Number(authorityState.minValidIssuedAtMs)) ? Number(authorityState.minValidIssuedAtMs) : 0,
  };
  const recheck = await verifyAccountAuthority({
    expectedAccountIdentityPublicKeyB64: accountIdentityPublicKeyB64,
    requiredCapability,
    opSignerPublicKeyB64,
    certChain: Array.isArray(certChain) ? certChain : null,
    crypto,
    nowMs,
    revocationState,
  });
  if (!recheck || recheck.ok !== true) {
    return {
      ok: false,
      code: "FORBIDDEN",
      message: "delegated authority is no longer valid (revoked or insufficient)",
      retryable: false,
    };
  }
  return { ok: true };
}
