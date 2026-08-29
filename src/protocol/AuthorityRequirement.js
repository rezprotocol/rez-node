/**
 * AuthorityRequirement — the principal-class authority an operation declares at
 * registration (SESSION_AUTH_V5 slice 1; plans/SESSION_AUTH_V5_PHASE0.md §5).
 *
 * Enforced by HandlerRegistry.dispatch BEFORE the handler is invoked, so an
 * operation cannot become reachable without an authorization classification.
 * Resource-level scope (which inbox, which account, which capability chain)
 * stays operation-specific and is enforced in the handler / ProtocolContext.
 *
 *   ACCOUNT       — only an ACCOUNT principal may invoke.
 *   ANY_PRINCIPAL — any authenticated principal may invoke. The name is
 *                   deliberately loud: every ANY_PRINCIPAL operation must have
 *                   content-carried or claimant-scoped authorization behind it
 *                   (signatures, ownership proofs, cap chains, inbox bindings),
 *                   and a reviewer seeing it in a diff should ask why either
 *                   identity suffices.
 *
 * No CLAIMANT-only value exists yet: no operation on the current wire surface
 * requires a claimant principal. The v5 handshake slice adds one if its matrix
 * needs it — values are not created in anticipation.
 */

import { SessionPrincipal } from "./SessionPrincipal.js";

const ACCOUNT = "ACCOUNT";
const ANY_PRINCIPAL = "ANY_PRINCIPAL";

export const AuthorityRequirement = Object.freeze({
  ACCOUNT,
  ANY_PRINCIPAL,

  /** @param {string} value */
  isValid(value) {
    return value === ACCOUNT || value === ANY_PRINCIPAL;
  },

  /**
   * Does the declared requirement admit this principal's class?
   * @param {string} requirement — a valid AuthorityRequirement value
   * @param {SessionPrincipal} principal
   * @returns {boolean}
   */
  admits(requirement, principal) {
    if (!(principal instanceof SessionPrincipal)) {
      return false;
    }
    if (requirement === ANY_PRINCIPAL) {
      return true;
    }
    if (requirement === ACCOUNT) {
      return principal.isAccount();
    }
    return false;
  },
});
