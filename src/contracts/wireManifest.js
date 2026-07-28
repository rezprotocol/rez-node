import { REZ_CONTRACT_TYPES } from "@rezprotocol/core";

const T = REZ_CONTRACT_TYPES;

/**
 * WIRE MANIFEST — the single declaration of what every wire type IS (audit #6, 2026-07-28).
 *
 * THE SPLIT-BRAIN. Two registries independently decided things about the same wire types and
 * neither knew about the other:
 *   - ContractRegistry  maps type → RRecord constructor (the SHAPE).
 *   - HandlerRegistry   maps type → { handler, method } (the BEHAVIOUR).
 * A type could be in either, both, or neither, and nothing noticed. Measured before this manifest
 * existed: 58 wire types, of which 11 request types were dispatched with no contract registered and
 * their 11 responses had none either. That is not "unvalidated" — those handlers validate by
 * constructing a record inline — but it does mean TWO parallel validation mechanisms with no single
 * place saying which one applies to which type. Deciding that per-handler is exactly how a new op
 * ends up with neither.
 *
 * This manifest is that single place. Every value in REZ_CONTRACT_TYPES must appear here exactly
 * once, and the guardrail (test/architecture.wire-manifest.test.js) fails otherwise — so adding a
 * wire type forces a decision about its direction, who validates it, and whether it is dispatched,
 * instead of letting it default into a gap.
 *
 * `direction`
 *   request   — client → node, dispatched through HandlerRegistry
 *   response  — node → client, sent in reply to a request
 *   event     — node → client, unsolicited
 *   bootstrap — the pre-session handshake, handled by sessionBootstrap OUTSIDE HandlerRegistry
 *
 * `validatedBy`
 *   contract  — a record is registered in ContractRegistry for this type
 *   handler   — the handler constructs/validates the body itself (a record class, or explicit
 *               field checks). Legitimate, but it is DEBT: it is invisible to the contract
 *               registry, so WS_CONTRACT_EXAMPLES cannot cover it and no generic layer can reject a
 *               malformed body before it reaches the handler.
 */
const REQUEST = "request";
const RESPONSE = "response";
const EVENT = "event";
const BOOTSTRAP = "bootstrap";
const BY_CONTRACT = "contract";
const BY_HANDLER = "handler";

export const WIRE_DIRECTIONS = Object.freeze({ REQUEST, RESPONSE, EVENT, BOOTSTRAP });
export const WIRE_VALIDATED_BY = Object.freeze({ CONTRACT: BY_CONTRACT, HANDLER: BY_HANDLER });

/**
 * @type {ReadonlyArray<{type: string, direction: string, validatedBy: string, nodeOnly?: boolean}>}
 * `nodeOnly` marks a request registered only when the node role is enabled (GatewaySession gates
 * these behind `_nodeEnabled`), so the guardrail does not demand it on a relay-only build.
 */
export const WIRE_MANIFEST = Object.freeze([
  // ── Handshake (outside HandlerRegistry by design) ───────────────────────────────────────────
  { type: T.SESSION_HELLO, direction: BOOTSTRAP, validatedBy: BY_CONTRACT },
  { type: T.SESSION_CHALLENGE, direction: BOOTSTRAP, validatedBy: BY_HANDLER },
  { type: T.SESSION_AUTHENTICATE, direction: BOOTSTRAP, validatedBy: BY_HANDLER },
  { type: T.SESSION_READY, direction: EVENT, validatedBy: BY_CONTRACT },
  { type: T.ERROR, direction: EVENT, validatedBy: BY_CONTRACT },

  // ── Mailbox ─────────────────────────────────────────────────────────────────────────────────
  { type: T.MAILBOX_DEPOSIT, direction: REQUEST, validatedBy: BY_CONTRACT },
  { type: T.MAILBOX_DEPOSIT_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },
  { type: T.MAILBOX_LIST, direction: REQUEST, validatedBy: BY_CONTRACT },
  { type: T.MAILBOX_LIST_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },
  { type: T.MAILBOX_FETCH, direction: REQUEST, validatedBy: BY_CONTRACT },
  { type: T.MAILBOX_FETCH_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },
  { type: T.MAILBOX_ACK, direction: REQUEST, validatedBy: BY_CONTRACT },
  { type: T.MAILBOX_ACK_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },
  { type: T.MAILBOX_CURSOR_ACK, direction: REQUEST, validatedBy: BY_CONTRACT },
  { type: T.MAILBOX_CURSOR_ACK_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },
  { type: T.EVT_MAILBOX_DEPOSITED, direction: EVENT, validatedBy: BY_CONTRACT },
  { type: T.EVT_OUTBOUND_STATUS, direction: EVENT, validatedBy: BY_CONTRACT },

  // ── Inbox ───────────────────────────────────────────────────────────────────────────────────
  { type: T.INBOX_CLAIM, direction: REQUEST, validatedBy: BY_CONTRACT },
  { type: T.INBOX_CLAIM_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },
  { type: T.INBOX_SET_DEPOSIT_POLICY, direction: REQUEST, validatedBy: BY_HANDLER },
  { type: T.INBOX_SET_DEPOSIT_POLICY_RES, direction: RESPONSE, validatedBy: BY_HANDLER },

  // ── Device binding ──────────────────────────────────────────────────────────────────────────
  { type: T.DEVICE_BIND, direction: REQUEST, validatedBy: BY_CONTRACT },
  { type: T.DEVICE_BIND_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },

  // ── Account authority (node-only) ───────────────────────────────────────────────────────────
  // These validate by constructing AccountDeviceMutationV1/V2 in the handler — which is why the
  // version split (audit #5) lives there and not in a contract record.
  { type: T.ACCOUNT_DEVICE_MUTATION_SUBMIT, direction: REQUEST, validatedBy: BY_HANDLER, nodeOnly: true },
  { type: T.ACCOUNT_DEVICE_MUTATION_SUBMIT_RES, direction: RESPONSE, validatedBy: BY_HANDLER },
  { type: T.ACCOUNT_AUTHORITY_STATE_GET, direction: REQUEST, validatedBy: BY_HANDLER, nodeOnly: true },
  { type: T.ACCOUNT_AUTHORITY_STATE_GET_RES, direction: RESPONSE, validatedBy: BY_HANDLER },
  { type: T.ACCOUNT_DEVICE_BUNDLE_PUBLISH, direction: REQUEST, validatedBy: BY_HANDLER, nodeOnly: true },
  { type: T.ACCOUNT_DEVICE_BUNDLE_PUBLISH_RES, direction: RESPONSE, validatedBy: BY_HANDLER },
  { type: T.ACCOUNT_DEVICE_SET_GET, direction: REQUEST, validatedBy: BY_HANDLER, nodeOnly: true },
  { type: T.ACCOUNT_DEVICE_SET_GET_RES, direction: RESPONSE, validatedBy: BY_HANDLER },

  // ── Authority-state propagation outbox (node-only) ──────────────────────────────────────────
  { type: T.ACCOUNT_OUTBOX_LEASE_CLAIM, direction: REQUEST, validatedBy: BY_CONTRACT, nodeOnly: true },
  { type: T.ACCOUNT_OUTBOX_LEASE_CLAIM_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },
  { type: T.ACCOUNT_OUTBOX_LEASE_PREPARE, direction: REQUEST, validatedBy: BY_CONTRACT, nodeOnly: true },
  { type: T.ACCOUNT_OUTBOX_LEASE_PREPARE_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },
  { type: T.ACCOUNT_OUTBOX_LEASE_RELEASE, direction: REQUEST, validatedBy: BY_CONTRACT, nodeOnly: true },
  { type: T.ACCOUNT_OUTBOX_LEASE_RELEASE_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },
  { type: T.ACCOUNT_OUTBOX_LEASE_FAIL, direction: REQUEST, validatedBy: BY_CONTRACT, nodeOnly: true },
  { type: T.ACCOUNT_OUTBOX_LEASE_FAIL_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },
  // complete's request/response records exist (OutboxLeaseComplete*) but are constructed in the
  // handler rather than registered — the one member of this family that is not contract-backed.
  { type: T.ACCOUNT_OUTBOX_LEASE_COMPLETE, direction: REQUEST, validatedBy: BY_HANDLER, nodeOnly: true },
  { type: T.ACCOUNT_OUTBOX_LEASE_COMPLETE_RES, direction: RESPONSE, validatedBy: BY_HANDLER },

  // ── Channels ────────────────────────────────────────────────────────────────────────────────
  { type: T.CHANNEL_OPEN, direction: REQUEST, validatedBy: BY_CONTRACT },
  { type: T.CHANNEL_OPEN_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },
  { type: T.CHANNEL_CLOSE, direction: REQUEST, validatedBy: BY_CONTRACT },
  { type: T.CHANNEL_CLOSE_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },
  { type: T.CHANNEL_SIGNAL, direction: EVENT, validatedBy: BY_CONTRACT },

  // ── Mesh status (node-only) ─────────────────────────────────────────────────────────────────
  { type: T.NODE_STATUS, direction: REQUEST, validatedBy: BY_CONTRACT, nodeOnly: true },
  { type: T.NODE_STATUS_RES, direction: RESPONSE, validatedBy: BY_CONTRACT },

  // ── Durable records ─────────────────────────────────────────────────────────────────────────
  { type: T.RECORD_PUT, direction: REQUEST, validatedBy: BY_HANDLER },
  { type: T.RECORD_PUT_RES, direction: RESPONSE, validatedBy: BY_HANDLER },
  { type: T.RECORD_GET, direction: REQUEST, validatedBy: BY_HANDLER },
  { type: T.RECORD_GET_RES, direction: RESPONSE, validatedBy: BY_HANDLER },

  // ── Handles ─────────────────────────────────────────────────────────────────────────────────
  { type: T.HANDLE_REGISTER, direction: REQUEST, validatedBy: BY_HANDLER },
  { type: T.HANDLE_REGISTER_RES, direction: RESPONSE, validatedBy: BY_HANDLER },
  { type: T.HANDLE_RESOLVE, direction: REQUEST, validatedBy: BY_HANDLER },
  { type: T.HANDLE_RESOLVE_RES, direction: RESPONSE, validatedBy: BY_HANDLER },
  { type: T.HANDLE_RELEASE, direction: REQUEST, validatedBy: BY_HANDLER },
  { type: T.HANDLE_RELEASE_RES, direction: RESPONSE, validatedBy: BY_HANDLER },
]);

/** Look up a wire type's manifest entry, or null. */
export function wireManifestEntry(type) {
  for (const entry of WIRE_MANIFEST) {
    if (entry.type === type) return entry;
  }
  return null;
}

/** Every declared type of a given direction. */
export function wireTypesByDirection(direction) {
  return WIRE_MANIFEST.filter((e) => e.direction === direction).map((e) => e.type).sort();
}

/**
 * Request types this build must dispatch. `nodeOnly` entries are excluded unless the node role is
 * enabled — GatewaySession registers those behind the same flag.
 */
export function dispatchableRequestTypes({ nodeEnabled }) {
  return WIRE_MANIFEST
    .filter((e) => e.direction === REQUEST && (nodeEnabled === true || e.nodeOnly !== true))
    .map((e) => e.type)
    .sort();
}
