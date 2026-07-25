import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

const T = REZ_CONTRACT_TYPES;

// ── Repository ownership (audit leaf-3c F1) ────────────────────────────────────────────────────
// These are NODE↔CLIENT RPC records (request/response envelopes for the outbox lease lifecycle), so
// they live in rez-node alongside every other transport record. That placement follows the record
// ownership rule in AGENTS.md ("Records: which repo owns which kind") — see it for the full split
// and the reasoning; do not re-derive the boundary from this comment.
//
// req 1 (audit leaf-3b F1): the lease token is the ONLY client-supplied field, and its size bound
// lives in the CONTRACT layer (not the handler) so every entry point validates the same way. The
// server mints a 48-hex token; 128 bytes leaves margin and matches the DB lease_token size CHECK
// (migration 0018). TextEncoder keeps the byte count portable.
export const MAX_LEASE_TOKEN_BYTES = 128;

// leaf-3c: the COMPLETE request carries a full signed publication (a DurableRecordV2 wrapping an
// AccountAuthorityStateV1, plus — in delegated mode — a cert chain), so it is materially larger than
// a token. Size-bound it in the CONTRACT layer (SSOT, same posture as MAX_LEASE_TOKEN_BYTES) so every
// entry point rejects an oversized submission BEFORE it reaches signature verification, capping
// pre-verification parse/memory work. The DHT store applies its own authoritative record-size gate on
// put; this only bounds what the node will parse/verify. 128 KiB comfortably fits a large authority
// state + chain while staying a firm abuse ceiling.
export const MAX_PUBLICATION_RECORD_BYTES = 131072;

function utf8ByteLength(s) {
  return new TextEncoder().encode(s).length;
}

function jsonByteLength(v) {
  return new TextEncoder().encode(JSON.stringify(v)).length;
}

/**
 * Shared base for the three token-bearing lease requests (prepare / release / fail). The
 * account and the lease OWNER are NEVER wire fields — the handler derives them from the
 * authenticated session — so a lease request carries only its token.
 *
 * F5 (audit leaf-3c): the raw value is preserved VERBATIM (no String() coercion). A non-string
 * token fails validation LOUDLY (BAD_REQUEST) rather than being silently stringified — client
 * contract drift must surface, never be masked.
 */
export class OutboxLeaseTokenRequest extends RRecord {
  constructor({ leaseToken } = {}) {
    super();
    this.leaseToken = leaseToken;
    if (this.constructor === OutboxLeaseTokenRequest) this._seal();
  }

  validate() {
    this.assert(this.leaseToken !== undefined && this.leaseToken !== null, "leaseToken is required");
    this.assert(typeof this.leaseToken === "string", "leaseToken must be a string");
    this.assert(this.leaseToken.trim().length > 0, "leaseToken is required");
    this.assert(utf8ByteLength(this.leaseToken) <= MAX_LEASE_TOKEN_BYTES, "leaseToken exceeds the " + MAX_LEASE_TOKEN_BYTES + "-byte limit");
  }
}

/** claim — no client input (account + owner come from the session). */
export class OutboxLeaseClaimRequest extends RRecord {
  static type = T.ACCOUNT_OUTBOX_LEASE_CLAIM;

  constructor(fields = {}) {
    super();
    void fields;
    if (this.constructor === OutboxLeaseClaimRequest) this._seal();
  }

  validate() {
    // no required fields — the body is intentionally empty.
  }
}

/**
 * claim response — the server-minted lease, or { leased: false } when nothing is claimable.
 *
 * F5 (audit leaf-3c): booleans/integers are preserved VERBATIM and type-checked strictly. A
 * missing/malformed `leased` no longer coerces to false — backend contract drift fails loudly
 * (the server-built record throws at seal, surfacing as INTERNAL) rather than silently returning
 * a plausible-but-wrong "nothing to lease".
 */
export class OutboxLeaseClaimResponse extends RRecord {
  static type = T.ACCOUNT_OUTBOX_LEASE_CLAIM_RES;

  constructor({ leased, token, anchorEpoch, headEpoch, leaseExpiresAtMs, attempts } = {}) {
    super();
    this.leased = leased;
    if (leased === true) {
      this.token = token;
      this.anchorEpoch = anchorEpoch;
      this.headEpoch = headEpoch;
      this.leaseExpiresAtMs = leaseExpiresAtMs;
      this.attempts = attempts;
    }
    if (this.constructor === OutboxLeaseClaimResponse) this._seal();
  }

  validate() {
    this.assert(typeof this.leased === "boolean", "leased must be a boolean");
    if (this.leased) {
      this.assert(typeof this.token === "string" && this.token.length > 0, "a leased response requires a token");
      this.assert(Number.isInteger(this.anchorEpoch) && this.anchorEpoch >= 1, "anchorEpoch must be a positive integer");
      this.assert(Number.isInteger(this.headEpoch) && this.headEpoch >= this.anchorEpoch, "headEpoch must be an integer >= anchorEpoch");
      this.assert(Number.isInteger(this.leaseExpiresAtMs), "leaseExpiresAtMs must be an integer");
      this.assert(Number.isInteger(this.attempts) && this.attempts >= 0, "attempts must be a non-negative integer");
    }
  }
}

export class OutboxLeasePrepareRequest extends OutboxLeaseTokenRequest {
  static type = T.ACCOUNT_OUTBOX_LEASE_PREPARE;

  constructor(fields = {}) {
    super(fields);
    if (this.constructor === OutboxLeasePrepareRequest) this._seal();
  }
}

/** prepare response — the frozen { anchorEpoch, headEpoch } to publish, or { prepared: false }. */
export class OutboxLeasePrepareResponse extends RRecord {
  static type = T.ACCOUNT_OUTBOX_LEASE_PREPARE_RES;

  constructor({ prepared, anchorEpoch, headEpoch } = {}) {
    super();
    this.prepared = prepared;
    if (prepared === true) {
      this.anchorEpoch = anchorEpoch;
      this.headEpoch = headEpoch;
    }
    if (this.constructor === OutboxLeasePrepareResponse) this._seal();
  }

  validate() {
    this.assert(typeof this.prepared === "boolean", "prepared must be a boolean");
    if (this.prepared) {
      this.assert(Number.isInteger(this.anchorEpoch) && this.anchorEpoch >= 1, "anchorEpoch must be a positive integer");
      this.assert(Number.isInteger(this.headEpoch) && this.headEpoch >= this.anchorEpoch, "headEpoch must be an integer >= anchorEpoch");
    }
  }
}

export class OutboxLeaseReleaseRequest extends OutboxLeaseTokenRequest {
  static type = T.ACCOUNT_OUTBOX_LEASE_RELEASE;

  constructor(fields = {}) {
    super(fields);
    if (this.constructor === OutboxLeaseReleaseRequest) this._seal();
  }
}

/** release response — whether this exact live lease was released. */
export class OutboxLeaseReleaseResponse extends RRecord {
  static type = T.ACCOUNT_OUTBOX_LEASE_RELEASE_RES;

  constructor({ released } = {}) {
    super();
    this.released = released;
    if (this.constructor === OutboxLeaseReleaseResponse) this._seal();
  }

  validate() {
    this.assert(typeof this.released === "boolean", "released must be a boolean");
  }
}

export class OutboxLeaseFailRequest extends OutboxLeaseTokenRequest {
  static type = T.ACCOUNT_OUTBOX_LEASE_FAIL;

  constructor(fields = {}) {
    super(fields);
    if (this.constructor === OutboxLeaseFailRequest) this._seal();
  }
}

/** fail response — the recorded backoff accounting, or { recorded: false } when no live lease. */
export class OutboxLeaseFailResponse extends RRecord {
  static type = T.ACCOUNT_OUTBOX_LEASE_FAIL_RES;

  constructor({ recorded, attemptedEpoch, anchorEpoch, attempts, backoffMs, blocked } = {}) {
    super();
    this.recorded = recorded;
    if (recorded === true) {
      this.attemptedEpoch = attemptedEpoch;
      this.anchorEpoch = anchorEpoch;
      this.attempts = attempts;
      this.backoffMs = backoffMs;
      this.blocked = blocked;
    }
    if (this.constructor === OutboxLeaseFailResponse) this._seal();
  }

  validate() {
    this.assert(typeof this.recorded === "boolean", "recorded must be a boolean");
    if (this.recorded) {
      this.assert(Number.isInteger(this.attemptedEpoch) && this.attemptedEpoch >= 1, "attemptedEpoch must be a positive integer");
      this.assert(Number.isInteger(this.anchorEpoch) && this.anchorEpoch >= 1, "anchorEpoch must be a positive integer");
      this.assert(Number.isInteger(this.attempts) && this.attempts >= 0, "attempts must be a non-negative integer");
      this.assert(Number.isInteger(this.backoffMs) && this.backoffMs >= 0, "backoffMs must be a non-negative integer");
      this.assert(typeof this.blocked === "boolean", "blocked must be a boolean");
    }
  }
}

/**
 * complete (leaf 3c) — the VERIFIED completion (ack). Unlike the four crypto-free ops, this request
 * carries the signed publication (`record`, a DurableRecordV2 wrapping AccountAuthorityStateV1) IN
 * ADDITION to the lease token. The account and lease OWNER are still derived from the session, never
 * the body.
 *
 * The contract validates SHAPE + SIZE only. The cryptographic verification — envelope + inner
 * signatures, cert chain vs the account's revocation state, and the epoch matching the frozen
 * prepared_epoch — is the HANDLER's job (PropagationOutboxHandler.handleComplete), not the record's;
 * a contract must never imply a security check it does not perform.
 *
 * F5 (audit leaf-3c): the record is preserved VERBATIM — no coercion. A non-object / null / array
 * record fails LOUDLY (BAD_REQUEST) rather than being massaged into a plausible-but-wrong shape.
 */
export class OutboxLeaseCompleteRequest extends OutboxLeaseTokenRequest {
  static type = T.ACCOUNT_OUTBOX_LEASE_COMPLETE;

  constructor({ leaseToken, record } = {}) {
    super({ leaseToken });
    this.record = record;
    if (this.constructor === OutboxLeaseCompleteRequest) this._seal();
  }

  validate() {
    super.validate(); // leaseToken: required, string, size-bounded.
    this.assert(this.record !== undefined && this.record !== null, "record is required");
    this.assert(typeof this.record === "object" && !Array.isArray(this.record), "record must be an object");
    this.assert(jsonByteLength(this.record) <= MAX_PUBLICATION_RECORD_BYTES, "record exceeds the " + MAX_PUBLICATION_RECORD_BYTES + "-byte limit");
  }
}

/**
 * complete response — whether the verified publication advanced the account's 'done' watermark.
 * `completed: false` is the benign lease-lost race (the lease lapsed before the ack landed; another
 * device will re-drain), carrying no epoch. `doneThroughEpoch` is the epoch M through which every
 * obligation was marked done.
 *
 * F5: booleans/integers strict + verbatim — a missing/malformed `completed` never coerces to false.
 */
export class OutboxLeaseCompleteResponse extends RRecord {
  static type = T.ACCOUNT_OUTBOX_LEASE_COMPLETE_RES;

  constructor({ completed, doneThroughEpoch } = {}) {
    super();
    this.completed = completed;
    if (completed === true) {
      this.doneThroughEpoch = doneThroughEpoch;
    }
    if (this.constructor === OutboxLeaseCompleteResponse) this._seal();
  }

  validate() {
    this.assert(typeof this.completed === "boolean", "completed must be a boolean");
    if (this.completed) {
      this.assert(Number.isInteger(this.doneThroughEpoch) && this.doneThroughEpoch >= 1, "doneThroughEpoch must be a positive integer");
    }
  }
}
