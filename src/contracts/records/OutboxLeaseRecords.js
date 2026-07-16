import { RRecord, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

const T = REZ_CONTRACT_TYPES;

// req 1 (audit leaf-3b F1): the lease token is the ONLY client-supplied field, and its size
// bound now lives in the CONTRACT layer (not the handler) so every entry point validates the
// same way. The server mints a 48-hex token; 128 bytes leaves margin and matches the DB
// lease_token size CHECK (migration 0018). TextEncoder keeps the byte count portable.
export const MAX_LEASE_TOKEN_BYTES = 128;

function utf8Bytes(s) {
  return new TextEncoder().encode(typeof s === "string" ? s : "").length;
}

/**
 * Shared base for the three token-bearing lease requests (prepare / release / fail). The
 * account and the lease OWNER are NEVER wire fields — the handler derives them from the
 * authenticated session — so a lease request carries only its token.
 */
export class OutboxLeaseTokenRequest extends RRecord {
  constructor({ leaseToken } = {}) {
    super();
    this.leaseToken = typeof leaseToken === "string" ? leaseToken : (leaseToken == null ? "" : String(leaseToken));
    if (this.constructor === OutboxLeaseTokenRequest) this._seal();
  }

  validate() {
    this.assert(typeof this.leaseToken === "string" && this.leaseToken.trim().length > 0, "leaseToken is required");
    this.assert(utf8Bytes(this.leaseToken) <= MAX_LEASE_TOKEN_BYTES, "leaseToken exceeds the " + MAX_LEASE_TOKEN_BYTES + "-byte limit");
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

/** claim response — the server-minted lease, or { leased: false } when nothing is claimable. */
export class OutboxLeaseClaimResponse extends RRecord {
  static type = T.ACCOUNT_OUTBOX_LEASE_CLAIM_RES;

  constructor({ leased, token, anchorEpoch, headEpoch, leaseExpiresAtMs, attempts } = {}) {
    super();
    this.leased = leased === true;
    if (this.leased) {
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
    this.prepared = prepared === true;
    if (this.prepared) {
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
    this.released = released === true;
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
    this.recorded = recorded === true;
    if (this.recorded) {
      this.attemptedEpoch = attemptedEpoch;
      this.anchorEpoch = anchorEpoch;
      this.attempts = attempts;
      this.backoffMs = backoffMs;
      this.blocked = blocked === true;
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
