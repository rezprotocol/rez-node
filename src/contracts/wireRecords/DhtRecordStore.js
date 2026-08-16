/**
 * Node-to-node control records for acknowledged durable-record replication
 * (ATLAS_PREREQUISITES P4.1). Node-owned wire records — deliberately NOT in
 * rez-core: two nodes exchanging them does not make them shared runtime
 * vocabulary.
 *
 * The request/ack pair carries exactly the fields the spec allows — no
 * endpoint, account, inbox, path, pricing, or capacity data. Timeout and
 * disconnect are LOCAL outcomes at the sender; they are never remote
 * acknowledgement statuses.
 */
import { RRecord } from "@rezprotocol/core";

export const CTL_DHT_REC_STORE = "dht.rec_store";
export const CTL_DHT_REC_STORE_ACK = "dht.rec_store.ack";
export const DHT_RECORD_STORE_PROTOCOL_VERSION = 1;

export const DHT_REC_STORE_ACK_STATUS = Object.freeze({
  STORED: "stored",
  REFRESHED: "refreshed",
  REJECTED: "rejected",
});

/**
 * Bounded, safe rejection vocabulary. Local store/verify reasons are mapped
 * into this set before hitting the wire so attacker-controlled error text is
 * never echoed and internals are not enumerated to peers.
 */
export const DHT_REC_STORE_REJECT_REASONS = Object.freeze([
  "invalid-record",
  "slot-mismatch",
  "epoch-floor",
  "immutable-conflict",
  "older-record",
  "quota",
  "rate-limited",
  "internal",
]);
const REJECT_REASON_SET = new Set(DHT_REC_STORE_REJECT_REASONS);

/** Map a local verify/store reason to the bounded wire vocabulary. */
export function boundedRejectReason(localReason) {
  const text = typeof localReason === "string" ? localReason : "";
  if (text === "epoch-floor" || text === "epoch-unreadable") return "epoch-floor";
  if (text === "immutable") return "immutable-conflict";
  if (text === "older-record") return "older-record";
  if (text === "publisher-record-quota" || text === "publisher-byte-quota") return "quota";
  if (text === "key-mismatch" || text === "slot-mismatch") return "slot-mismatch";
  if (text === "rate-limited") return "rate-limited";
  if (text.startsWith("bad-") || text === "expired" || text === "too-large"
    || text === "invalid" || text === "kind-requires-v2" || text.startsWith("sig")
    || text.startsWith("authority") || text.startsWith("cert")) return "invalid-record";
  return "internal";
}

const HEX64_RE = /^[0-9a-f]{64}$/;

export class DhtRecordStoreRequestV1 extends RRecord {
  static type = "DhtRecordStoreRequestV1";

  constructor({ protocolVersion, requestId, key, record } = {}) {
    super();
    this.ctl = CTL_DHT_REC_STORE;
    this.protocolVersion = Number(protocolVersion);
    this.requestId = typeof requestId === "string" ? requestId.trim() : "";
    this.key = typeof key === "string" ? key.trim() : "";
    this.record = record;
    if (this.constructor === DhtRecordStoreRequestV1) this._seal();
  }

  validate() {
    this.assert(this.protocolVersion === DHT_RECORD_STORE_PROTOCOL_VERSION,
      "DhtRecordStoreRequestV1.protocolVersion unsupported", { protocolVersion: this.protocolVersion });
    this.assert(this.requestId.length > 0 && this.requestId.length <= 128,
      "DhtRecordStoreRequestV1.requestId invalid");
    this.assert(HEX64_RE.test(this.key),
      "DhtRecordStoreRequestV1.key must be a 64-hex slot id", { key: this.key });
    this.assert(this.record !== null && typeof this.record === "object" && !Array.isArray(this.record),
      "DhtRecordStoreRequestV1.record must be an object");
  }
}

export class DhtRecordStoreAckV1 extends RRecord {
  static type = "DhtRecordStoreAckV1";

  constructor({ protocolVersion, requestId, key, recordDigestHex, status, reason = null } = {}) {
    super();
    this.ctl = CTL_DHT_REC_STORE_ACK;
    this.protocolVersion = Number(protocolVersion);
    this.requestId = typeof requestId === "string" ? requestId.trim() : "";
    this.key = typeof key === "string" ? key.trim() : "";
    this.recordDigestHex = typeof recordDigestHex === "string" ? recordDigestHex.trim() : "";
    this.status = status;
    this.reason = reason == null ? null : String(reason);
    if (this.constructor === DhtRecordStoreAckV1) this._seal();
  }

  validate() {
    this.assert(this.protocolVersion === DHT_RECORD_STORE_PROTOCOL_VERSION,
      "DhtRecordStoreAckV1.protocolVersion unsupported", { protocolVersion: this.protocolVersion });
    this.assert(this.requestId.length > 0 && this.requestId.length <= 128,
      "DhtRecordStoreAckV1.requestId invalid");
    this.assert(HEX64_RE.test(this.key), "DhtRecordStoreAckV1.key must be a 64-hex slot id", { key: this.key });
    this.assert(HEX64_RE.test(this.recordDigestHex),
      "DhtRecordStoreAckV1.recordDigestHex must be 64 hex chars", { recordDigestHex: this.recordDigestHex });
    const statuses = Object.values(DHT_REC_STORE_ACK_STATUS);
    this.assert(statuses.includes(this.status), "DhtRecordStoreAckV1.status invalid", { status: this.status });
    if (this.status === DHT_REC_STORE_ACK_STATUS.REJECTED) {
      this.assert(this.reason !== null && REJECT_REASON_SET.has(this.reason),
        "DhtRecordStoreAckV1.reason must be a bounded rejection reason", { reason: this.reason });
    } else {
      this.assert(this.reason === null, "DhtRecordStoreAckV1.reason must be null unless rejected", { reason: this.reason });
    }
  }
}
