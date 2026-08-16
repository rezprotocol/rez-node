/**
 * Typed lookup report (ATLAS_PREREQUISITES P3.2).
 *
 * The RRecord half of a lookup result: bounded counters and the completion
 * reason. Live candidate entries (which carry raw sockets) ride alongside it
 * on the plain result object — an RRecord deep-freezes on seal, and freezing
 * live socket handles would corrupt the transport, so handles never enter
 * the record. `closestRelayKeyIds` is the record's view of the candidate set.
 */
import { RRecord } from "@rezprotocol/core";

export const DHT_LOOKUP_COMPLETION_REASONS = Object.freeze([
  "value-found",
  "converged",
  "deadline",
  "budget",
  "no-candidates",
]);
const REASON_SET = new Set(DHT_LOOKUP_COMPLETION_REASONS);

function asCount(value, label, record) {
  const num = Number(value);
  record.assert(Number.isInteger(num) && num >= 0, "DhtLookupReportV1." + label + " must be a non-negative integer", { [label]: value });
  return num;
}

export class DhtLookupReportV1 extends RRecord {
  static type = "DhtLookupReportV1";

  constructor({
    valueFound = false,
    closestRelayKeyIds = [],
    queriedCount = 0,
    dialAttemptCount = 0,
    timeoutCount = 0,
    rejectedCandidateCount = 0,
    completionReason,
  } = {}) {
    super();
    this.valueFound = valueFound === true;
    this.closestRelayKeyIds = Array.isArray(closestRelayKeyIds) ? closestRelayKeyIds.slice() : closestRelayKeyIds;
    this.queriedCount = asCount(queriedCount, "queriedCount", this);
    this.dialAttemptCount = asCount(dialAttemptCount, "dialAttemptCount", this);
    this.timeoutCount = asCount(timeoutCount, "timeoutCount", this);
    this.rejectedCandidateCount = asCount(rejectedCandidateCount, "rejectedCandidateCount", this);
    this.completionReason = completionReason;
    if (this.constructor === DhtLookupReportV1) this._seal();
  }

  validate() {
    this.assert(Array.isArray(this.closestRelayKeyIds), "DhtLookupReportV1.closestRelayKeyIds must be an array");
    for (const id of this.closestRelayKeyIds) {
      this.assert(typeof id === "string" && id.length > 0, "DhtLookupReportV1.closestRelayKeyIds entries must be strings");
    }
    this.assert(REASON_SET.has(this.completionReason),
      "DhtLookupReportV1.completionReason invalid", { completionReason: this.completionReason });
    if (this.valueFound) {
      this.assert(this.completionReason === "value-found", "valueFound requires completionReason value-found");
    }
  }
}
