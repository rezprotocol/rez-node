/**
 * Honest replication result for DhtNode.putRecord (ATLAS_PREREQUISITES P4.3).
 *
 * Truth rules, enforced by construction and by the producer:
 * - local storage NEVER counts as a remote replica;
 * - a successful socket write is only an attempt;
 * - only authenticated `stored`/`refreshed` acknowledgements count as remote
 *   holders;
 * - one relay ID counts at most once;
 * - the counters must reconcile: every attempt settles as acked, rejected,
 *   or timed out; disconnected/skipped candidates were never attempts; and
 *   targetReplicaCount = attempted + disconnected + skipped.
 */
import { RRecord } from "@rezprotocol/core";

function asCount(value, label, record) {
  const num = Number(value);
  record.assert(Number.isInteger(num) && num >= 0, "DhtRecordPutResultV1." + label + " must be a non-negative integer", { [label]: value });
  return num;
}

export class DhtRecordPutResultV1 extends RRecord {
  static type = "DhtRecordPutResultV1";

  constructor({
    storedLocally,
    localId = null,
    attemptedRemote = 0,
    acknowledgedStored = 0,
    acknowledgedRefreshed = 0,
    rejectedRemote = 0,
    timedOutRemote = 0,
    disconnectedRemote = 0,
    skippedRemote = 0,
    targetReplicaCount = 0,
    completedAtMs,
    reason = null,
  } = {}) {
    super();
    this.storedLocally = storedLocally === true;
    this.localId = localId == null ? null : String(localId);
    this.attemptedRemote = asCount(attemptedRemote, "attemptedRemote", this);
    this.acknowledgedStored = asCount(acknowledgedStored, "acknowledgedStored", this);
    this.acknowledgedRefreshed = asCount(acknowledgedRefreshed, "acknowledgedRefreshed", this);
    this.rejectedRemote = asCount(rejectedRemote, "rejectedRemote", this);
    this.timedOutRemote = asCount(timedOutRemote, "timedOutRemote", this);
    this.disconnectedRemote = asCount(disconnectedRemote, "disconnectedRemote", this);
    this.skippedRemote = asCount(skippedRemote, "skippedRemote", this);
    this.targetReplicaCount = asCount(targetReplicaCount, "targetReplicaCount", this);
    this.completedAtMs = Number(completedAtMs);
    this.reason = reason == null ? null : String(reason);
    if (this.constructor === DhtRecordPutResultV1) this._seal();
  }

  /** Remote holders that actually acknowledged holding the record. */
  get acknowledgedRemote() {
    return this.acknowledgedStored + this.acknowledgedRefreshed;
  }

  validate() {
    this.assert(Number.isFinite(this.completedAtMs), "DhtRecordPutResultV1.completedAtMs must be finite", { completedAtMs: this.completedAtMs });
    const settled = this.acknowledgedStored + this.acknowledgedRefreshed
      + this.rejectedRemote + this.timedOutRemote;
    this.assert(settled === this.attemptedRemote,
      "DhtRecordPutResultV1 counters must reconcile (attempted = acked + rejected + timedOut)",
      { attemptedRemote: this.attemptedRemote, settled });
    this.assert(this.targetReplicaCount === this.attemptedRemote + this.disconnectedRemote + this.skippedRemote,
      "DhtRecordPutResultV1 counters must reconcile (target = attempted + disconnected + skipped)",
      { targetReplicaCount: this.targetReplicaCount, attemptedRemote: this.attemptedRemote, disconnectedRemote: this.disconnectedRemote, skippedRemote: this.skippedRemote });
    if (this.storedLocally) {
      this.assert(this.localId !== null && this.localId.length > 0, "DhtRecordPutResultV1.localId required when storedLocally");
    }
  }
}
