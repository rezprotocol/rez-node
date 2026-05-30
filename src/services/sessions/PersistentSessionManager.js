import { MemorySessionManager, RatchetState, RatchetKeyPair, SecureSessionRecord, isBytes } from "@rezprotocol/core";
import { RSessionStore } from "@rezprotocol/core";
import { ratchetStateToJson, ratchetStateFromJson } from "./serializeRatchetStateV1.js";

/**
 * Check whether the store has encryption configured.
 * When the store has encryption, we pass the SecureSessionRecord
 * from the in-memory Map instead of converting to a plain object first.
 */
function storeSupportsEncryption(store) {
  return store && store.encryptionEnabled === true;
}

function sidHex(sid) {
  return Buffer.from(sid).toString("hex");
}

/**
 * Session health states. A peer enters "degraded" after consecutive
 * persist failures, indicating the ratchet may be out of sync.
 */
const HEALTH = Object.freeze({
  HEALTHY: "healthy",
  DEGRADED: "degraded",
});

const DEGRADE_THRESHOLD = 3;

export class PersistentSessionManager {
  #versions = new Map();
  #health = new Map();
  #onHealthChange = null;

  constructor({ inner, store } = {}) {
    if (!(inner instanceof MemorySessionManager)) {
      throw new Error("PersistentSessionManager requires inner (MemorySessionManager)");
    }
    if (!(store instanceof RSessionStore)) {
      throw new Error("PersistentSessionManager requires store (RSessionStore)");
    }

    this.inner = inner;
    this.store = store;
  }

  static HEALTH = HEALTH;

  /**
   * Set a callback for health state changes.
   * Called with (peerId, healthState, consecutiveFailures).
   */
  setOnHealthChange(fn) {
    this.#onHealthChange = typeof fn === "function" ? fn : null;
  }

  /**
   * Get the health state for a peer.
   * @param {string} peerId
   * @returns {{ state: string, consecutiveFailures: number }}
   */
  getHealth(peerId) {
    const entry = this.#health.get(peerId);
    if (!entry) {
      return { state: HEALTH.HEALTHY, consecutiveFailures: 0 };
    }
    return {
      state: entry.consecutiveFailures >= DEGRADE_THRESHOLD ? HEALTH.DEGRADED : HEALTH.HEALTHY,
      consecutiveFailures: entry.consecutiveFailures,
    };
  }

  #recordSuccess(peerId) {
    const entry = this.#health.get(peerId);
    if (!entry || entry.consecutiveFailures === 0) return;
    const wasDegraded = entry.consecutiveFailures >= DEGRADE_THRESHOLD;
    entry.consecutiveFailures = 0;
    if (wasDegraded && this.#onHealthChange) {
      try { this.#onHealthChange(peerId, HEALTH.HEALTHY, 0); } catch { /* ignore */ }
    }
  }

  #recordFailure(peerId) {
    let entry = this.#health.get(peerId);
    if (!entry) {
      entry = { consecutiveFailures: 0 };
      this.#health.set(peerId, entry);
    }
    const wasDegraded = entry.consecutiveFailures >= DEGRADE_THRESHOLD;
    entry.consecutiveFailures += 1;
    const isDegraded = entry.consecutiveFailures >= DEGRADE_THRESHOLD;
    if (!wasDegraded && isDegraded && this.#onHealthChange) {
      try { this.#onHealthChange(peerId, HEALTH.DEGRADED, entry.consecutiveFailures); } catch { /* ignore */ }
    }
  }

  async createInitiatorSession(args = {}) {
    const sid = await this.inner.createInitiatorSession(args);
    await this.persistByPeer(args.peerId);
    return sid;
  }

  async createResponderSession(args = {}) {
    const sid = await this.inner.createResponderSession(args);
    await this.persistByPeer(args.peerId);
    return sid;
  }

  getSendContext(peerId) {
    const ctx = this.inner.getSendContext(peerId);
    const innerCommit = ctx.commit;
    return {
      ...ctx,
      commit: async (nextState, opts) => {
        // Save pre-commit state for rollback
        const prevState = ctx.ratchetState;
        const prevIncludeDh = ctx.includeDh;

        innerCommit(nextState, opts);
        try {
          await this.persistByPeer(peerId);
          this.#recordSuccess(peerId);
        } catch (err) {
          // Rollback in-memory state to match what's on disk
          innerCommit(prevState, { keepIncludeDh: true });
          const record = this.inner.byPeerId.get(peerId);
          if (record) {
            record.includeDh = prevIncludeDh;
          }
          this.#recordFailure(peerId);
          throw err;
        }
      },
    };
  }

  getRecvContext(sid) {
    const ctx = this.inner.getRecvContext(sid);
    const innerCommit = ctx.commit;
    return {
      ...ctx,
      commit: async (nextState) => {
        // Save pre-commit state for rollback
        const prevState = ctx.ratchetState;

        innerCommit(nextState);
        try {
          await this.persistBySid(sid);
          this.#recordSuccess(ctx.peerId);
        } catch (err) {
          // Rollback in-memory state to match what's on disk
          innerCommit(prevState);
          this.#recordFailure(ctx.peerId);
          throw err;
        }
      },
    };
  }

  async loadAll({ onError } = {}) {
    const records = await this.store.list();
    for (const record of records) {
      try {
        this.upsertRecord(record);
      } catch (err) {
        if (onError) onError(err, record);
        else throw err;
      }
    }
  }

  upsertRecord(record) {
    if (!record || typeof record !== "object") {
      throw new Error("PersistentSessionManager.upsertRecord requires record");
    }

    let rec;
    if (record instanceof SecureSessionRecord) {
      // Encrypted path: record is already a validated SecureSessionRecord
      rec = record;
    } else {
      // Legacy plaintext path: build SecureSessionRecord from plain object
      if (!isBytes(record.sid)) {
        throw new Error("PersistentSessionManager.upsertRecord requires sid Uint8Array");
      }
      if (typeof record.peerId !== "string" || record.peerId.trim().length === 0) {
        throw new Error("PersistentSessionManager.upsertRecord requires peerId");
      }
      if (typeof record.includeDh !== "boolean") {
        throw new Error("PersistentSessionManager.upsertRecord requires includeDh boolean");
      }
      const ratchetState = ratchetStateFromJson(record.ratchetState);
      rec = new SecureSessionRecord({
        sid: record.sid,
        peerId: record.peerId,
        ratchetState,
        includeDh: record.includeDh,
      });
    }

    const sidKey = sidHex(rec.sid);
    this.inner.bySidHex.set(sidKey, rec);
    this.inner.byPeerId.set(rec.peerId, rec);

    // Track the version from disk so optimistic locking works
    const version = typeof record.version === "number" && record.version > 0 ? record.version : 1;
    this.#versions.set(rec.peerId, version);
    this.#versions.set(sidKey, version);
  }

  async persistByPeer(peerId) {
    const record = this.inner.byPeerId.get(peerId);
    if (!record) return;
    const currentVersion = this.#versions.get(peerId) || 0;

    let newVersion;
    if (storeSupportsEncryption(this.store)) {
      // Encrypted path: pass SecureSessionRecord directly
      newVersion = await this.store.putEncrypted(record, { version: currentVersion });
    } else {
      // Legacy plaintext path
      const sessionRecord = this.toSessionRecord(record);
      sessionRecord.version = currentVersion;
      newVersion = await this.store.put(sessionRecord);
    }

    if (typeof newVersion === "number" && newVersion > 0) {
      this.#versions.set(peerId, newVersion);
      this.#versions.set(sidHex(record.sid), newVersion);
    }
  }

  async persistBySid(sid) {
    if (!isBytes(sid)) {
      throw new Error("PersistentSessionManager.persistBySid requires sid Uint8Array");
    }
    const key = sidHex(sid);
    const record = this.inner.bySidHex.get(key);
    if (!record) return;
    const currentVersion = this.#versions.get(key) || 0;

    let newVersion;
    if (storeSupportsEncryption(this.store)) {
      newVersion = await this.store.putEncrypted(record, { version: currentVersion });
    } else {
      const sessionRecord = this.toSessionRecord(record);
      sessionRecord.version = currentVersion;
      newVersion = await this.store.put(sessionRecord);
    }

    if (typeof newVersion === "number" && newVersion > 0) {
      this.#versions.set(key, newVersion);
      this.#versions.set(record.peerId, newVersion);
    }
  }

  toSessionRecord(record) {
    if (!(record.ratchetState instanceof RatchetState)) {
      throw new Error("PersistentSessionManager requires RatchetState");
    }

    return {
      v: 1,
      sid: record.sid,
      peerId: record.peerId,
      includeDh: record.includeDh,
      ratchetState: ratchetStateToJson(record.ratchetState),
    };
  }
}
