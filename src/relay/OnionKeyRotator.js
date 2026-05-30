import { OnionKeyringV1, OnionKeyRecordV1 } from "@rezprotocol/core";

const DEFAULT_TTL_MS = 86_400_000 * 30; // 30 days
const DEFAULT_ROTATE_AT_FRACTION = 0.8;
const CLOCK_SKEW_MS = 3_600_000; // 1 hour backdated notBefore

/**
 * Manages onion key lifecycle: generates keys, rotates at a fraction of TTL,
 * keeps old keys in "draining" until notAfter, then revokes them.
 * Caller wires onDescriptorUpdate to republish the descriptor with getActiveKeyRecords().
 */
export class OnionKeyRotator {
  constructor({
    cryptoProvider,
    keyring,
    onDescriptorUpdate,
    deviceId,
    ttlMs = DEFAULT_TTL_MS,
    rotateAtFraction = DEFAULT_ROTATE_AT_FRACTION,
    nowMs = () => Date.now(),
  } = {}) {
    if (!cryptoProvider || typeof cryptoProvider.dhGenerateKeyPair !== "function") {
      throw new Error("OnionKeyRotator requires cryptoProvider with dhGenerateKeyPair");
    }
    if (!(keyring instanceof OnionKeyringV1)) {
      throw new Error("OnionKeyRotator requires OnionKeyringV1 keyring");
    }
    if (typeof onDescriptorUpdate !== "function") {
      throw new Error("OnionKeyRotator requires onDescriptorUpdate function");
    }
    if (typeof deviceId !== "string" || deviceId.trim().length === 0) {
      throw new Error("OnionKeyRotator requires deviceId string");
    }
    this._crypto = cryptoProvider;
    this._keyring = keyring;
    this._onDescriptorUpdate = onDescriptorUpdate;
    this._deviceId = String(deviceId).trim();
    this._ttlMs = Number(ttlMs) || DEFAULT_TTL_MS;
    this._rotateAtFraction = Number(rotateAtFraction) || DEFAULT_ROTATE_AT_FRACTION;
    this._nowMs = typeof nowMs === "function" ? nowMs : () => Date.now();

    /** @type {{ onionKeyId: string, publicKeyBytes: Uint8Array, privateKeyBytes: Uint8Array, format: string, createdAt: number, notBefore: number, notAfter: number, status: string }[]} */
    this._records = [];
    this._rotationTimer = null;
    this._revokeTimers = new Map();
    this._started = false;
  }

  start() {
    if (this._started) return;
    this._started = true;
    this._generateAndAddKey("active");
    this._scheduleNextRotation();
  }

  stop() {
    this._started = false;
    if (this._rotationTimer) {
      clearTimeout(this._rotationTimer);
      this._rotationTimer = null;
    }
    for (const t of this._revokeTimers.values()) {
      clearTimeout(t);
    }
    this._revokeTimers.clear();
  }

  /**
   * Returns current key records (active + draining, not revoked, not yet expired) for descriptor publishing.
   * @param {{ nowMs?: number }} opts
   * @returns {import("@rezprotocol/core").OnionKeyRecordV1[]}
   */
  getActiveKeyRecords({ nowMs = this._nowMs() } = {}) {
    const now = Number(nowMs);
    return this._records
      .filter(
        (r) =>
          (r.status === "active" || r.status === "draining") &&
          r.notBefore <= now &&
          now < r.notAfter
      )
      .map(
        (r) =>
          new OnionKeyRecordV1({
            v: 1,
            onionKeyId: r.onionKeyId,
            publicKeyBytes: r.publicKeyBytes,
            format: r.format,
            createdAt: r.createdAt,
            notBefore: r.notBefore,
            notAfter: r.notAfter,
            status: r.status,
          })
      );
  }

  _generateAndAddKey(status) {
    const nowMs = this._nowMs();
    const onionKeyId = `${this._deviceId}-${nowMs}`;
    const pair = this._crypto.dhGenerateKeyPair({ alg: "X25519", fmt: "spki" });
    const notBefore = nowMs - CLOCK_SKEW_MS;
    const notAfter = nowMs + this._ttlMs;

    this._keyring.addKey({
      onionKeyId,
      privateKeyBytes: pair.privateKey,
      notBefore,
      notAfter,
      status,
    });

    const record = {
      onionKeyId,
      publicKeyBytes: pair.publicKey,
      privateKeyBytes: pair.privateKey,
      format: "spki",
      createdAt: nowMs,
      notBefore,
      notAfter,
      status,
    };
    this._records.push(record);
    return record;
  }

  _scheduleNextRotation() {
    if (!this._started) return;
    const active = this._records.find((r) => r.status === "active");
    if (!active) return;

    const rotateAtMs = active.createdAt + this._ttlMs * this._rotateAtFraction;
    const nowMs = this._nowMs();
    const delayMs = Math.max(0, rotateAtMs - nowMs);

    this._rotationTimer = setTimeout(() => {
      this._rotationTimer = null;
      this._rotate();
      this._scheduleNextRotation();
    }, delayMs);
  }

  _rotate() {
    const active = this._records.find((r) => r.status === "active");
    if (!active) return;

    // Mark current active as draining in keyring and in _records
    this._keyring.addKey({
      onionKeyId: active.onionKeyId,
      privateKeyBytes: active.privateKeyBytes,
      notBefore: active.notBefore,
      notAfter: active.notAfter,
      status: "draining",
    });
    active.status = "draining";

    // Schedule revoke when draining key passes notAfter
    const revokeDelayMs = Math.max(0, active.notAfter - this._nowMs());
    const revokeTimer = setTimeout(() => {
      this._revokeKey(active.onionKeyId);
      this._revokeTimers.delete(active.onionKeyId);
    }, revokeDelayMs);
    this._revokeTimers.set(active.onionKeyId, revokeTimer);

    // Generate new active key
    this._generateAndAddKey("active");

    this._onDescriptorUpdate(this.getActiveKeyRecords());
  }

  _revokeKey(onionKeyId) {
    const record = this._records.find((r) => r.onionKeyId === onionKeyId);
    if (!record) return;

    this._keyring.addKey({
      onionKeyId,
      privateKeyBytes: record.privateKeyBytes,
      notBefore: record.notBefore,
      notAfter: record.notAfter,
      status: "revoked",
    });
    record.status = "revoked";
  }
}
