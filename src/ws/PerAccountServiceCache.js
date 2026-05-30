import { nonEmpty } from "@rezprotocol/core";

/**
 * Lazy cache of per-account services (thread store, thread index, backup store).
 * Used by connection handlers and WsGatewayServer to scope thread/message data per account.
 *
 * Requires a `createServices` factory that returns { threadStore, threadIndex, backup }
 * for a given { storageProvider, ownerAccountId, clock, backup }.
 */
export class PerAccountServiceCache {
  constructor({ storageProvider, clock = () => Date.now(), backup = null, createServices } = {}) {
    if (!storageProvider || typeof storageProvider.getKeyValueStore !== "function") {
      throw new Error("PerAccountServiceCache requires storageProvider.getKeyValueStore()");
    }
    if (typeof clock !== "function") {
      throw new Error("PerAccountServiceCache requires clock function");
    }
    if (typeof createServices !== "function") {
      throw new Error("PerAccountServiceCache requires createServices factory function");
    }
    this._storageProvider = storageProvider;
    this._clock = clock;
    this._backup = backup && typeof backup === "object" ? backup : {};
    this._createServices = createServices;
    this._cache = new Map();
  }

  getServices(ownerAccountId) {
    const owner = nonEmpty(ownerAccountId);
    if (!owner) {
      throw new Error("PerAccountServiceCache.getServices requires non-empty ownerAccountId");
    }
    let entry = this._cache.get(owner);
    if (!entry) {
      entry = this._createServices({
        storageProvider: this._storageProvider,
        ownerAccountId: owner,
        clock: this._clock,
        backup: this._backup,
      });
      this._cache.set(owner, entry);
    }
    return entry;
  }
}
