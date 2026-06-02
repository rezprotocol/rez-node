/**
 * Durable-record disk persistence — a thin async wrapper over an
 * RDataStore (FileSystemDataStore in production) so a relay's held records
 * survive restart. Keyed by the publisher-bound slot id (a sha256 hex,
 * already a safe single-segment store key). Each value is the full retention
 * entry `{ record, storedAtMs, ttlMs }`; expired entries are dropped on load.
 */
export class DurableRecordPersistence {
  /** @type {import("@rezprotocol/core").RDataStore} */
  #store;

  /**
   * @param {{ store: object }} options
   */
  constructor({ store }) {
    if (!store || typeof store.put !== "function" || typeof store.list !== "function"
      || typeof store.remove !== "function") {
      throw new Error("DurableRecordPersistence requires a data store with put/list/remove");
    }
    this.#store = store;
  }

  /**
   * @param {string} localId
   * @param {{ record: object, storedAtMs: number, ttlMs: number }} entry
   * @returns {Promise<void>}
   */
  async put(localId, entry) {
    await this.#store.put(localId, entry);
  }

  /**
   * @param {string} localId
   * @returns {Promise<boolean>}
   */
  async remove(localId) {
    return this.#store.remove(localId);
  }

  /**
   * Load every persisted entry. Shape mirrors DurableRecordStore.loadFromSnapshot.
   * @returns {Promise<Array<{ localId: string, record: object, storedAtMs: number, ttlMs: number }>>}
   */
  async loadAll() {
    const { items } = await this.#store.list("");
    const out = [];
    for (const { key, value } of items) {
      if (!value || typeof value !== "object") continue;
      out.push({
        localId: key,
        record: value.record,
        storedAtMs: value.storedAtMs,
        ttlMs: value.ttlMs,
      });
    }
    return out;
  }
}
