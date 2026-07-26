/**
 * Disk persistence for the durable-record ROLLBACK FLOOR — the highest epoch this node has ever
 * accepted into each epoch-ordered slot (see DurableRecordStore#epochFloors).
 *
 * A separate store from DurableRecordPersistence on purpose, not a namespace inside it: the two
 * have opposite lifetimes. A record is removed the moment it expires; its floor must SURVIVE that
 * expiry, because the empty slot is precisely when a replayed older snapshot would win. Sharing one
 * keyspace would also make `loadAll()` ambiguous — every value there is expected to be a retention
 * entry.
 *
 * Keyed by the slot id (a sha256 hex, already a safe single-segment store key). Values are
 * `{ epoch, ownerPublicKeyB64, observedAtMs }`.
 */
export class DurableRecordEpochFloorPersistence {
  /** @type {import("@rezprotocol/core").RDataStore} */
  #store;

  /**
   * @param {{ store: object }} options
   */
  constructor({ store }) {
    if (!store || typeof store.put !== "function" || typeof store.list !== "function") {
      throw new Error("DurableRecordEpochFloorPersistence requires a data store with put/list");
    }
    this.#store = store;
  }

  /**
   * Write through one slot's floor. Idempotent — the store overwrites by key, and a floor is only
   * ever re-written when it rises.
   *
   * @param {{ localId: string, epoch: number, ownerPublicKeyB64: string, observedAtMs: number }} entry
   * @returns {Promise<void>}
   */
  async put(entry) {
    if (!entry || typeof entry.localId !== "string" || entry.localId.trim().length === 0) {
      throw new Error("DurableRecordEpochFloorPersistence.put requires an entry with a localId");
    }
    if (!Number.isSafeInteger(entry.epoch) || entry.epoch < 0) {
      throw new Error("DurableRecordEpochFloorPersistence.put requires a non-negative safe-integer epoch");
    }
    await this.#store.put(entry.localId.trim(), {
      epoch: entry.epoch,
      ownerPublicKeyB64: typeof entry.ownerPublicKeyB64 === "string" ? entry.ownerPublicKeyB64 : "",
      observedAtMs: Number.isFinite(entry.observedAtMs) ? entry.observedAtMs : 0,
    });
  }

  /**
   * Load every persisted floor. Shape mirrors DurableRecordStore.loadEpochFloors, which drops
   * malformed entries loudly — this layer does not silently repair them.
   *
   * There is deliberately NO remove(): a floor is never deleted. It is the one piece of state whose
   * whole job is to outlive the record it came from.
   *
   * @returns {Promise<Array<{ localId: string, epoch: number, ownerPublicKeyB64: string, observedAtMs: number }>>}
   */
  async loadAll() {
    const { items } = await this.#store.list("");
    const out = [];
    for (const { key, value } of items) {
      if (!value || typeof value !== "object") continue;
      out.push({
        localId: key,
        epoch: value.epoch,
        ownerPublicKeyB64: value.ownerPublicKeyB64,
        observedAtMs: value.observedAtMs,
      });
    }
    return out;
  }
}
