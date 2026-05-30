/**
 * Inbox-claim registry — the node's only source of truth for who owns an inbox.
 *
 * Maps inboxId → { claimantPublicKeyB64, claimedAtMs }. Persistent across
 * restarts. This is the trust root for every owner-scoped operation: the node
 * validates incoming requests by chaining capability signatures back to the
 * claimant pubkey stored here (see docs/CAPABILITY_MODEL.md).
 *
 * Deliberately knows NOTHING about accounts. Per the multi-tenant memo, the
 * node operates in inbox-ID space only — there is no `accountId → inboxId`
 * mapping anywhere in this class, and there will not be.
 *
 * v1 invariants:
 *   - Open registration: any inbox may be claimed by the first caller with a
 *     valid signature; no allowlist, no gate.
 *   - Inbox IDs are caller-supplied (SDK-generated random strings). The node
 *     does not derive them. If a caller submits a colliding ID, the claim
 *     fails — the SDK retries with a fresh ID.
 *   - Once claimed, the claimant pubkey is the permanent trust root for that
 *     inbox. Key rotation requires explicit re-claim with a transition record
 *     (out of scope for v1).
 *
 * Concurrency model (closes docs/SECURITY_AUDIT.md MED-6):
 *   - All writes serialize through a promise-chain mutex (`#writeQueue`) so
 *     concurrent claims for different inboxIds don't overwrite each other's
 *     persist, AND so a duplicate-check + persist + in-memory commit runs
 *     as one critical section per claim.
 *   - The in-memory `#claims` map is updated ONLY after the KV write
 *     resolves. Readers (`getClaimantPublicKey`, `hasInbox`, etc.) never
 *     observe transient state, so authz decisions never anchor against a
 *     claim that may roll back.
 */

const STORE_KEY = "node:inbox:claims:v1";

export class InboxClaimRegistry {
  #kv;
  #claims;
  #hydrated;
  #writeQueue;

  /**
   * @param {{ storageProvider: import("@rezprotocol/core").StorageProvider }} opts
   */
  constructor({ storageProvider } = {}) {
    if (!storageProvider || typeof storageProvider.getKeyValueStore !== "function") {
      throw new Error("InboxClaimRegistry requires storageProvider.getKeyValueStore()");
    }
    this.#kv = storageProvider.getKeyValueStore(null);
    /** @type {Map<string, { claimantPublicKeyB64: string, claimedAtMs: number }>} */
    this.#claims = new Map();
    this.#hydrated = false;
    // Single-writer serialization. Each claim() chains its async work
    // behind the previous one's completion so we never have two
    // concurrent persists running against stale snapshots of #claims.
    this.#writeQueue = Promise.resolve();
  }

  /**
   * Load all persisted claims into memory. Idempotent.
   * @returns {Promise<void>}
   */
  async hydrate() {
    if (this.#hydrated) return;
    const stored = await this.#kv.get(STORE_KEY);
    const entries = Array.isArray(stored && stored.claims) ? stored.claims : [];
    for (const entry of entries) {
      const normalized = this.#normalizeStoredEntry(entry);
      if (normalized) {
        this.#claims.set(normalized.inboxId, {
          claimantPublicKeyB64: normalized.claimantPublicKeyB64,
          claimedAtMs: normalized.claimedAtMs,
        });
      }
    }
    this.#hydrated = true;
  }

  /**
   * Register a new inbox claim. Fails if the inboxId is already claimed
   * (collision or replay). Caller is responsible for signature verification
   * BEFORE calling this — the registry persists what it's told.
   *
   * @param {{ inboxId: string, claimantPublicKeyB64: string, claimedAtMs: number }} record
   * @throws {Error} INBOX_ALREADY_CLAIMED if the inbox is already in the registry
   * @returns {Promise<{ inboxId: string, claimantPublicKeyB64: string, claimedAtMs: number }>}
   */
  async claim({ inboxId, claimantPublicKeyB64, claimedAtMs } = {}) {
    if (!this.#hydrated) {
      throw new Error("InboxClaimRegistry.claim() called before hydrate()");
    }
    const id = this.#normalize(inboxId);
    const pubkey = this.#normalize(claimantPublicKeyB64);
    const at = Number(claimedAtMs);
    if (!id) throw new Error("InboxClaimRegistry.claim requires inboxId");
    if (!pubkey) throw new Error("InboxClaimRegistry.claim requires claimantPublicKeyB64");
    if (!Number.isFinite(at) || at <= 0) {
      throw new Error("InboxClaimRegistry.claim requires positive claimedAtMs");
    }

    // Enter the write critical section: serialize behind any in-flight
    // claim and queue the next one behind us.
    const previous = this.#writeQueue;
    let releaseNext;
    this.#writeQueue = new Promise((resolve) => { releaseNext = resolve; });

    try {
      await previous;
      // Inside the mutex. Re-check against the durable map.
      if (this.#claims.has(id)) {
        const err = new Error("inbox already claimed");
        err.code = "INBOX_ALREADY_CLAIMED";
        throw err;
      }
      const record = { claimantPublicKeyB64: pubkey, claimedAtMs: at };
      // Persist FIRST. If kv.set throws, #claims is untouched — there is
      // no transient state for a reader or a subsequent claim to observe.
      const proposed = new Map(this.#claims);
      proposed.set(id, record);
      await this.#persist(proposed);
      // KV write succeeded → atomically swap in the new map so readers
      // see the durable view.
      this.#claims = proposed;
      return { inboxId: id, claimantPublicKeyB64: pubkey, claimedAtMs: at };
    } finally {
      releaseNext();
    }
  }

  /**
   * Look up the claimant pubkey for an inbox.
   * @param {string} inboxId
   * @returns {string | null} claimantPublicKeyB64 or null if not claimed
   */
  getClaimantPublicKey(inboxId) {
    if (!this.#hydrated) {
      throw new Error("InboxClaimRegistry.getClaimantPublicKey() called before hydrate()");
    }
    const id = this.#normalize(inboxId);
    if (!id) return null;
    const record = this.#claims.get(id);
    return record ? record.claimantPublicKeyB64 : null;
  }

  /**
   * Check whether an inbox is claimed.
   * @param {string} inboxId
   * @returns {boolean}
   */
  hasInbox(inboxId) {
    if (!this.#hydrated) {
      throw new Error("InboxClaimRegistry.hasInbox() called before hydrate()");
    }
    const id = this.#normalize(inboxId);
    if (!id) return false;
    return this.#claims.has(id);
  }

  /**
   * All currently-claimed inbox IDs.
   * @returns {string[]}
   */
  listInboxIds() {
    if (!this.#hydrated) {
      throw new Error("InboxClaimRegistry.listInboxIds() called before hydrate()");
    }
    return Array.from(this.#claims.keys());
  }

  /**
   * The count of claimed inboxes. Useful for metrics.
   * @returns {number}
   */
  size() {
    if (!this.#hydrated) {
      throw new Error("InboxClaimRegistry.size() called before hydrate()");
    }
    return this.#claims.size;
  }

  async #persist(claimsMap) {
    const claims = [];
    for (const [inboxId, record] of claimsMap.entries()) {
      claims.push({
        inboxId,
        claimantPublicKeyB64: record.claimantPublicKeyB64,
        claimedAtMs: record.claimedAtMs,
      });
    }
    await this.#kv.set(STORE_KEY, { claims });
  }

  #normalize(value) {
    return typeof value === "string" && value.trim() ? value.trim() : null;
  }

  #normalizeStoredEntry(entry) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) return null;
    const inboxId = this.#normalize(entry.inboxId);
    const claimantPublicKeyB64 = this.#normalize(entry.claimantPublicKeyB64);
    const claimedAtMs = Number(entry.claimedAtMs);
    if (!inboxId || !claimantPublicKeyB64 || !Number.isFinite(claimedAtMs) || claimedAtMs <= 0) {
      return null;
    }
    return { inboxId, claimantPublicKeyB64, claimedAtMs };
  }
}
