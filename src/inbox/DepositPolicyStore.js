import { DepositPolicyV1 } from "@rezprotocol/core";

/**
 * Persistent per-inbox deposit-policy store.
 *
 * Keyed by inboxId. The relay rejects deposits whose depositor pubkey
 * (session.ownerPublicKeyB64) is blocked by the stored policy. Absence of
 * a policy means default-allow per docs/CAPABILITY_MODEL.md §2.
 */
const STORE_KEY = "node:deposit-policy:v1";

export class DepositPolicyStore {
  #kv;
  #policies;
  #hydrated;

  constructor({ storageProvider } = {}) {
    if (!storageProvider || typeof storageProvider.getKeyValueStore !== "function") {
      throw new Error("DepositPolicyStore requires storageProvider.getKeyValueStore()");
    }
    this.#kv = storageProvider.getKeyValueStore(null);
    /** @type {Map<string, DepositPolicyV1>} */
    this.#policies = new Map();
    this.#hydrated = false;
  }

  async hydrate() {
    if (this.#hydrated) return;
    const stored = await this.#kv.get(STORE_KEY);
    const entries = Array.isArray(stored && stored.policies) ? stored.policies : [];
    for (const entry of entries) {
      try {
        const policy = DepositPolicyV1.fromJSON(entry);
        if (!policy.isExpired()) {
          this.#policies.set(policy.inboxId, policy);
        }
      } catch {
        // Skip malformed entries; never crash hydrate on bad data.
      }
    }
    this.#hydrated = true;
  }

  /**
   * Replace the stored policy for the given inbox. Caller is responsible for
   * verifying the policy's signature BEFORE calling this.
   *
   * Monotonic policy versioning: rejects a policy with a lower or equal
   * policyVersion than the currently stored one, preventing replay of an
   * older (more-permissive) policy.
   *
   * @param {DepositPolicyV1} policy
   * @returns {Promise<DepositPolicyV1>} the stored policy
   * @throws {Error} POLICY_VERSION_STALE / POLICY_EXPIRED
   */
  async put(policy) {
    if (!this.#hydrated) {
      throw new Error("DepositPolicyStore.put() called before hydrate()");
    }
    if (!(policy instanceof DepositPolicyV1)) {
      throw new Error("DepositPolicyStore.put requires DepositPolicyV1");
    }
    if (policy.isExpired()) {
      const err = new Error("policy already expired");
      err.code = "POLICY_EXPIRED";
      throw err;
    }
    const existing = this.#policies.get(policy.inboxId);
    if (existing && policy.policyVersion <= existing.policyVersion) {
      const err = new Error("policyVersion must strictly increase");
      err.code = "POLICY_VERSION_STALE";
      throw err;
    }
    this.#policies.set(policy.inboxId, policy);
    try {
      await this.#persist();
    } catch (persistErr) {
      if (existing) {
        this.#policies.set(policy.inboxId, existing);
      } else {
        this.#policies.delete(policy.inboxId);
      }
      throw persistErr;
    }
    return policy;
  }

  /**
   * Fetch the policy for `inboxId`, or null if none is registered or the
   * stored policy has expired.
   * @param {string} inboxId
   * @returns {DepositPolicyV1 | null}
   */
  get(inboxId) {
    if (!this.#hydrated) {
      throw new Error("DepositPolicyStore.get() called before hydrate()");
    }
    if (typeof inboxId !== "string" || inboxId.length === 0) return null;
    const policy = this.#policies.get(inboxId);
    if (!policy) return null;
    if (policy.isExpired()) {
      this.#policies.delete(inboxId);
      return null;
    }
    return policy;
  }

  async #persist() {
    const policies = [];
    for (const policy of this.#policies.values()) {
      policies.push(policy.toJSON());
    }
    await this.#kv.set(STORE_KEY, { policies });
  }
}
