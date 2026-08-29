/**
 * Tracks which claimants are hosted on this node, keyed by claimant pubkey.
 *
 * Per the cap model (docs/CAPABILITY_MODEL.md), the claimant pubkey is the
 * trust root for an inbox. The node knows nothing about accounts — it routes
 * by inboxId and identifies hosted owners by pubkey only.
 *
 * Used by RelayConnectionPool to build inbox.register frames (announcing
 * claimant-signed delegations to upstream relays) and by the deposit router
 * to map an incoming inboxId back to the connected claimant sessions that
 * should be notified.
 */
import { validateRelayIdentityBinding } from "@rezprotocol/core";

const STORE_KEY = "substrate:hostedInboxRegistry:v2";

export class HostedInboxRegistry {
  #kv;
  #claimantToInbox;
  #onChange;

  /**
   * @param {{ storageProvider?: import("@rezprotocol/core").StorageProvider | null }} opts
   */
  constructor({ storageProvider = null } = {}) {
    this.#kv = storageProvider && typeof storageProvider.getKeyValueStore === "function"
      ? storageProvider.getKeyValueStore()
      : null;
    /** @type {Map<string, object>} claimantPublicKeyB64 -> delegation record */
    this.#claimantToInbox = new Map();
    this.#onChange = null;
  }

  /**
   * Load previously-learned claimant -> inbox mappings so routing survives node restarts.
   */
  async hydrate() {
    if (!this.#kv || typeof this.#kv.get !== "function") return;
    const snapshot = await this.#kv.get(STORE_KEY);
    for (const [claimantPublicKeyB64, record] of this.#readSnapshot(snapshot)) {
      this.#claimantToInbox.set(claimantPublicKeyB64, record);
    }
  }

  /**
   * Register a hosted claimant: their inbox is reachable on this node, with a
   * delegation signed by their claimant private key authorizing this node to
   * advertise the inbox to upstream relays.
   *
   * @param {string} claimantPublicKeyB64
   * @param {object} registration
   */
  async add(claimantPublicKeyB64, registration) {
    const key = this.#normalize(claimantPublicKeyB64);
    const nextRecord = this.#normalizeRegistration(registration);
    if (!key || !nextRecord) return;
    const previous = this.#claimantToInbox.get(key);
    if (this.#isSameRecord(previous, nextRecord)) return;
    this.#claimantToInbox.set(key, nextRecord);
    try {
      await this.#persist();
    } catch (err) {
      if (previous) this.#claimantToInbox.set(key, previous);
      else this.#claimantToInbox.delete(key);
      throw err;
    }
    this.#notifyChange();
  }

  /**
   * Unregister a hosted claimant (e.g. on session close).
   * @param {string} claimantPublicKeyB64
   */
  async remove(claimantPublicKeyB64) {
    const key = this.#normalize(claimantPublicKeyB64);
    if (!key) return;
    const previous = this.#claimantToInbox.get(key);
    if (!previous) return;
    this.#claimantToInbox.delete(key);
    try {
      await this.#persist();
    } catch (err) {
      this.#claimantToInbox.set(key, previous);
      throw err;
    }
    this.#notifyChange();
  }

  /**
   * All inbox IDs this node currently hosts. Used for relay registration and
   * inbound deposit demux allowlist.
   * @returns {string[]}
   */
  getInboxIds() {
    const out = [];
    const seen = new Set();
    for (const record of this.#claimantToInbox.values()) {
      const inbox = this.#normalize(record && record.inboxId);
      if (inbox && !seen.has(inbox)) {
        seen.add(inbox);
        out.push(inbox);
      }
    }
    return out;
  }

  /**
   * Returns all known claimant pubkeys reachable at the given inbox.
   * @param {string} inboxId
   * @returns {Set<string>}
   */
  getOwnerPublicKeysForInbox(inboxId) {
    const inbox = this.#normalize(inboxId);
    if (!inbox) return new Set();
    const owners = new Set();
    for (const [claimantPublicKeyB64, record] of this.#claimantToInbox.entries()) {
      if (record && record.inboxId === inbox) owners.add(claimantPublicKeyB64);
    }
    return owners;
  }

  /**
   * Get the current signed registrations announced to upstream relays.
   * Entries past expiry are filtered out.
   * @returns {object[]}
   */
  getRegistrations() {
    const nowMs = Date.now();
    const out = [];
    for (const [claimantPublicKeyB64, record] of this.#claimantToInbox.entries()) {
      if (!record || typeof record !== "object") continue;
      if (!this.#normalize(record.inboxId)) continue;
      if (!this.#normalize(record.nodeKeyId)) continue;
      if (!this.#normalize(record.nodePublicKeyB64)) continue;
      if (!this.#normalize(record.relayKeyId)) continue;
      if (!this.#normalize(record.delegationSigB64)) continue;
      const expiresAtMs = Number(record.expiresAtMs);
      if (!Number.isFinite(expiresAtMs) || expiresAtMs <= nowMs) continue;
      const projected = {
        inboxId: record.inboxId,
        claimantPublicKeyB64,
        nodeKeyId: record.nodeKeyId,
        nodePublicKeyB64: record.nodePublicKeyB64,
        relayKeyId: record.relayKeyId,
        issuedAtMs: Number(record.issuedAtMs) || null,
        expiresAtMs,
        delegationSigB64: record.delegationSigB64,
      };
      // Lease L1 (P1.3d fix, 2026-08-28): generation + retentionClass are
      // INSIDE the claimant's signed delegation bytes. This projection used
      // to DROP them, so every lease-bearing (v2) claim announced upstream
      // failed signature reconstruction at the receiving relay
      // (verifyClaimantNodeDelegation rebuilt the payload without the pair)
      // — the registration was rejected, no cross-node route ever existed,
      // and deposits to fresh post-F8 inboxes died with "no route to
      // target". The normalizer below this method has warned about exactly
      // this drop since L1 shipped; the projection just didn't listen.
      // ALL-OR-NONE, mirroring the stored record.
      if (Number.isInteger(record.generation)) {
        projected.generation = record.generation;
        projected.retentionClass = record.retentionClass;
      }
      out.push(projected);
    }
    return out;
  }

  /**
   * Set a callback invoked when add/remove changes the registry (used by the
   * relay-connection pool to re-announce upstream).
   */
  setOnChange(fn) {
    this.#onChange = typeof fn === "function" ? fn : null;
  }

  #normalize(value) {
    return typeof value === "string" && value.trim() ? value.trim() : null;
  }

  #readSnapshot(snapshot) {
    const entries = Array.isArray(snapshot && snapshot.claimantDelegations)
      ? snapshot.claimantDelegations
      : [];
    const out = [];
    for (const entry of entries) {
      if (!Array.isArray(entry) || entry.length !== 2) continue;
      const claimantPublicKeyB64 = this.#normalize(entry[0]);
      const record = this.#normalizeRegistration(entry[1]);
      if (!claimantPublicKeyB64 || !record) continue;
      out.push([claimantPublicKeyB64, record]);
    }
    return out;
  }

  async #persist() {
    if (!this.#kv || typeof this.#kv.set !== "function") return;
    await this.#kv.set(STORE_KEY, {
      claimantDelegations: Array.from(this.#claimantToInbox.entries()),
    });
  }

  #notifyChange() {
    if (this.#onChange) this.#onChange();
  }

  #normalizeRegistration(registration) {
    if (!registration || typeof registration !== "object" || Array.isArray(registration)) {
      return null;
    }
    const inboxId = this.#normalize(registration.inboxId);
    if (!inboxId) return null;
    const nodeKeyId = this.#normalize(registration.nodeKeyId);
    const nodePublicKeyB64 = this.#normalize(registration.nodePublicKeyB64);
    const relayKeyId = this.#normalize(registration.relayKeyId);
    // ADR-RELAY-IDENTITY: a registration that names a delivery relay must name
    // it by its self-certifying identity — the full (relayKeyId, nodeKeyId,
    // publicKey) triple must be present and must bind. Registrations without
    // any relay identity remain local-routing records.
    if (relayKeyId || nodeKeyId || nodePublicKeyB64) {
      const binding = validateRelayIdentityBinding({ relayKeyId, nodeKeyId, nodePublicKeyB64 });
      if (binding.ok !== true) return null;
    }
    const out = {
      inboxId,
      nodeKeyId,
      nodePublicKeyB64,
      relayKeyId,
      issuedAtMs: Number.isFinite(Number(registration.issuedAtMs)) ? Number(registration.issuedAtMs) : null,
      expiresAtMs: Number.isFinite(Number(registration.expiresAtMs)) ? Number(registration.expiresAtMs) : null,
      delegationSigB64: this.#normalize(registration.delegationSigB64),
    };
    // Lease L1: generation + retentionClass are INSIDE the signed delegation
    // bytes — dropping them here would break every downstream verifier that
    // reconstructs the payload. ALL-OR-NONE; a partial pair is a corrupt
    // registration.
    const generation = Number(registration.generation);
    const retentionClass = this.#normalize(registration.retentionClass);
    const hasGen = Number.isInteger(generation) && generation >= 1;
    if (hasGen !== (retentionClass !== null)) return null;
    if (hasGen) {
      out.generation = generation;
      out.retentionClass = retentionClass;
    }
    return out;
  }

  #isSameRecord(left, right) {
    if (!left || !right) return false;
    return left.inboxId === right.inboxId
      && left.nodeKeyId === right.nodeKeyId
      && left.nodePublicKeyB64 === right.nodePublicKeyB64
      && left.relayKeyId === right.relayKeyId
      && Number(left.issuedAtMs) === Number(right.issuedAtMs)
      && Number(left.expiresAtMs) === Number(right.expiresAtMs)
      && left.delegationSigB64 === right.delegationSigB64;
  }
}
