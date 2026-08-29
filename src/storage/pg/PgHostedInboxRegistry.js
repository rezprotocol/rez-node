/**
 * Postgres-backed registry of claimant delegations to hosted nodes.
 *
 * The database is authoritative across a cluster. This process keeps only the
 * registrations delegated to its own relay identity in memory because route
 * announcements and owner lookup are synchronous hot-path operations.
 *
 * A hosted registration is durable node state, not socket presence. Closing a
 * browser session must not remove it: the whole purpose of the registration is
 * to keep routing deposits to the durable home while the browser is offline.
 */
import { validateRelayIdentityBinding } from "@rezprotocol/core";

export class PgHostedInboxRegistry {
  #conn;
  #relayKeyId;
  #claimantToInbox;
  #onChange;

  constructor({ connection, relayKeyId } = {}) {
    if (!connection) throw new Error("PgHostedInboxRegistry requires connection");
    const relay = this.#normalize(relayKeyId);
    if (!relay) throw new Error("PgHostedInboxRegistry requires relayKeyId");
    this.#conn = connection;
    this.#relayKeyId = relay;
    this.#claimantToInbox = new Map();
    this.#onChange = null;
  }

  async hydrate() {
    const result = await this.#conn.query(
      "SELECT claimant_pubkey, delegation FROM hosted_inboxes WHERE delegation->>'relayKeyId' = $1",
      [this.#relayKeyId],
    );
    this.#claimantToInbox.clear();
    for (const row of result.rows) {
      const claimant = this.#normalize(row.claimant_pubkey);
      const registration = this.#normalizeRegistration(row.delegation);
      if (claimant && registration && registration.relayKeyId === this.#relayKeyId) {
        this.#claimantToInbox.set(claimant, registration);
      }
    }
  }

  async add(claimantPublicKeyB64, registration) {
    const claimant = this.#normalize(claimantPublicKeyB64);
    const next = this.#normalizeRegistration(registration);
    if (!claimant || !next) return;
    if (next.relayKeyId !== this.#relayKeyId) {
      throw new Error("PgHostedInboxRegistry registration relayKeyId does not match this node");
    }
    const previous = this.#claimantToInbox.get(claimant);
    if (this.#isSameRecord(previous, next)) return;
    await this.#conn.query(
      `INSERT INTO hosted_inboxes (claimant_pubkey, delegation)
       VALUES ($1, $2::jsonb)
       ON CONFLICT (claimant_pubkey) DO UPDATE
       SET delegation = EXCLUDED.delegation, updated_at = now()`,
      [claimant, JSON.stringify(next)],
    );
    this.#claimantToInbox.set(claimant, next);
    this.#notifyChange();
  }

  /**
   * Explicit administrative removal. Session disconnects do not call this in
   * durable mode; expired registrations are ignored by getRegistrations().
   */
  async remove(claimantPublicKeyB64) {
    const claimant = this.#normalize(claimantPublicKeyB64);
    if (!claimant) return;
    const previous = this.#claimantToInbox.get(claimant);
    if (!previous) return;
    await this.#conn.query(
      "DELETE FROM hosted_inboxes WHERE claimant_pubkey = $1 AND delegation->>'relayKeyId' = $2",
      [claimant, this.#relayKeyId],
    );
    this.#claimantToInbox.delete(claimant);
    this.#notifyChange();
  }

  getInboxIds() {
    const ids = new Set();
    for (const registration of this.#claimantToInbox.values()) {
      const inboxId = this.#normalize(registration && registration.inboxId);
      if (inboxId) ids.add(inboxId);
    }
    return Array.from(ids);
  }

  getOwnerPublicKeysForInbox(inboxId) {
    const inbox = this.#normalize(inboxId);
    const owners = new Set();
    if (!inbox) return owners;
    for (const [claimant, registration] of this.#claimantToInbox.entries()) {
      if (registration.inboxId === inbox) owners.add(claimant);
    }
    return owners;
  }

  getRegistrations() {
    const nowMs = Date.now();
    const registrations = [];
    for (const [claimantPublicKeyB64, record] of this.#claimantToInbox.entries()) {
      if (record.expiresAtMs <= nowMs) continue;
      registrations.push({ claimantPublicKeyB64, ...record });
    }
    return registrations;
  }

  setOnChange(fn) {
    this.#onChange = typeof fn === "function" ? fn : null;
  }

  #normalize(value) {
    return typeof value === "string" && value.trim() ? value.trim() : null;
  }

  #normalizeRegistration(registration) {
    if (!registration || typeof registration !== "object" || Array.isArray(registration)) return null;
    const inboxId = this.#normalize(registration.inboxId);
    const nodeKeyId = this.#normalize(registration.nodeKeyId);
    const nodePublicKeyB64 = this.#normalize(registration.nodePublicKeyB64);
    const relayKeyId = this.#normalize(registration.relayKeyId);
    const delegationSigB64 = this.#normalize(registration.delegationSigB64);
    const issuedAtMs = Number(registration.issuedAtMs);
    const expiresAtMs = Number(registration.expiresAtMs);
    if (!inboxId || !nodeKeyId || !nodePublicKeyB64 || !relayKeyId || !delegationSigB64) return null;
    if (!Number.isFinite(issuedAtMs) || !Number.isFinite(expiresAtMs)) return null;
    // ADR-RELAY-IDENTITY: the named delivery relay identity triple must bind.
    // A row naming this node's relayKeyId with someone else's key material is
    // invalid regardless of who wrote it to the shared database.
    const binding = validateRelayIdentityBinding({ relayKeyId, nodeKeyId, nodePublicKeyB64 });
    if (binding.ok !== true) return null;
    const out = {
      inboxId,
      nodeKeyId,
      nodePublicKeyB64,
      relayKeyId,
      issuedAtMs,
      expiresAtMs,
      delegationSigB64,
    };
    // Lease L1 (P1.3d fix, 2026-08-28 — the pg twin of the HostedInboxRegistry
    // projection defect): generation + retentionClass are INSIDE the
    // claimant's signed delegation bytes. This normalizer used to strip them
    // AT REST, so every lease-bearing (v2) claim hosted on a pg home was
    // announced upstream without its signed fields, failed signature
    // reconstruction at the receiving relay, and was unroutable across nodes.
    // ALL-OR-NONE; a partial pair is a corrupt row.
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
      && left.issuedAtMs === right.issuedAtMs
      && left.expiresAtMs === right.expiresAtMs
      && left.delegationSigB64 === right.delegationSigB64
      && left.generation === right.generation
      && left.retentionClass === right.retentionClass;
  }

  #notifyChange() {
    if (this.#onChange) this.#onChange();
  }
}
