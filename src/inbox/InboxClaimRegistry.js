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

import { RetentionPolicy } from "./RetentionPolicy.js";

const STORE_KEY = "node:inbox:claims:v1";

/**
 * Lease L2 lifecycle states (plans/PORTABLE_INBOX_LEASE_SPEC.md §3), derived
 * PURELY from durable state + now — never from an in-process timer, so a
 * restarted provider reconstructs the identical verdict from disk.
 */
export const INBOX_LIFECYCLE = Object.freeze({
  ACTIVE: "ACTIVE",                    // deposits yes, reads yes, renewal yes
  CLOSED_EXPIRED: "CLOSED_EXPIRED",    // deposits NO, reads yes, RENEWAL RESTORES ACTIVE
  CLOSED_TERMINAL: "CLOSED_TERMINAL",  // deposits NO, reads yes, renewal NEVER
  RECLAIMABLE: "RECLAIMABLE",          // grace over: due for the sweep; admission dead
  UNKNOWN: "UNKNOWN",                  // no claim (never registered, or reclaimed)
});

// Track 2 abuse quota: how many inboxes ONE claimant key may hold. Open registration means anyone
// with a keypair can claim; without a ceiling a single key can mint inboxes without bound and, since
// each inbox carries its own retention budget, multiply this node's storage by the claim count. The
// per-inbox item/byte caps bound each inbox — this bounds how many a claimant gets.
export const DEFAULT_MAX_INBOXES_PER_CLAIMANT = 32;

export class InboxClaimRegistry {
  #kv;
  #claims;
  #tombstones;
  #hydrated;
  #writeQueue;
  #maxInboxesPerClaimant;

  /**
   * @param {{ storageProvider: import("@rezprotocol/core").StorageProvider }} opts
   */
  #retentionPolicy;

  constructor({ storageProvider, maxInboxesPerClaimant = DEFAULT_MAX_INBOXES_PER_CLAIMANT, retentionPolicy = null } = {}) {
    if (!storageProvider || typeof storageProvider.getKeyValueStore !== "function") {
      throw new Error("InboxClaimRegistry requires storageProvider.getKeyValueStore()");
    }
    this.#retentionPolicy = retentionPolicy instanceof RetentionPolicy ? retentionPolicy : new RetentionPolicy();
    if (!Number.isInteger(maxInboxesPerClaimant) || maxInboxesPerClaimant < 1) {
      throw new Error("InboxClaimRegistry requires a positive integer maxInboxesPerClaimant");
    }
    this.#maxInboxesPerClaimant = maxInboxesPerClaimant;
    this.#kv = storageProvider.getKeyValueStore(null);
    /** @type {Map<string, { claimantPublicKeyB64: string, claimedAtMs: number, closePublicKeyB64?: string, generation?: number }>} */
    this.#claims = new Map();
    // Portable inbox lease L1 (plans/PORTABLE_INBOX_LEASE_SPEC.md §4):
    // (inboxId → { finalGeneration, closedAtMs }) tombstones from accepted
    // TerminalInboxClose records. A tombstone permanently kills its
    // generation: claims and leases at ≤ finalGeneration are refused, and
    // deposits to the inbox are refused. Retained through RECLAIMED — this
    // map IS the whole replay-protection story.
    /** @type {Map<string, { finalGeneration: number, closedAtMs: number }>} */
    this.#tombstones = new Map();
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
        this.#claims.set(normalized.inboxId, this.#claimRecordFrom(normalized));
      }
    }
    const tombstones = Array.isArray(stored && stored.tombstones) ? stored.tombstones : [];
    for (const entry of tombstones) {
      const normalized = this.#normalizeTombstone(entry);
      if (normalized) {
        this.#tombstones.set(normalized.inboxId, {
          finalGeneration: normalized.finalGeneration,
          closedAtMs: normalized.closedAtMs,
          reason: normalized.reason,
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
  async claim({ inboxId, claimantPublicKeyB64, claimedAtMs, closePublicKeyB64 = null, generation = null, retentionClass = null, leaseExpiresAtMs = null } = {}) {
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
    // Lease L1: v2 fields are ALL-OR-NONE (the caller verified them inside
    // the signed claim payload; a partial pair here is a caller bug).
    const closePub = this.#normalize(closePublicKeyB64);
    const gen = generation === null ? null : Number(generation);
    if ((closePub !== null) !== (gen !== null)) {
      throw new Error("InboxClaimRegistry.claim requires closePublicKeyB64 and generation together or neither");
    }
    if (gen !== null && (!Number.isInteger(gen) || gen < 1)) {
      throw new Error("InboxClaimRegistry.claim generation must be a positive integer");
    }
    // Lease L2: v2 claims carry their lease's class + expiry so the retention
    // lifecycle is derivable from DURABLE state alone. All-or-none with the
    // v2 pair; class is fixed at claim time.
    const leaseClass = this.#normalize(retentionClass);
    const leaseExpiry = leaseExpiresAtMs === null ? null : Number(leaseExpiresAtMs);
    if (gen !== null) {
      if (leaseClass === null || !this.#retentionPolicy.isKnownClass(leaseClass)) {
        throw new Error("InboxClaimRegistry.claim: v2 claims require a known retentionClass");
      }
      if (!Number.isFinite(leaseExpiry) || leaseExpiry <= 0) {
        throw new Error("InboxClaimRegistry.claim: v2 claims require positive leaseExpiresAtMs");
      }
    } else if (leaseClass !== null || leaseExpiry !== null) {
      throw new Error("InboxClaimRegistry.claim: legacy claims carry no lease fields");
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
      // Lease L1 generation kill rule, M6 reason-scoped (§7e — same semantic
      // as the handler and lifecycleFor): "terminal" kills the LINEAGE (any
      // generation refused, forever); "reclaimed" kills ≤ finalGeneration
      // and a strictly higher generation starts a fresh lifetime.
      const tombstone = this.#tombstones.get(id);
      if (tombstone) {
        const tombstoneReason = tombstone.reason === "reclaimed" ? "reclaimed" : "terminal";
        if (tombstoneReason === "terminal" || gen === null || gen <= tombstone.finalGeneration) {
          const err = new Error(tombstoneReason === "reclaimed"
            ? "inbox generation was reclaimed at generation " + tombstone.finalGeneration
            : "inbox is terminally closed at generation " + tombstone.finalGeneration);
          err.code = "INBOX_CLOSED";
          err.closeReason = tombstoneReason;
          err.finalGeneration = tombstone.finalGeneration;
          throw err;
        }
      }
      // The ceiling is counted INSIDE the mutex, so two concurrent claims by one key cannot both
      // read a count below the limit and both pass.
      let held = 0;
      for (const record of this.#claims.values()) {
        if (record.claimantPublicKeyB64 === pubkey) held += 1;
      }
      if (held >= this.#maxInboxesPerClaimant) {
        const err = new Error(
          "claimant already holds " + held + " inboxes (max " + this.#maxInboxesPerClaimant + ")",
        );
        err.code = "INBOX_CLAIM_QUOTA_EXCEEDED";
        throw err;
      }
      const record = { claimantPublicKeyB64: pubkey, claimedAtMs: at };
      if (gen !== null) {
        record.closePublicKeyB64 = closePub;
        record.generation = gen;
        record.retentionClass = leaseClass;
        record.leaseExpiresAtMs = leaseExpiry;
      }
      // Persist FIRST. If kv.set throws, #claims is untouched — there is
      // no transient state for a reader or a subsequent claim to observe.
      const proposed = new Map(this.#claims);
      proposed.set(id, record);
      await this.#persist(proposed, this.#tombstones);
      // KV write succeeded → atomically swap in the new map so readers
      // see the durable view.
      this.#claims = proposed;
      return { inboxId: id, claimantPublicKeyB64: pubkey, claimedAtMs: at };
    } finally {
      releaseNext();
    }
  }

  /**
   * Record an accepted TerminalInboxClose (lease L1). The CALLER verified the
   * close-key signature and generation equality against the stored claim —
   * this registry persists the monotonic fact. Idempotent: re-closing at the
   * same (or lower) generation is a no-op. The claim record is KEPT (CLOSED
   * means drain-your-mail-then-die: claimant reads stay authorized through
   * the grace window; L2 reclamation removes both).
   * @param {{ inboxId: string, finalGeneration: number, closedAtMs: number }} record
   */
  async recordTerminalClose({ inboxId, finalGeneration, closedAtMs } = {}) {
    if (!this.#hydrated) {
      throw new Error("InboxClaimRegistry.recordTerminalClose() called before hydrate()");
    }
    const id = this.#normalize(inboxId);
    const gen = Number(finalGeneration);
    const at = Number(closedAtMs);
    if (!id) throw new Error("recordTerminalClose requires inboxId");
    if (!Number.isInteger(gen) || gen < 1) throw new Error("recordTerminalClose requires positive integer finalGeneration");
    if (!Number.isFinite(at) || at <= 0) throw new Error("recordTerminalClose requires positive closedAtMs");

    const previous = this.#writeQueue;
    let releaseNext;
    this.#writeQueue = new Promise((resolve) => { releaseNext = resolve; });
    try {
      await previous;
      const existing = this.#tombstones.get(id);
      if (existing && existing.finalGeneration >= gen) {
        return { inboxId: id, finalGeneration: existing.finalGeneration, closedAtMs: existing.closedAtMs };
      }
      const proposed = new Map(this.#tombstones);
      proposed.set(id, { finalGeneration: gen, closedAtMs: at, reason: "terminal" });
      await this.#persist(this.#claims, proposed);
      this.#tombstones = proposed;
      return { inboxId: id, finalGeneration: gen, closedAtMs: at };
    } finally {
      releaseNext();
    }
  }

  /**
   * Renew a v2 claim's lease (L2): a valid reattestation extends the stored
   * leaseExpiresAtMs. Class is FIXED at claim time — a renewal presenting a
   * different class is refused. Monotonic: never moves the expiry backwards.
   * The CALLER has already verified the reattestation signature, the record
   * consistency, and the lifecycle admissibility (renewal is legal in ACTIVE
   * and CLOSED_EXPIRED, never in CLOSED_TERMINAL/RECLAIMABLE).
   */
  async renewLease({ inboxId, retentionClass, leaseExpiresAtMs } = {}) {
    if (!this.#hydrated) {
      throw new Error("InboxClaimRegistry.renewLease() called before hydrate()");
    }
    const id = this.#normalize(inboxId);
    const leaseClass = this.#normalize(retentionClass);
    const expiry = Number(leaseExpiresAtMs);
    if (!id) throw new Error("renewLease requires inboxId");
    if (!leaseClass || !this.#retentionPolicy.isKnownClass(leaseClass)) {
      throw new Error("renewLease requires a known retentionClass");
    }
    if (!Number.isFinite(expiry) || expiry <= 0) throw new Error("renewLease requires positive leaseExpiresAtMs");

    const previous = this.#writeQueue;
    let releaseNext;
    this.#writeQueue = new Promise((resolve) => { releaseNext = resolve; });
    try {
      await previous;
      const existing = this.#claims.get(id);
      if (!existing || !Number.isInteger(existing.generation)) {
        const err = new Error("renewLease: no v2 claim for " + id);
        err.code = "UNKNOWN_INBOX";
        throw err;
      }
      if (existing.retentionClass !== leaseClass) {
        const err = new Error("renewLease: retentionClass is fixed at claim time");
        err.code = "CLAIM_RECORD_MISMATCH";
        throw err;
      }
      const nextExpiry = Math.max(Number(existing.leaseExpiresAtMs) || 0, expiry);
      const proposed = new Map(this.#claims);
      proposed.set(id, { ...existing, leaseExpiresAtMs: nextExpiry });
      await this.#persist(proposed, this.#tombstones);
      this.#claims = proposed;
      return { inboxId: id, leaseExpiresAtMs: nextExpiry };
    } finally {
      releaseNext();
    }
  }

  /**
   * Lease L2: the PURE lifecycle verdict for an inbox, derived entirely from
   * durable state (claim, stored lease expiry, tombstone) + the supplied
   * clock reading. NO timers, NO cached transitions — a provider restarted
   * after any amount of downtime computes the identical answer, which is the
   * property the adversarial spike attacks hardest.
   *
   * @param {string} inboxId
   * @param {number} nowMs
   * @returns {{ state: string, reason: string|null, graceEndsAtMs: number|null }}
   */
  lifecycleFor(inboxId, nowMs) {
    if (!this.#hydrated) {
      throw new Error("InboxClaimRegistry.lifecycleFor() called before hydrate()");
    }
    const id = this.#normalize(inboxId);
    const now = Number(nowMs);
    if (!id || !Number.isFinite(now)) {
      throw new Error("lifecycleFor requires inboxId and a finite nowMs");
    }
    const claim = this.#claims.get(id);
    const tombstone = this.#tombstones.get(id);

    // The tombstone is consulted FIRST so no lease state can shadow it — but
    // M6 (rez-chat plans/MOBILE_LIFECYCLE_ADAPTER_PLAN.md §7e): it GOVERNS
    // only the generations it killed. Two reasons, two scopes (frozen):
    //   "terminal"  — the close key killed the inboxId LINEAGE: governs every
    //                 claim forever (admission also refuses all future claims;
    //                 this branch is belt-and-braces for pre-hardening data).
    //   "reclaimed" — expiry reclamation killed generations ≤ finalGeneration:
    //                 a live claim at a HIGHER generation is a fresh lifetime
    //                 (admitted over the tombstone by the claim handler) and
    //                 is evaluated by its own lease below.
    // Whether a reason is eligible for client auto-re-mint is deliberately
    // NOT this method's question — the node stays generic.
    if (tombstone) {
      const tombstoneReason = tombstone.reason === "reclaimed" ? "reclaimed" : "terminal";
      const claimGeneration = claim && Number.isInteger(claim.generation) ? claim.generation : null;
      const governs = tombstoneReason === "terminal"
        || !claim
        || claimGeneration === null
        || claimGeneration <= tombstone.finalGeneration;
      if (governs) {
        const cls = claim && Number.isInteger(claim.generation) ? claim.retentionClass : "transient";
        const graceEndsAtMs = tombstone.closedAtMs + this.#retentionPolicy.terminalGraceMs(cls);
        if (!claim) {
          // Already reclaimed (or never held here): only the tombstone remains.
          return { state: INBOX_LIFECYCLE.UNKNOWN, reason: "terminal", graceEndsAtMs: null };
        }
        return now < graceEndsAtMs
          ? { state: INBOX_LIFECYCLE.CLOSED_TERMINAL, reason: "terminal", graceEndsAtMs }
          : { state: INBOX_LIFECYCLE.RECLAIMABLE, reason: "terminal", graceEndsAtMs };
      }
      // Fresh lifetime past a reclaimed tombstone: fall through to the normal
      // claim + lease evaluation.
    }

    if (!claim) {
      return { state: INBOX_LIFECYCLE.UNKNOWN, reason: null, graceEndsAtMs: null };
    }
    // Legacy claims, and classes whose expiry does not drive retention
    // (transient = legacy-identical), are permanently ACTIVE here — RMailbox
    // retention/caps govern their mail exactly as shipped.
    if (!Number.isInteger(claim.generation) || !this.#retentionPolicy.expiryLifecycleApplies(claim.retentionClass)) {
      return { state: INBOX_LIFECYCLE.ACTIVE, reason: null, graceEndsAtMs: null };
    }
    const expiresAtMs = Number(claim.leaseExpiresAtMs);
    if (now < expiresAtMs) {
      return { state: INBOX_LIFECYCLE.ACTIVE, reason: null, graceEndsAtMs: null };
    }
    const graceEndsAtMs = expiresAtMs + this.#retentionPolicy.leaseGraceMs(claim.retentionClass);
    return now < graceEndsAtMs
      ? { state: INBOX_LIFECYCLE.CLOSED_EXPIRED, reason: "expired", graceEndsAtMs }
      : { state: INBOX_LIFECYCLE.RECLAIMABLE, reason: "expired", graceEndsAtMs };
  }

  /**
   * All inboxIds whose lifecycle verdict at `nowMs` is RECLAIMABLE — the
   * sweep's work list. Pure read; the sweep calls markReclaimed per inbox.
   */
  reclaimDue(nowMs) {
    if (!this.#hydrated) {
      throw new Error("InboxClaimRegistry.reclaimDue() called before hydrate()");
    }
    const due = [];
    for (const inboxId of this.#claims.keys()) {
      if (this.lifecycleFor(inboxId, nowMs).state === INBOX_LIFECYCLE.RECLAIMABLE) {
        due.push(inboxId);
      }
    }
    return due;
  }

  /**
   * Reclaim an inbox (L2): re-derives the verdict INSIDE the mutex (never
   * trusts a stale caller decision), removes the claim record, and ensures a
   * tombstone exists for the generation — expiry-reclamation tombstones too,
   * so a stale lease of a reclaimed lifetime can never re-activate it (want
   * the address back? random inboxIds are free — mint a new one). The caller
   * (sweeper) is responsible for purging the stored ciphertext.
   * @returns {Promise<{ inboxId: string, reclaimed: boolean }>}
   */
  async markReclaimed(inboxId, nowMs) {
    if (!this.#hydrated) {
      throw new Error("InboxClaimRegistry.markReclaimed() called before hydrate()");
    }
    const id = this.#normalize(inboxId);
    if (!id) throw new Error("markReclaimed requires inboxId");

    const previous = this.#writeQueue;
    let releaseNext;
    this.#writeQueue = new Promise((resolve) => { releaseNext = resolve; });
    try {
      await previous;
      const verdict = this.lifecycleFor(id, nowMs);
      if (verdict.state !== INBOX_LIFECYCLE.RECLAIMABLE) {
        return { inboxId: id, reclaimed: false };
      }
      const claim = this.#claims.get(id);
      const proposedClaims = new Map(this.#claims);
      proposedClaims.delete(id);
      const proposedTombstones = new Map(this.#tombstones);
      if (!proposedTombstones.has(id)) {
        proposedTombstones.set(id, {
          finalGeneration: claim && Number.isInteger(claim.generation) ? claim.generation : 1,
          closedAtMs: Number(nowMs),
          reason: "reclaimed",
        });
      }
      await this.#persist(proposedClaims, proposedTombstones);
      this.#claims = proposedClaims;
      this.#tombstones = proposedTombstones;
      return { inboxId: id, reclaimed: true };
    } finally {
      releaseNext();
    }
  }

  /**
   * The tombstone for a terminally-closed inbox, or null.
   * @param {string} inboxId
   * @returns {{ finalGeneration: number, closedAtMs: number } | null}
   */
  getTombstone(inboxId) {
    if (!this.#hydrated) {
      throw new Error("InboxClaimRegistry.getTombstone() called before hydrate()");
    }
    const id = this.#normalize(inboxId);
    if (!id) return null;
    const record = this.#tombstones.get(id);
    return record ? { finalGeneration: record.finalGeneration, closedAtMs: record.closedAtMs, reason: record.reason || "terminal" } : null;
  }

  /**
   * The full stored claim record (claimant key, and for v2 claims the
   * closePublicKeyB64 + generation), or null.
   * @param {string} inboxId
   */
  getClaim(inboxId) {
    if (!this.#hydrated) {
      throw new Error("InboxClaimRegistry.getClaim() called before hydrate()");
    }
    const id = this.#normalize(inboxId);
    if (!id) return null;
    const record = this.#claims.get(id);
    return record ? { inboxId: id, ...this.#claimRecordFrom(record) } : null;
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

  async #persist(claimsMap, tombstonesMap) {
    const claims = [];
    for (const [inboxId, record] of claimsMap.entries()) {
      claims.push({ inboxId, ...this.#claimRecordFrom(record) });
    }
    const tombstones = [];
    for (const [inboxId, record] of tombstonesMap.entries()) {
      tombstones.push({ inboxId, finalGeneration: record.finalGeneration, closedAtMs: record.closedAtMs, reason: record.reason || "terminal" });
    }
    await this.#kv.set(STORE_KEY, { claims, tombstones });
  }

  #claimRecordFrom(entry) {
    const record = {
      claimantPublicKeyB64: entry.claimantPublicKeyB64,
      claimedAtMs: entry.claimedAtMs,
    };
    if (Number.isInteger(entry.generation)) {
      record.closePublicKeyB64 = entry.closePublicKeyB64;
      record.generation = entry.generation;
      record.retentionClass = entry.retentionClass;
      record.leaseExpiresAtMs = entry.leaseExpiresAtMs;
    }
    return record;
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
    const out = { inboxId, claimantPublicKeyB64, claimedAtMs };
    // Lease fields: ALL-OR-NONE across the v2 quad — a partial row is
    // corruption, drop it rather than half-adopting.
    const closePublicKeyB64 = this.#normalize(entry.closePublicKeyB64);
    const generation = Number(entry.generation);
    const retentionClass = this.#normalize(entry.retentionClass);
    const leaseExpiresAtMs = Number(entry.leaseExpiresAtMs);
    const hasClose = closePublicKeyB64 !== null;
    const hasGen = Number.isInteger(generation) && generation >= 1;
    const hasClass = retentionClass !== null;
    const hasExpiry = Number.isFinite(leaseExpiresAtMs) && leaseExpiresAtMs > 0;
    if (hasClose || hasGen || hasClass || hasExpiry) {
      if (!(hasClose && hasGen && hasClass && hasExpiry)) return null;
      out.closePublicKeyB64 = closePublicKeyB64;
      out.generation = generation;
      out.retentionClass = retentionClass;
      out.leaseExpiresAtMs = leaseExpiresAtMs;
    }
    return out;
  }

  #normalizeTombstone(entry) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) return null;
    const inboxId = this.#normalize(entry.inboxId);
    const finalGeneration = Number(entry.finalGeneration);
    const closedAtMs = Number(entry.closedAtMs);
    if (!inboxId || !Number.isInteger(finalGeneration) || finalGeneration < 1
      || !Number.isFinite(closedAtMs) || closedAtMs <= 0) {
      return null;
    }
    // L2: `reason` is observability metadata ("terminal" | "reclaimed") — the
    // admission consequence is identical either way. Legacy rows default to
    // "terminal" (the only reason that existed when they were written).
    const reason = entry.reason === "reclaimed" ? "reclaimed" : "terminal";
    return { inboxId, finalGeneration, closedAtMs, reason };
  }
}
