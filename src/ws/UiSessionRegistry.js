/**
 * Maps claimant pubkey → WS sessions belonging to that claimant.
 * The relay knows nothing about accounts; identity here is the pubkey that
 * claimed an inbox via inbox.claim and is bound to this session.
 */
export class UiSessionRegistry {
  constructor() {
    this._byOwner = new Map();
  }

  addSession({ ownerPublicKeyB64, session } = {}) {
    const owner = normalizeOwner(ownerPublicKeyB64);
    if (!owner || !session) return;
    let bucket = this._byOwner.get(owner);
    if (!bucket) {
      bucket = new Set();
      this._byOwner.set(owner, bucket);
    }
    bucket.add(session);
  }

  removeSession({ ownerPublicKeyB64, session } = {}) {
    const owner = normalizeOwner(ownerPublicKeyB64);
    if (!owner || !session) return;
    const bucket = this._byOwner.get(owner);
    if (!bucket) return;
    bucket.delete(session);
    if (bucket.size === 0) {
      this._byOwner.delete(owner);
    }
  }

  broadcastToOwner(ownerPublicKeyB64, frame) {
    const owner = normalizeOwner(ownerPublicKeyB64);
    if (!owner) return 0;
    const bucket = this._byOwner.get(owner);
    if (!bucket || bucket.size === 0) return 0;
    let sent = 0;
    for (const session of bucket) {
      if (!session || typeof session.send !== "function") continue;
      if (typeof session.isOpen === "function" && session.isOpen() !== true) continue;
      try {
        session.send(frame);
        sent += 1;
      } catch {
        // best effort broadcast
      }
    }
    return sent;
  }

  forEachOwnerSession(ownerPublicKeyB64, callback) {
    const owner = normalizeOwner(ownerPublicKeyB64);
    if (!owner || typeof callback !== "function") return 0;
    const bucket = this._byOwner.get(owner);
    if (!bucket || bucket.size === 0) return 0;
    let count = 0;
    for (const session of bucket) {
      if (!session || typeof session.isOpen === "function" && session.isOpen() !== true) continue;
      callback(session);
      count += 1;
    }
    return count;
  }

  /**
   * Returns the set of owner pubkeys with at least one session whose
   * localInboxId matches the given id.
   */
  getOwnerPublicKeysByInboxId(inboxId) {
    const id = typeof inboxId === "string" ? inboxId.trim() : "";
    if (!id) return new Set();
    const owners = new Set();
    for (const [ownerPublicKeyB64, bucket] of this._byOwner.entries()) {
      if (!ownerPublicKeyB64) continue;
      for (const session of bucket) {
        if (session && session.localInboxId === id) {
          owners.add(ownerPublicKeyB64);
          break;
        }
      }
    }
    return owners;
  }

  countSessions(ownerPublicKeyB64) {
    const owner = normalizeOwner(ownerPublicKeyB64);
    if (!owner) return 0;
    return this._byOwner.get(owner)?.size || 0;
  }

  countAll() {
    let count = 0;
    for (const bucket of this._byOwner.values()) {
      count += bucket.size;
    }
    return count;
  }
}

function normalizeOwner(ownerPublicKeyB64) {
  if (typeof ownerPublicKeyB64 !== "string") return null;
  const value = ownerPublicKeyB64.trim();
  return value || null;
}
