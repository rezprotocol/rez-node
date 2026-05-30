/**
 * Maps owner identity (claimant pubkey) → WS sessions belonging to that owner.
 * The relay does not know about accounts; identity here is the pubkey that
 * claimed an inbox via inbox.claim and is bound to this session.
 */
export class SessionRegistry {
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
      } catch (err) {
        console.error("[SessionRegistry] broadcastToOwner send failed for owner=" + owner + ": " + (err && err.message ? err.message : err));
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

  countSessions(ownerPublicKeyB64) {
    const owner = normalizeOwner(ownerPublicKeyB64);
    if (!owner) return 0;
    const bucket = this._byOwner.get(owner);
    return bucket ? bucket.size : 0;
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
