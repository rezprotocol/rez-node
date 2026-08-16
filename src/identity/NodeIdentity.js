import { generateKeyPairSync, randomBytes } from "node:crypto";
import { deriveRelayIdentity, RelayIdentityMismatchError } from "../util/relayKeyId.js";

const STORE_KEY = "substrate:nodeIdentity:v1";

/**
 * Re-derive both identity IDs from the public key (ADR-RELAY-IDENTITY) and
 * return the identity with the canonical `relayKeyId` attached. A stored or
 * configured `nodeKeyId` that does not re-derive from the key is invalid —
 * it is never an override.
 */
function finalizeIdentity(identity) {
  let derived;
  try {
    derived = deriveRelayIdentity(identity.nodePublicKeyB64);
  } catch (err) {
    if (!(err instanceof Error)) throw err;
    throw new RelayIdentityMismatchError(
      "node identity public key is not a valid Ed25519 SPKI DER base64 key: " + err.message,
    );
  }
  if (identity.nodeKeyId !== derived.nodeKeyId) {
    throw new RelayIdentityMismatchError(
      "configured/persisted nodeKeyId does not re-derive from nodePublicKeyB64 (RELAY_IDENTITY_MISMATCH)",
    );
  }
  return { ...identity, relayKeyId: derived.relayKeyId };
}

export async function ensureNodeIdentity({ storageProvider, configuredIdentity } = {}) {
  // A config identity that ALREADY carries node key material is fully pinned by
  // the operator — accepted only when its IDs re-derive from the key; nothing
  // is generated or persisted for it.
  if (hasMeshAuthMaterial(configuredIdentity)) {
    const pinned = ensureIdentityShape(configuredIdentity);
    if (pinned) {
      return finalizeIdentity(pinned);
    }
  }

  const kv = storageProvider && typeof storageProvider.getKeyValueStore === "function"
    ? storageProvider.getKeyValueStore()
    : null;
  const persisted = kv ? await kv.get(STORE_KEY) : null;

  // Node key material MUST be stable across boots: it is the mesh-auth key and
  // (in fs mode) derives the at-rest storage key, so regenerating it per boot
  // would rotate the storage key and lose access to prior data. Reuse persisted
  // node keys; otherwise generate ONCE and persist to node-local storage. This
  // covers partial config identities (e.g. `rez-node init` writes account/device/
  // inbox only) and legacy persisted identities that predate mesh keys.
  const meshAuth = hasMeshAuthMaterial(persisted)
    ? {
        nodeKeyId: String(persisted.nodeKeyId).trim(),
        nodePublicKeyB64: String(persisted.nodePublicKeyB64).trim(),
        nodePrivateKeyB64: String(persisted.nodePrivateKeyB64).trim(),
      }
    : generateMeshAuthMaterial();

  // account/device/inbox: an explicit config wins, else persisted, else fresh.
  const coreSource = hasCoreIds(configuredIdentity)
    ? configuredIdentity
    : (hasCoreIds(persisted) ? persisted : generateCoreIds());
  const identity = {
    accountId: String(coreSource.accountId).trim(),
    deviceId: String(coreSource.deviceId).trim(),
    localInboxId: String(coreSource.localInboxId).trim(),
    ...meshAuth,
  };

  // Persist when storage exists and the stored copy is absent, key-less, or has
  // drifted — so the node keys are reused (stable) on the next boot. The
  // persisted shape stays key-material-only; relayKeyId is derivable and is
  // attached on the returned identity, never stored.
  if (kv) {
    const stored = persisted && typeof persisted === "object" ? persisted : null;
    const drift = !stored
      || !hasMeshAuthMaterial(stored)
      || stored.accountId !== identity.accountId
      || stored.deviceId !== identity.deviceId
      || stored.localInboxId !== identity.localInboxId;
    if (drift) {
      await kv.set(STORE_KEY, identity);
    }
  }
  return finalizeIdentity(identity);
}

export function ensureIdentityShape(identity) {
  if (!identity || typeof identity !== "object") return null;
  const accountId = String(identity.accountId || "").trim();
  const deviceId = String(identity.deviceId || "").trim();
  const localInboxId = String(identity.localInboxId || "").trim();
  if (!accountId || !deviceId || !localInboxId) return null;

  const meshAuth = ensureMeshAuthMaterial(identity);
  return {
    accountId,
    deviceId,
    localInboxId,
    nodeKeyId: meshAuth.nodeKeyId,
    nodePublicKeyB64: meshAuth.nodePublicKeyB64,
    nodePrivateKeyB64: meshAuth.nodePrivateKeyB64,
  };
}

function hasMeshAuthMaterial(identity) {
  if (!identity || typeof identity !== "object") {
    return false;
  }
  const nodeKeyId = String(identity.nodeKeyId || "").trim();
  const nodePublicKeyB64 = String(identity.nodePublicKeyB64 || "").trim();
  const nodePrivateKeyB64 = String(identity.nodePrivateKeyB64 || "").trim();
  return Boolean(nodeKeyId && nodePublicKeyB64 && nodePrivateKeyB64);
}

function ensureMeshAuthMaterial(identity) {
  if (hasMeshAuthMaterial(identity)) {
    return {
      nodeKeyId: String(identity.nodeKeyId).trim(),
      nodePublicKeyB64: String(identity.nodePublicKeyB64).trim(),
      nodePrivateKeyB64: String(identity.nodePrivateKeyB64).trim(),
    };
  }
  return generateMeshAuthMaterial();
}

function generateMeshAuthMaterial() {
  const { publicKey, privateKey } = generateKeyPairSync("ed25519", {
    publicKeyEncoding: { format: "der", type: "spki" },
    privateKeyEncoding: { format: "der", type: "pkcs8" },
  });
  const publicKeyB64 = Buffer.from(publicKey).toString("base64");
  const privateKeyB64 = Buffer.from(privateKey).toString("base64");
  // Derivation SSOT lives in rez-core; do not hash locally.
  const { nodeKeyId } = deriveRelayIdentity(publicKeyB64);
  return {
    nodeKeyId,
    nodePublicKeyB64: publicKeyB64,
    nodePrivateKeyB64: privateKeyB64,
  };
}

function hasCoreIds(identity) {
  if (!identity || typeof identity !== "object") {
    return false;
  }
  return Boolean(
    String(identity.accountId || "").trim()
      && String(identity.deviceId || "").trim()
      && String(identity.localInboxId || "").trim(),
  );
}

function generateCoreIds() {
  const rand = () => Buffer.from(randomBytes(4)).toString("hex");
  return {
    accountId: `rez:node:${rand()}`,
    deviceId: `dev:${rand()}`,
    localInboxId: `inbox:${rand()}`,
  };
}
