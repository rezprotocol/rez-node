import { createHash, generateKeyPairSync, randomBytes } from "node:crypto";

const STORE_KEY = "substrate:nodeIdentity:v1";

export async function ensureNodeIdentity({ storageProvider, configuredIdentity } = {}) {
  // A config identity that ALREADY carries node key material is fully pinned by
  // the operator — return it verbatim, generate and persist nothing.
  if (hasMeshAuthMaterial(configuredIdentity)) {
    const pinned = ensureIdentityShape(configuredIdentity);
    if (pinned) {
      return pinned;
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
  // drifted — so the node keys are reused (stable) on the next boot.
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
  return identity;
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
  const nodeKeyId = String(identity?.nodeKeyId || "").trim();
  const nodePublicKeyB64 = String(identity?.nodePublicKeyB64 || "").trim();
  const nodePrivateKeyB64 = String(identity?.nodePrivateKeyB64 || "").trim();
  return !!(nodeKeyId && nodePublicKeyB64 && nodePrivateKeyB64);
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
  const nodeKeyId = `nodekey:${createHash("sha256").update(publicKey).digest("hex").slice(0, 32)}`;
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

function generateIdentity() {
  return { ...generateCoreIds(), ...generateMeshAuthMaterial() };
}
