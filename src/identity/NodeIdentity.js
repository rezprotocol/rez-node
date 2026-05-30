import { createHash, generateKeyPairSync, randomBytes } from "node:crypto";

const STORE_KEY = "substrate:nodeIdentity:v1";

export async function ensureNodeIdentity({ storageProvider, configuredIdentity } = {}) {
  const explicitIdentity = ensureIdentityShape(configuredIdentity);
  if (explicitIdentity) return explicitIdentity;

  const kv = storageProvider?.getKeyValueStore?.();
  if (!kv) return generateIdentity();

  const loadedRaw = await kv.get(STORE_KEY);
  const loadedIdentity = ensureIdentityShape(loadedRaw);
  if (loadedIdentity) {
    if (!hasMeshAuthMaterial(loadedRaw)) {
      await kv.set(STORE_KEY, loadedIdentity);
    }
    return loadedIdentity;
  }

  const generated = generateIdentity();
  await kv.set(STORE_KEY, generated);
  return generated;
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

function generateIdentity() {
  const rand = () => Buffer.from(randomBytes(4)).toString("hex");
  return {
    accountId: `rez:node:${rand()}`,
    deviceId: `dev:${rand()}`,
    localInboxId: `inbox:${rand()}`,
    ...generateMeshAuthMaterial(),
  };
}
