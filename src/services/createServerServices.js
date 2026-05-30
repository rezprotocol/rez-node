import { KeystoreBlobStore } from "./keystore/KeystoreBlobStore.js";

/**
 * Construct node-side services. After Shape A, the node holds no per-account
 * crypto state — all PeerLinkService instances live in chat-server. The only
 * thing this factory still owns is the per-owner keystore blob (used by the
 * SDK to round-trip its keystore through the node's storage).
 */
export function createServerServices({
  storageProvider,
  ownerAccountId = null,
} = {}) {
  if (!storageProvider || typeof storageProvider.getKeyValueStore !== "function") {
    throw new Error("createServerServices requires storageProvider");
  }
  const localOwnerAccountId = String(ownerAccountId || "").trim();
  if (!localOwnerAccountId) {
    throw new Error("createServerServices requires ownerAccountId");
  }

  const keystoreStore = new KeystoreBlobStore({
    storageProvider,
    ownerAccountId: localOwnerAccountId,
  });

  return {
    keystore: keystoreStore,
  };
}

/**
 * Factory for creating per-account services (keystore).
 * Designed for injection into PerAccountServiceCache.
 * @param {{ storageProvider, ownerAccountId: string }} opts
 * @returns {{ keystore }}
 */
export function createPerAccountServices({ storageProvider, ownerAccountId } = {}) {
  const keystoreStore = new KeystoreBlobStore({ storageProvider, ownerAccountId });
  return { keystore: keystoreStore };
}
