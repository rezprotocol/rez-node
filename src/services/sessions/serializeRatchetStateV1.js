import {
  RatchetState,
  RatchetChainState,
  RatchetKeyPair,
  SkippedKeyStore,
  isBytes,
} from "@rezprotocol/core";

function bytesToBase64(bytes) {
  return Buffer.from(bytes).toString("base64");
}

function base64ToBytes(value, label) {
  if (typeof value !== "string") {
    throw new Error(`serializeRatchetStateV1 ${label} must be base64 string`);
  }
  return new Uint8Array(Buffer.from(value, "base64"));
}

function encodeChain(chain) {
  if (!chain) return null;
  return {
    chainKey: bytesToBase64(chain.chainKey),
    messageIndex: chain.messageIndex,
  };
}

function decodeChain(chain) {
  if (!chain) return null;
  return new RatchetChainState({
    chainKey: base64ToBytes(chain.chainKey, "chainKey"),
    messageIndex: chain.messageIndex,
  });
}

function encodeSkipped(skipped) {
  if (!skipped) return { entries: [], totalBytes: 0 };
  return {
    entries: skipped.entries.map((entry) => ({ k: entry.k, mk: bytesToBase64(entry.mk) })),
    totalBytes: skipped.totalBytes,
  };
}

function decodeSkipped(skipped) {
  if (!skipped || !Array.isArray(skipped.entries)) {
    return new SkippedKeyStore();
  }
  return new SkippedKeyStore({
    entries: skipped.entries.map((entry) => ({ k: entry.k, mk: base64ToBytes(entry.mk, "skipped.mk") })),
  });
}

export function ratchetStateToJson(state) {
  if (!(state instanceof RatchetState)) {
    throw new Error("ratchetStateToJson requires RatchetState");
  }
  return {
    rootKey: bytesToBase64(state.rootKey),
    sendingChain: encodeChain(state.sendingChain),
    receivingChain: encodeChain(state.receivingChain),
    selfDhKeyPair: {
      publicKey: bytesToBase64(state.selfDhKeyPair.publicKey),
      privateKey: bytesToBase64(state.selfDhKeyPair.privateKey),
    },
    remoteDhPublicKey: bytesToBase64(state.remoteDhPublicKey),
    skipped: encodeSkipped(state.skipped),
    maxSkip: state.maxSkip,
    maxSkippedKeys: state.maxSkippedKeys,
    maxSkippedBytes: state.maxSkippedBytes,
  };
}

export function ratchetStateFromJson(json) {
  if (!json || typeof json !== "object") {
    throw new Error("ratchetStateFromJson requires object");
  }

  const rootKey = base64ToBytes(json.rootKey, "rootKey");
  const selfDh = json.selfDhKeyPair || {};
  const selfDhKeyPair = new RatchetKeyPair({
    publicKey: base64ToBytes(selfDh.publicKey, "selfDh.publicKey"),
    privateKey: base64ToBytes(selfDh.privateKey, "selfDh.privateKey"),
  });

  return new RatchetState({
    rootKey,
    sendingChain: decodeChain(json.sendingChain),
    receivingChain: decodeChain(json.receivingChain),
    selfDhKeyPair,
    remoteDhPublicKey: base64ToBytes(json.remoteDhPublicKey, "remoteDhPublicKey"),
    skipped: decodeSkipped(json.skipped),
    maxSkip: json.maxSkip,
    maxSkippedKeys: json.maxSkippedKeys,
    maxSkippedBytes: json.maxSkippedBytes,
  });
}

export function isRatchetStateJson(value) {
  return value && typeof value === "object" && typeof value.rootKey === "string";
}

export function ensureRatchetStateJson(value) {
  if (!value || typeof value !== "object") {
    throw new Error("ratchetState must be object");
  }
  if (!isRatchetStateJson(value)) {
    throw new Error("ratchetState must be serialized with base64 fields");
  }
  if (!isBytes(base64ToBytes(value.rootKey, "rootKey"))) {
    throw new Error("ratchetState rootKey invalid");
  }
}
