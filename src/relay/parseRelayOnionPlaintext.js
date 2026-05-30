import { isNonEmptyString } from "@rezprotocol/core";

const decoder = new TextDecoder();

function toBytes(value, label) {
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return new Uint8Array(value);
  if (typeof value === "string") return new Uint8Array(Buffer.from(value, "base64"));
  throw new Error(`parseRelayOnionPlaintext.${label} must be Uint8Array`);
}

export function parseRelayOnionPlaintext(plaintextBytes) {
  if (!(plaintextBytes instanceof Uint8Array)) {
    throw new Error("parseRelayOnionPlaintext requires Uint8Array");
  }
  const json = decoder.decode(plaintextBytes);
  const obj = JSON.parse(json);
  if (!obj || typeof obj !== "object") {
    throw new Error("parseRelayOnionPlaintext requires object");
  }
  if (!Number.isInteger(obj.ttl) || obj.ttl < 0) {
    throw new Error("parseRelayOnionPlaintext requires ttl >= 0");
  }

  const deliverInboxId = obj.deliver && isNonEmptyString(obj.deliver.inboxId)
    ? obj.deliver.inboxId
    : null;
  const receiptInboxId = obj.receipt && isNonEmptyString(obj.receipt.inboxId)
    ? obj.receipt.inboxId
    : null;

  let returnPath = null;
  if (obj.returnPath && typeof obj.returnPath === "object" && isNonEmptyString(obj.returnPath.entryRelayKeyId)) {
    returnPath = {
      pathEntries: Array.isArray(obj.returnPath.pathEntries) ? obj.returnPath.pathEntries : [],
      finalRelayKeyId: obj.returnPath.finalRelayKeyId ?? null,
      deliverInboxId: obj.returnPath.deliverInboxId ?? null,
      entryRelayKeyId: String(obj.returnPath.entryRelayKeyId).trim(),
    };
  }

  if (!deliverInboxId) {
    if (!obj.next || typeof obj.next !== "object") {
      throw new Error("parseRelayOnionPlaintext requires next when no deliver");
    }
    if (!isNonEmptyString(obj.next.relayKeyId)) {
      throw new Error("parseRelayOnionPlaintext requires next.relayKeyId when no deliver");
    }
  }

  const innerBytes = toBytes(obj.inner, "inner");
  if (innerBytes.length === 0) {
    throw new Error("parseRelayOnionPlaintext requires inner bytes");
  }

  return {
    ttl: obj.ttl,
    next: obj.next ?? null,
    inner: innerBytes,
    deliverInboxId,
    receiptInboxId,
    returnPath,
    flags: obj.flags ?? {},
  };
}
