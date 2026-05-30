import { createHash } from "node:crypto";

export function clampListLimit(limit) {
  const n = Number(limit);
  if (!Number.isFinite(n)) return 50;
  return Math.max(1, Math.min(200, Math.trunc(n)));
}

export function clampInteger(value, { min = 1, max = Number.MAX_SAFE_INTEGER, fallback = min } = {}) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(n)));
}

export function normalizeBeforeCursor(before) {
  if (!before || typeof before !== "object") return null;
  const createdAtMs = Number(before.createdAtMs);
  const messageId = String(before.messageId || "").trim();
  if (!Number.isFinite(createdAtMs) || !messageId) return null;
  return {
    createdAtMs: Math.trunc(createdAtMs),
    messageId,
  };
}

export function toMessageItem(message) {
  const source = message && typeof message === "object" ? message : {};
  const status = normalizeMessageStatus(source.status);
  const createdAtMs = finiteOrFallback(source.createdAtMs, source.acceptedAtMs, Date.now());
  const item = {
    messageId: String(source.messageId == null ? source.packetId == null ? source.id == null ? "" : source.id : source.packetId : source.messageId),
    createdAtMs,
    senderAccountId:
      source.senderAccountId == null ? null : String(source.senderAccountId),
    status,
  };
  const messageId = String(source.messageId || "").trim();
  if (messageId) item.messageId = messageId;
  if (typeof source.packetB64 === "string" && source.packetB64.length > 0) {
    item.packetB64 = source.packetB64;
  }
  return item;
}

export function normalizeMessageStatus(status) {
  const normalized = String(status || "").trim();
  switch (normalized) {
    case "pending":
    case "sent":
    case "delivered":
    case "failed":
      return normalized;
    default:
      return "delivered";
  }
}

export function finiteOrFallback(value, fallbackA, fallbackB) {
  const first = Number(value);
  if (Number.isFinite(first)) return Math.trunc(first);
  const second = Number(fallbackA);
  if (Number.isFinite(second)) return Math.trunc(second);
  const third = Number(fallbackB);
  if (Number.isFinite(third)) return Math.trunc(third);
  return Date.now();
}

export function normalizeContactState(state) {
  const text = String(state || "").trim();
  if (!text) return null;
  if (text === "active" || text === "invited" || text === "blocked") return text;
  throw new Error("state must be active|invited|blocked");
}

export function normalizeInviteStatus(status) {
  const text = String(status || "").trim();
  if (!text) return null;
  if (text === "active" || text === "used" || text === "expired" || text === "revoked") return text;
  throw new Error("status must be active|used|expired|revoked");
}

export function normalizeInviteKind(kind, fallback = "direct") {
  const text = String(kind || "").trim() || fallback;
  if (text === "direct" || text === "group") return text;
  throw new Error("kind must be direct|group");
}

export function sanitizeTitle(title) {
  if (title == null) return null;
  const text = String(title).trim();
  if (!text) return null;
  return text.length > 64 ? text.slice(0, 64) : text;
}

export function newGroupId() {
  const digest = createHash("sha256")
    .update(`group:${Date.now()}:${Math.random()}`)
    .digest("base64url");
  return `grp_${digest.slice(0, 22)}`;
}

export function bufferFromBase64(str) {
  return new Uint8Array(Buffer.from(String(str), "base64"));
}

export function bufferToBase64(buf) {
  return Buffer.from(buf).toString("base64");
}

export function mapInviteErrorCode(err) {
  const code = String((err && err.code) || "").trim();
  if (!code) return "INVITE_INTERNAL_ERROR";
  switch (code) {
    case "INVITE_INVALID_FORMAT":
    case "INVITE_UNSUPPORTED_VERSION":
    case "INVITE_SIGNATURE_INVALID":
    case "INVITE_EXPIRED":
    case "INVITE_USED_UP":
    case "INVITE_REVOKED":
    case "INVITE_KIND_UNSUPPORTED":
    case "GROUP_INVITE_MISSING_FIELDS":
    case "GROUP_ROLE_UNSUPPORTED":
    case "GROUP_ALREADY_MEMBER":
    case "GROUP_INTERNAL_ERROR":
      return code;
    default:
      return "INVITE_INTERNAL_ERROR";
  }
}

export function mapInviteCommandError(err) {
  const code = String((err && err.code) || "").trim();
  switch (code) {
    case "INVITE_INVALID_FORMAT":
    case "INVITE_UNSUPPORTED_VERSION":
    case "INVITE_SIGNATURE_INVALID":
    case "INVITE_EXPIRED":
    case "INVITE_USED_UP":
    case "INVITE_REVOKED":
      return "INVITE_INVALID";
    case "GROUP_INVITE_MISSING_FIELDS":
    case "GROUP_ROLE_UNSUPPORTED":
    case "GROUP_ALREADY_MEMBER":
      return "BAD_REQUEST";
    case "INVITE_KIND_UNSUPPORTED":
      return "BAD_REQUEST";
    default:
      return "INTEGRITY_ERROR";
  }
}

export function normalizeFrameShape(frame) {
  if (!frame || typeof frame !== "object") return frame;
  const normalized = { ...frame };
  const type =
    typeof normalized.type === "string" && normalized.type.trim().length > 0
      ? normalized.type.trim()
      : typeof normalized.t === "string" && normalized.t.trim().length > 0
        ? normalized.t.trim()
        : "";
  if (type) {
    normalized.type = type;
    normalized.t = type;
  }
  return normalized;
}

/**
 * Resolves relay identity public key bytes from runtime.relayStore by relayKeyId.
 * Single place for "which field holds the key?" logic used by receipt verification etc.
 * @param {string} relayKeyId
 * @param {{ relayStore?: { getAll?: () => unknown[] } }} runtime
 * @returns {Uint8Array|null}
 */
export function lookupRelayPublicKey(relayKeyId, runtime) {
  const wanted = String(relayKeyId || "").trim();
  if (!wanted) return null;
  const relayStore = runtime && runtime.relayStore ? runtime.relayStore : null;
  const relays = relayStore && typeof relayStore.getAll === "function" ? relayStore.getAll() : [];
  for (const relay of relays) {
    const id = String((relay && relay.relayKeyId) || (relay && relay.id) || "").trim();
    if (!id || id !== wanted) continue;
    const descriptor = relay && relay.descriptor && typeof relay.descriptor === "object" ? relay.descriptor : null;
    const descriptorMeta = descriptor && descriptor.meta && typeof descriptor.meta === "object" ? descriptor.meta : null;
    const identityKey = descriptorMeta && descriptorMeta.identityKey && typeof descriptorMeta.identityKey === "object" ? descriptorMeta.identityKey : null;
    const candidates = [
      identityKey ? identityKey.publicKeyBytes : null,
      descriptorMeta ? descriptorMeta.identityPublicKey : null,
      descriptor ? descriptor.identityPublicKeyBytes : null,
      relay ? relay.identityPublicKeyBytes : null,
      relay ? relay.publicKeyBytes : null,
      relay ? relay.publicKey : null,
    ];
    for (const candidate of candidates) {
      if (candidate instanceof Uint8Array && candidate.length > 0) return candidate;
      if (Array.isArray(candidate) && candidate.length > 0) return new Uint8Array(candidate);
      if (typeof candidate === "string" && candidate.trim()) {
        try {
          return new Uint8Array(Buffer.from(candidate, "base64"));
        } catch {
          continue;
        }
      }
    }
  }
  return null;
}
