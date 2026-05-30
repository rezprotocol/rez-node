import {
  OnionLayerAeadV2,
  OnionPacketV2,
  Header,
  Envelope,
  JsonCodec,
  ONION_V2_SIZE_CLASSES,
} from "@rezprotocol/core";

const encoder = new TextEncoder();

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === "object") {
    const out = {};
    const keys = Object.keys(value).sort();
    for (const key of keys) {
      out[key] = canonicalize(value[key]);
    }
    return out;
  }
  return value;
}

function canonicalStringify(value) {
  return JSON.stringify(canonicalize(value));
}

function encodeJsonBytes(obj) {
  return encoder.encode(canonicalStringify(obj));
}

function bytesToBase64(bytes) {
  return Buffer.from(bytes).toString("base64");
}

function selectSizeClass(blobBytes) {
  const sizeClass = ONION_V2_SIZE_CLASSES.find((s) => s >= blobBytes.length);
  if (!sizeClass) {
    throw new Error("buildOnionPacketV2 blob exceeds max size class (" + blobBytes.length + " bytes)");
  }
  return sizeClass;
}

function padToSize(bytes, size) {
  const payload = new Uint8Array(size);
  payload.set(bytes, 0);
  return payload;
}

export async function buildOnionPacketV2({
  crypto,
  innerBytes,
  deliverInboxId,
  receiptInboxId,
  returnPath = null,
  pathEntries,
  finalRelayKeyId,
  ttl,
  nowMs = Date.now(),
} = {}) {
  if (!(innerBytes instanceof Uint8Array)) {
    throw new Error("buildOnionPacketV2 requires innerBytes Uint8Array");
  }
  if (!Array.isArray(pathEntries) || pathEntries.length === 0) {
    throw new Error("buildOnionPacketV2 requires pathEntries[]");
  }
  if (typeof finalRelayKeyId !== "string" || finalRelayKeyId.length === 0) {
    throw new Error("buildOnionPacketV2 requires finalRelayKeyId");
  }
  if (typeof deliverInboxId !== "string" || deliverInboxId.length === 0) {
    throw new Error("buildOnionPacketV2 requires deliverInboxId");
  }

  const layer = new OnionLayerAeadV2({ crypto });
  const totalTtl = Number.isInteger(ttl) ? ttl : pathEntries.length + 2;

  let blob = innerBytes;
  for (let i = pathEntries.length - 1; i >= 0; i -= 1) {
    const hop = pathEntries[i];
    const hopTtl = Math.max(0, totalTtl - i);
    const next = (i + 1 < pathEntries.length)
      ? { relayKeyId: pathEntries[i + 1].relayKeyId }
      : { relayKeyId: finalRelayKeyId };

    const descriptor = hop.relayDescriptor;
    if (!descriptor) {
      throw new Error("buildOnionPacketV2 requires relayDescriptor in pathEntries");
    }
    const onionKeyId = hop.onionKeyId || descriptor.onionKeys[0]?.onionKeyId;
    const onionPubKeyBytes = hop.onionPubKeyBytes || descriptor.onionKeys[0]?.publicKeyBytes;
    if (!onionKeyId || !onionPubKeyBytes) {
      throw new Error("buildOnionPacketV2 requires onionKeyId and onionPubKeyBytes");
    }

    const layerPlain = {
      v: 2,
      ttl: hopTtl,
      next,
      flags: { dropOnFail: true },
      inner: bytesToBase64(blob),
      ...(i === pathEntries.length - 1 ? { deliver: { inboxId: deliverInboxId } } : {}),
      ...(i === pathEntries.length - 1 && receiptInboxId ? { receipt: { inboxId: receiptInboxId } } : {}),
      ...(i === pathEntries.length - 1 && returnPath && typeof returnPath === "object" ? { returnPath } : {}),
    };

    const plaintextBytes = encodeJsonBytes(layerPlain);
    const encrypted = await layer.encryptLayerV2({
      relayPubKeyBytes: onionPubKeyBytes,
      plaintextBytes,
      hopIndex: i,
      ttl: hopTtl,
      onionKeyId,
    });

    const cipherObj = {
      v: 2,
      hopIndex: i,
      onionKeyId: encrypted.onionKeyId,
      ttl: hopTtl,
      ephPub: bytesToBase64(encrypted.ephPub),
      ct: bytesToBase64(encrypted.ct),
    };

    blob = encodeJsonBytes(cipherObj);
  }

  const sizeClass = selectSizeClass(blob);
  const payload = padToSize(blob, sizeClass);
  const packet = new OnionPacketV2({ v: 2, sizeClass, payload });

  const header = new Header({ id: `onion-${nowMs}`, type: "rez.onion.v2", createdAt: nowMs });
  const envelope = new Envelope({ header, body: packet.toJSON() });
  const codec = new JsonCodec();
  const ctx = await codec.encode({ envelope });

  return { packetBytes: ctx.bytes, envelope };
}
