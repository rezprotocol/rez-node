import { randomInt } from "node:crypto";
import { descriptorHasUsableOnionKey, isNonEmptyString } from "@rezprotocol/core";

export class NotEnoughRelaysError extends Error {
  constructor(message = "Not enough relays") {
    super(message);
    this.name = "NotEnoughRelaysError";
  }
}

function hasTcpEndpoint(descriptor) {
  return Array.isArray(descriptor.endpoints)
    && descriptor.endpoints.some((ep) => ep && isNonEmptyString(ep.host) && Number.isInteger(ep.port));
}

export class GatewayRelaySelector {
  constructor({ rng } = {}) {
    this.rng = rng || ((max) => randomInt(max));
  }

  select({ descriptors, minHops = 1, maxHops = 3, excludeRelayKeyIds = [], requireTcpEndpoint = true, nowMs } = {}) {
    if (!Array.isArray(descriptors)) {
      throw new Error("GatewayRelaySelector.select requires descriptors[]");
    }

    const exclude = new Set(excludeRelayKeyIds || []);
    const now = Number.isFinite(Number(nowMs)) ? Number(nowMs) : Date.now();
    const eligible = descriptors.filter((desc) => {
      if (!desc || !isNonEmptyString(desc.relayKeyId)) return false;
      if (exclude.has(desc.relayKeyId)) return false;
      if (requireTcpEndpoint && !hasTcpEndpoint(desc)) return false;
      if (!Array.isArray(desc.onionKeys) || desc.onionKeys.length === 0) return false;
      if (!descriptorHasUsableOnionKey(desc, now)) return false;
      return true;
    });

    const hops = Math.max(minHops, Math.min(maxHops, 3));
    if (hops === 0) return [];
    if (eligible.length === 0) {
      throw new NotEnoughRelaysError("No eligible relays available");
    }
    // Select up to `hops` relays, using whatever is available
    const actualHops = Math.min(hops, eligible.length);

    const selected = [];
    const pool = [...eligible];
    for (let i = 0; i < actualHops; i += 1) {
      const idx = this.rng(pool.length);
      selected.push(pool.splice(idx, 1)[0]);
    }

    return selected;
  }
}
