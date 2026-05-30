import { selectOnionKeyForSendV1 } from "@rezprotocol/core";

export class GatewayPathPlanner {
  constructor({ nowMs = () => Date.now() } = {}) {
    this.nowMs = nowMs;
  }

  plan({ descriptors } = {}) {
    if (!Array.isArray(descriptors) || descriptors.length === 0) {
      throw new Error("GatewayPathPlanner.plan requires descriptors[]");
    }

    const nowMs = this.nowMs();
    const hops = [];
    const pathEntries = [];

    for (const descriptor of descriptors) {
      const selected = selectOnionKeyForSendV1(descriptor.onionKeys, nowMs);

      hops.push({
        relayKeyId: descriptor.relayKeyId,
        onionKeyId: selected.onionKeyId,
        onionKeyFormat: selected.format,
      });

      pathEntries.push({ relayKeyId: descriptor.relayKeyId, relayDescriptor: descriptor });
    }

    return { hops, pathEntries };
  }
}
