export class NodeDeliveryAdapter {
  constructor({
    inboxStore = null,
    localInboxId = "",
    gatewayLoop = null,
    routingEngine = null,
    routingPolicy = null,
  } = {}) {
    this.inboxStore = inboxStore;
    this.localInboxId = String(localInboxId || "").trim();
    this.gatewayLoop = gatewayLoop;
    this.routingEngine = routingEngine;
    this.routingPolicy = routingPolicy && typeof routingPolicy === "object" ? routingPolicy : {};
  }

  async sendPacket({ targetHandle, packetBytes, receiptInboxId = null } = {}) {
    const target = String(targetHandle || "").trim();
    if (!target) throw new Error("delivery target is required");
    if (!(packetBytes instanceof Uint8Array)) {
      throw new Error("delivery packetBytes must be Uint8Array");
    }

    const remoteInboxTarget = isRemoteInboxTarget({
      targetHandle: target,
      localInboxId: this.localInboxId,
    });

    if (!remoteInboxTarget) {
      if (!this.inboxStore || typeof this.inboxStore.deposit !== "function") {
        throw new Error("inbox store unavailable");
      }
      const packetId = await this.inboxStore.depositFromWire(target, packetBytes);
      return { packetId, status: "delivered", remote: false };
    }

    if (this.gatewayLoop && typeof this.gatewayLoop.sendToInbox === "function") {
      const policy = this.routingPolicy;
      const hops = Number(policy ? policy.hops : undefined);
      const minHops = Number(policy ? policy.minHops : undefined);
      const maxHops = Number(policy ? policy.maxHops : undefined);
      await this.gatewayLoop.sendToInbox({
        innerBytes: packetBytes,
        deliverInboxId: target,
        receiptInboxId: receiptInboxId || undefined,
        ...(Number.isFinite(hops) ? { hops: Math.max(1, Math.floor(hops)) } : {}),
        ...(Number.isFinite(minHops) ? { minHops: Math.max(1, Math.floor(minHops)) } : {}),
        ...(Number.isFinite(maxHops) ? { maxHops: Math.max(1, Math.floor(maxHops)) } : {}),
      });
      return { packetId: null, status: "sent", remote: true };
    }

    if (this.routingEngine && typeof this.routingEngine.routePayload === "function") {
      const routed = await this.routingEngine.routePayload({
        targetHandle: target,
        payloadBytes: packetBytes,
      });
      const mode = routed && typeof routed.mode === "string" ? routed.mode : "";
      if (mode === "local" || mode === "local-deliver") {
        return { packetId: (routed && routed.packetId) || null, status: "delivered", remote: false };
      }
      if (mode && mode !== "unresolved" && mode !== "dropped-ttl") {
        return { packetId: (routed && routed.packetId) || null, status: "sent", remote: true };
      }
    }

    throw new Error("Thread binding target is hosted on another node");
  }
}

function isRemoteInboxTarget({ targetHandle, localInboxId } = {}) {
  const target = String(targetHandle || "").trim();
  const local = String(localInboxId || "").trim();
  if (!target.startsWith("inbox:")) return false;
  if (!local) return false;
  return target !== local;
}
