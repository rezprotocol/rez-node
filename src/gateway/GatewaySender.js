import { TcpRelayTransport } from "../relay/TcpRelayTransport.js";

export class GatewaySender {
  constructor({ endpointId = "gateway", pool = null } = {}) {
    this.endpointId = endpointId;
    this.pool = pool;
  }

  async sendOnionPacket({ entryRelayKeyId, packetBytes } = {}) {
    if (typeof entryRelayKeyId !== "string" || !entryRelayKeyId.trim()) {
      throw new Error("GatewaySender.sendOnionPacket requires entryRelayKeyId");
    }
    if (!(packetBytes instanceof Uint8Array)) {
      throw new Error("GatewaySender.sendOnionPacket requires Uint8Array packetBytes");
    }
    if (this.pool) {
      await this.pool.sendByRelayId(entryRelayKeyId.trim(), packetBytes);
    } else {
      throw new Error("GatewaySender.sendOnionPacket requires pool for ID-based routing");
    }
  }

  /**
   * Forward a deposit to all connected relays.
   * Throws if pool is unavailable or all sends fail.
   * @returns {{ sent: number, failed: number, total: number }}
   */
  async forwardDepositToAllRelays(deliverInboxId, innerBytes) {
    if (!this.pool) {
      throw new Error("GatewaySender.forwardDepositToAllRelays: no relay connection pool");
    }
    return this.pool.sendDepositToAllConnections(deliverInboxId, innerBytes);
  }
}
