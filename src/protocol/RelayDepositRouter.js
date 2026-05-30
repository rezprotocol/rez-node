import { randomUUID } from "node:crypto";
import { decodeOuterPacket, CONTRACT_VERSION, REZ_CONTRACT_TYPES } from "@rezprotocol/core";

const T = REZ_CONTRACT_TYPES;

/**
 * Relay-level deposit processing: outer packet decode + evt.mailbox.deposited.
 * Pure RCGP — knows nothing about peer-links, invites, or app services.
 *
 * When a nodeDepositProcessor is provided (via opts), it gets first crack at
 * handling decoded packets. If the processor returns true, the packet is
 * considered handled (peer-link handshake, invite claim, etc.). Otherwise
 * the relay emits a generic evt.mailbox.deposited to session owners.
 *
 * @returns {Function} async onInboundDeposit({ inboxId, packetId, packetBytes, sessionRegistry, runtime, nodeDepositProcessor })
 */
export function createRelayDepositRouter() {
  return async function onInboundDeposit({ inboxId, packetId, packetBytes, sessionRegistry, runtime, nodeDepositProcessor } = {}) {
    // Skip thread-prefixed inbox IDs (legacy)
    if (typeof inboxId === "string" && inboxId.startsWith("th_")) {
      return;
    }

    let owners = typeof runtime.getOwnerPublicKeysForInbox === "function"
      ? runtime.getOwnerPublicKeysForInbox(inboxId)
      : new Set();
    if (!(owners instanceof Set) || owners.size === 0) {
      owners = typeof sessionRegistry.getOwnerPublicKeysByInboxId === "function"
        ? sessionRegistry.getOwnerPublicKeysByInboxId(inboxId)
        : new Set();
    }

    let packet = null;
    try {
      packet = decodeOuterPacket(packetBytes);
    } catch {
      packet = null;
    }

    if (packet) {
      const packetB64 = Buffer.from(packet.bodyBytesView).toString("base64");

      // Let node-level processor handle app-specific packets (handshakes, claims)
      if (typeof nodeDepositProcessor === "function") {
        const handled = await nodeDepositProcessor({
          owners,
          sessionRegistry,
          runtime,
          inboxId,
          packetId,
          packetBytes,
          packet,
          packetB64,
        });
        if (handled) return;
      }

      // Generic deposit: emit evt.mailbox.deposited
      emitMailboxDeposited(sessionRegistry, owners, {
        mailboxId: inboxId,
        eventId: packetId,
        ciphertextB64: packetB64,
      });
      return;
    }

    // Unrecognized payload — drop silently.
  };
}

// --- Generic mailbox deposit notification ---

function emitMailboxDeposited(sessionRegistry, owners, { mailboxId, eventId, ciphertextB64 }) {
  if (!sessionRegistry || typeof sessionRegistry.broadcastToOwner !== "function") {
    return;
  }
  const frame = {
    id: `${T.EVT_MAILBOX_DEPOSITED}:${Date.now()}:${randomUUID()}`,
    t: T.EVT_MAILBOX_DEPOSITED,
    v: CONTRACT_VERSION,
    body: {
      mailboxId,
      eventId,
      ciphertextB64: ciphertextB64 || null,
    },
  };
  for (const ownerPublicKeyB64 of owners) {
    sessionRegistry.broadcastToOwner(ownerPublicKeyB64, frame);
  }
}
