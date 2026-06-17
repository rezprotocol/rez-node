import { decodeOuterPacket } from "@rezprotocol/core";
import { buildMailboxDepositedFrame } from "./mailboxDepositedFrame.js";

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
  return async function onInboundDeposit({ inboxId, packetId, packetBytes, sessionRegistry, runtime, nodeDepositProcessor, seq = null } = {}) {
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

      // Option Y delivery gate — ONE authoritative signal, mutually exclusive.
      // `hasLocalLiveSocket` is the set of owners with a live socket HERE whose
      // localInboxId is this inbox: it is exactly what determines whether a local
      // broadcast would deliver, so the same value decides broadcast-vs-publish.
      //   live socket here  -> direct broadcast (no Redis round-trip).
      //   none here         -> the socket (if any) is on another node: publish a
      //                        liveness ping; that node drains from the durable log.
      // (Single device, D3: exactly one socket, so these can never both apply.)
      const hasLocalLiveSocket = typeof sessionRegistry.getOwnerPublicKeysByInboxId === "function"
        && sessionRegistry.getOwnerPublicKeysByInboxId(inboxId).size > 0;

      if (hasLocalLiveSocket) {
        emitMailboxDeposited(sessionRegistry, owners, {
          mailboxId: inboxId,
          eventId: packetId,
          ciphertextB64: packetB64,
          seq,
        });
      } else if (seq != null) {
        // Durable deposit with no local socket: ping the bus so a remote holder
        // drains. No bus configured ⇒ nothing live; reconnect-drain delivers.
        const bus = runtime && runtime.livenessBus;
        if (bus && typeof bus.publishDeposit === "function") {
          try {
            await bus.publishDeposit(inboxId, { seq });
          } catch (err) {
            // The row is already durable; a failed ping only delays real-time
            // delivery until the next deposit or reconnect. Log, don't throw.
            console.error("[RelayDepositRouter] liveness publish failed for " + inboxId
              + ": " + (err && err.message ? err.message : err));
          }
        }
      }
      return;
    }

    // Unrecognized payload — drop silently.
  };
}

// --- Generic mailbox deposit notification ---

function emitMailboxDeposited(sessionRegistry, owners, { mailboxId, eventId, ciphertextB64, seq = null }) {
  if (!sessionRegistry || typeof sessionRegistry.broadcastToOwner !== "function") {
    return;
  }
  const frame = buildMailboxDepositedFrame({ mailboxId, eventId, ciphertextB64, seq });
  for (const ownerPublicKeyB64 of owners) {
    sessionRegistry.broadcastToOwner(ownerPublicKeyB64, frame);
  }
}
