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

      if (seq != null) {
        // DURABLE deposit — ONE delivery mechanism, the per-session drain, gated
        // on ONE signal (sessions bound to this inbox HERE). Local-vs-cross-node
        // is mutually exclusive and the drain is identical either way, so the
        // local fast path advances `last_delivered` exactly like the cross-node
        // path (a same-node live push must NOT leave cursorAck clamped to 0).
        //   socket here -> drain it IN-PROCESS (no Redis round-trip, Option Y).
        //   none here   -> the socket is on another node: ping the bus; that node
        //                  drains. No bus ⇒ nothing live; reconnect-drain delivers.
        // Direct per-session send (inside the drain) also means a claimed inbox's
        // mail never fans out to an unrelated session under the same auth owner.
        let deliveredLocally = 0;
        if (typeof sessionRegistry.forEachSessionByInboxId === "function") {
          sessionRegistry.forEachSessionByInboxId(inboxId, (session) => {
            if (typeof session.notifyLocalDeposit === "function") {
              session.notifyLocalDeposit(inboxId, seq);
              deliveredLocally += 1;
            }
          });
        }
        if (deliveredLocally === 0) {
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
      } else {
        // TRANSIENT deposit (no durable seq — RMailbox / non-durable node): the
        // generic EVT broadcast. There is no per-device watermark to advance.
        emitMailboxDeposited(sessionRegistry, owners, {
          mailboxId: inboxId,
          eventId: packetId,
          ciphertextB64: packetB64,
          seq: null,
        });
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
