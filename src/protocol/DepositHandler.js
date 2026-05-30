import { createRelayDepositRouter } from "./RelayDepositRouter.js";

/**
 * Creates the inbound deposit handler. After Shape A, the node performs no
 * application-level processing on inbound deposits — every packet is emitted
 * as evt.mailbox.deposited and chat-server's ServerPeerLinkProtocolService
 * decides what to do with peer-link / E2EE bodies. Both the node-enabled and
 * relay-only modes use the same handler.
 *
 * @returns {Function} async onInboundDeposit({ inboxId, packetId, packetBytes, sessionRegistry, runtime })
 */
export function createDepositHandler() {
  const relayRouter = createRelayDepositRouter();
  return (opts) => relayRouter({ ...opts, nodeDepositProcessor: null });
}

export const createRelayOnlyDepositHandler = createDepositHandler;
