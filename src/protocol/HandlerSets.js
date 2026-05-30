import { REZ_CONTRACT_TYPES } from "@rezprotocol/core";

const T = REZ_CONTRACT_TYPES;

/**
 * Relay-level handler types: work without node services.
 * Need only inboxStore, capability middleware.
 */
export const RELAY_HANDLER_TYPES = Object.freeze([
  T.MAILBOX_DEPOSIT,
  T.MAILBOX_LIST,
  T.MAILBOX_FETCH,
  T.MAILBOX_ACK,
  T.INBOX_CLAIM,
  T.CHANNEL_OPEN,
  T.CHANNEL_CLOSE,
]);

/**
 * Node-level handler types: require serverServices, serviceCache, peer-link.
 */
export const NODE_HANDLER_TYPES = Object.freeze([
  T.NODE_STATUS,
]);
