import { GatewaySession } from "./GatewaySession.js";

/**
 * Creates a protocol factory for the gateway session protocol.
 *
 * The returned function is injected into WsGatewayServer and called
 * once per WebSocket connection to create the protocol handler.
 *
 * @param {object} [options]
 * @param {boolean} [options.nodeEnabled=true] - Whether node-level handlers are enabled
 * @returns {Function} ({ runtime, ws, request, sessionRegistry }) => GatewaySession
 */
export function createProtocolFactory({ nodeEnabled = true } = {}) {
  return function protocolFactory({ runtime, ws, request, sessionRegistry }) {
    return new GatewaySession({ runtime, ws, request, sessionRegistry, nodeEnabled });
  };
}
