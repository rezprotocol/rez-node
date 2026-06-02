import { peerIpKey } from "../../util/peerIpKey.js";

/**
 * Shared peer rate-limit keying for DHT control-message stores. Used by both
 * the route DHT (DhtProtocol) and the durable-record DHT
 * (DurableRecordProtocol) so the two never drift — the keying is
 * security-relevant (SECURITY_AUDIT LOW-6 per-peer + MED-13 per-IP caps).
 */

/**
 * Resolve a socket to a per-peer rate-limit key. Production wires a
 * `getPeerKey` callback returning the peer's `relayKeyId` (stable across
 * socket reconnects — denying an attacker the free reset a socket-keyed
 * limiter would give). Falls back to the socket's own `id`, or null for
 * synthetic sockets (which skips the gate).
 *
 * @param {object} socket
 * @param {((socket: object) => string|null)|null} getPeerKey
 * @returns {string|null}
 */
export function peerRateLimitKey(socket, getPeerKey) {
  if (!socket) return null;
  if (getPeerKey) {
    const key = getPeerKey(socket);
    if (typeof key === "string" && key.length > 0) return key;
  }
  if (typeof socket.id === "string" && socket.id.length > 0) return "socket:" + socket.id;
  return null;
}

/**
 * Resolve a socket to a /64-aggregated per-IP rate-limit key (the outer cap
 * above the per-relayKeyId limiter — SECURITY_AUDIT MED-13/14). Falls back to
 * the raw `socket.remoteAddress`. Empty result skips the IP gate.
 *
 * @param {object} socket
 * @param {((socket: object) => string|null)|null} getPeerIp
 * @returns {string|null}
 */
export function peerRateLimitIpKey(socket, getPeerIp) {
  if (!socket) return null;
  if (getPeerIp) {
    const key = getPeerIp(socket);
    if (typeof key === "string" && key.length > 0) return key;
  }
  const raw = typeof socket.remoteAddress === "string" ? socket.remoteAddress : "";
  const key = peerIpKey(raw);
  return key.length > 0 ? key : null;
}
