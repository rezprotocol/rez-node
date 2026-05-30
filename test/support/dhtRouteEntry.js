import { bytesToBase64, canonicalJSONStringify } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../../src/crypto/NodeCryptoProvider.js";

const CRYPTO = new NodeCryptoProvider();

/**
 * Build a DHT-storable routeEntry with a valid claimant-signed
 * `registration` for tests. Matches the production envelope produced by
 * `InboxRouter._createAnnouncedRouteEntry` for direct routes.
 *
 * Production `nodeKeyId` (crypto identity) and `relayKeyId` (routing-layer
 * address) are distinct identifier namespaces. The claimant signs both so
 * the delegation binds the inbox to a specific (node, relay) pair end-to-
 * end — see docs/SECURITY_AUDIT.md HIGH-8.
 *
 * @param {object} opts
 * @param {string} opts.inboxId
 * @param {string} opts.deliveryRelayKeyId — also used as `registration.relayKeyId`
 * @param {string} [opts.nodeKeyId] — defaults to a synthetic `nodekey:` derived from deliveryRelayKeyId so tests exercise the distinct-namespace path
 * @param {string} [opts.nodePublicKeyB64] — defaults to deliveryRelayKeyId
 * @param {number} [opts.hops] — defaults to 0
 * @param {number} [opts.ttlMs] — defaults to 60s
 * @returns {{ routeEntry: object, claimantPublicKeyB64: string }}
 */
export function makeSignedRouteEntry({ inboxId, deliveryRelayKeyId, nodeKeyId, nodePublicKeyB64, hops = 0, ttlMs = 60_000, issuedAtMs: issuedAtMsOverride } = {}) {
  if (!inboxId) throw new Error("makeSignedRouteEntry requires inboxId");
  if (!deliveryRelayKeyId) throw new Error("makeSignedRouteEntry requires deliveryRelayKeyId");
  const claimantKp = CRYPTO.generateSigningKeyPair();
  const claimantPublicKeyB64 = bytesToBase64(claimantKp.publicKey);
  const issuedAtMs = typeof issuedAtMsOverride === "number" ? issuedAtMsOverride : Date.now();
  const expiresAtMs = issuedAtMs + ttlMs;
  const resolvedNodeKeyId = nodeKeyId || ("nodekey:" + deliveryRelayKeyId);
  const nodePub = nodePublicKeyB64 || deliveryRelayKeyId;
  const payload = {
    kind: "inbox-node-delegation",
    inboxId,
    claimantPublicKeyB64,
    nodeKeyId: resolvedNodeKeyId,
    nodePublicKeyB64: nodePub,
    relayKeyId: deliveryRelayKeyId,
    issuedAtMs,
    expiresAtMs,
  };
  const msg = new TextEncoder().encode(canonicalJSONStringify(payload));
  const sig = CRYPTO.sign({ privateKey: claimantKp.privateKey, msg });
  const routeEntry = {
    inboxId,
    deliveryRelayKeyId,
    relayKeyId: deliveryRelayKeyId,
    nextHopRelayKeyId: deliveryRelayKeyId,
    hops,
    registration: {
      inboxId,
      claimantPublicKeyB64,
      nodeKeyId: resolvedNodeKeyId,
      nodePublicKeyB64: nodePub,
      relayKeyId: deliveryRelayKeyId,
      issuedAtMs,
      expiresAtMs,
      delegationSigB64: bytesToBase64(sig),
    },
  };
  return { routeEntry, claimantPublicKeyB64 };
}
