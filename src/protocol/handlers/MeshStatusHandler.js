import { REZ_CONTRACT_TYPES } from "@rezprotocol/core";

const T = REZ_CONTRACT_TYPES;

export class MeshStatusHandler {
  #ctx;

  constructor(ctx) {
    this.#ctx = ctx;
  }

  handleMeshStatus(requestId) {
    const identity = typeof this.#ctx.runtime.getIdentity === "function" ? this.#ctx.runtime.getIdentity() : null;
    const mesh = typeof this.#ctx.runtime.getMeshStatus === "function" ? this.#ctx.runtime.getMeshStatus() : null;
    // Per the multi-tenant model, the node identifies itself by nodeKeyId
    // (its mesh keypair fingerprint). accountId / deviceId / localInboxId
    // were leftovers from the conflated single-tenant era and should not
    // appear in node-status responses — they would expose either nothing
    // meaningful (no one user owns the node) or the wrong thing (the node's
    // single tenant under the old model).
    this.#ctx.sendRawRecord(T.NODE_STATUS_RES, {
      id: requestId,
      body: {
        node: {
          nodeKeyId: identity && identity.nodeKeyId ? identity.nodeKeyId : null,
          nodePublicKeyB64: identity && identity.nodePublicKeyB64 ? identity.nodePublicKeyB64 : null,
        },
        mesh: mesh
          ? {
              enabled: mesh.enabled === true,
              mode: mesh.mode || "seeded-gossip",
              participateInRouting: mesh.participateInRouting === true,
              peerCount: Number(mesh.peerCount || 0),
              seedReachable: mesh.seedReachable || {},
              lastDiscoveryAtMs: mesh.lastDiscoveryAtMs || null,
              routeStats: mesh.routeStats || null,
              policy: mesh.policy || null,
            }
          : null,
        peers: Array.isArray(mesh?.peers) ? mesh.peers : [],
      },
    });
  }
}
