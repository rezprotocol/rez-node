/**
 * Builds relay-level runtime. Contains only what relay handlers need:
 * relayStore, inboxStore, identity, metrics, mesh status, inbox resolution.
 *
 * Does NOT include: serverServices, serviceCache, storageProvider,
 * groupLookupClass, deliveryReceiptIndex, pendingClaims — those are node-level.
 *
 * Routing-side identity is the inbox claimant's pubkey. The relay knows
 * nothing about accounts; per the cap model an inbox's trust root is the
 * pubkey that claimed it (see docs/CAPABILITY_MODEL.md).
 *
 * @param {object} opts
 * @param {object} opts.relayStore
 * @param {object} opts.inboxStore
 * @param {object} opts.identity - Must already be resolved (e.g. from ensureNodeIdentity)
 * @param {object} [opts.metrics]
 * @param {object} [opts.meshCoordinator]
 * @param {object} [opts.meshConfig]
 * @param {object} [opts.gatewayLoop]
 * @param {object} [opts.inboxRouter]
 * @param {object} [opts.hostedInboxRegistry]
 * @returns {object} relay runtime
 */
export function createRelayRuntime({
  relayStore,
  inboxStore,
  identity,
  metrics,
  meshCoordinator = null,
  meshConfig = null,
  gatewayLoop = null,
  inboxRouter = null,
  hostedInboxRegistry = null,
  inboxClaimRegistry = null,
  depositPolicyStore = null,
  depositRateLimitStore = null,
} = {}) {
  const stableIdentity = identity;
  return {
    relayStore,
    inboxStore,
    metrics,
    inboxRouter,
    gatewayLoop,
    inboxClaimRegistry,
    depositPolicyStore,
    depositRateLimitStore,
    getIdentity() {
      return { ...stableIdentity };
    },
    getOwnerPublicKeysForInbox(inboxId) {
      if (hostedInboxRegistry && typeof hostedInboxRegistry.getOwnerPublicKeysForInbox === "function") {
        return hostedInboxRegistry.getOwnerPublicKeysForInbox(inboxId);
      }
      return new Set();
    },
    registerHostedSession(claimantPublicKeyB64, registration) {
      if (hostedInboxRegistry && typeof hostedInboxRegistry.add === "function") {
        return hostedInboxRegistry.add(claimantPublicKeyB64, registration);
      }
      return Promise.resolve();
    },
    unregisterHostedSession(claimantPublicKeyB64) {
      if (hostedInboxRegistry && typeof hostedInboxRegistry.remove === "function") {
        return hostedInboxRegistry.remove(claimantPublicKeyB64);
      }
      return undefined;
    },
    getMeshStatus() {
      if (meshCoordinator && typeof meshCoordinator.getStatus === "function") {
        return meshCoordinator.getStatus();
      }
      const mode = meshConfig && meshConfig.mode ? meshConfig.mode : "seeded-gossip";
      const policy = meshConfig && meshConfig.policy ? meshConfig.policy : null;
      return {
        enabled: true,
        mode,
        participateInRouting: true,
        peerCount: 0,
        seedReachable: {},
        lastDiscoveryAtMs: null,
        routeStats: { evicted: 0 },
        policy,
        peers: [],
      };
    },
    async refreshMesh() {
      if (meshCoordinator && typeof meshCoordinator.refresh === "function") {
        await meshCoordinator.refresh();
      }
      return this.getMeshStatus();
    },
    onMeshStatusChanged(handler) {
      if (!meshCoordinator || typeof meshCoordinator.onStatusChanged !== "function") {
        return () => {};
      }
      return meshCoordinator.onStatusChanged(handler);
    },
    async stop() {
      if (meshCoordinator) {
        await meshCoordinator.stop();
      }
    },
  };
}
