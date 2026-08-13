import { assertMultiDeviceFanoutReady } from "./deviceFanoutReadiness.js";

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
  durableInbox = null,
  accountDeviceRegistry = null,
  accountMutationSerializer = null,
  accountAuthorityRevocationCache = null,
  accountDeviceBundleStore = null,
  multiDeviceFanout = false,
  isHostedHere = null,
} = {}) {
  // The release-blocker interlock (audit R4 L2c review P1). This factory is a
  // package-root export: an embedding app could otherwise pass multiDeviceFanout:true
  // and advertise fan-out while the F2/F3 blockers are unbuilt, bypassing
  // validateConfig. Consult the ONE readiness SSOT here too — fail loud, never a
  // silent open. A false request (the default) is a no-op.
  const fanoutReady = assertMultiDeviceFanoutReady(multiDeviceFanout === true);
  const effectiveMultiDeviceFanout = multiDeviceFanout === true && fanoutReady;
  const stableIdentity = identity;
  // Audit leaf-3b F5: DERIVE the propagation outbox from the serializer rather than accepting it
  // as an independent argument — an embedder cannot then wire the wire-lease handler to a
  // different database than the one the mutation fold enqueues into (no split-brain outbox).
  const propagationOutbox = accountMutationSerializer && typeof accountMutationSerializer.propagationOutbox !== "undefined"
    ? accountMutationSerializer.propagationOutbox
    : null;
  // F3 (audit leaf-3c): the cluster-wide per-account request budget. DERIVED from the same
  // connection the outbox uses — like propagationOutbox above, an embedder cannot point the budget
  // at a different database than the one it is supposed to be bounding. Null on fs/desktop, where
  // there is no cluster to spread requests across and the per-node limiter is the whole story.
  const rateBudget = propagationOutbox && typeof propagationOutbox.rateBudget !== "undefined"
    ? propagationOutbox.rateBudget
    : null;
  return {
    relayStore,
    inboxStore,
    metrics,
    inboxRouter,
    gatewayLoop,
    inboxClaimRegistry,
    depositPolicyStore,
    depositRateLimitStore,
    // Durable home log (pg cluster) — null on fs/desktop so the cursor model and
    // the durableInbox session capability stay off (legacy delete-ack path).
    durableInbox,
    // S2.5 S11: canonical device set + serialized mutation authority (pg cluster
    // only). Null on fs/desktop ⇒ the mutation/authority-state handlers answer
    // SERVICE_UNAVAILABLE and revocationState stays null (byte-identical).
    accountDeviceRegistry,
    accountMutationSerializer,
    // P1#3 leaf 3b: the shared authority-state propagation outbox, read by
    // PropagationOutboxHandler for the lease lifecycle (claim/prepare/release/fail).
    // Null on fs/desktop ⇒ that handler answers SERVICE_UNAVAILABLE.
    propagationOutbox,
    // F3: cluster-wide per-account rate budget, read by PropagationOutboxHandler. Null ⇒ only the
    // per-node limiter applies (single-node / fs deployments).
    rateBudget,
    // Always-fresh reader over the home authority-state (revoked certs + issued-at
    // cutoff + epoch + terminal device status). Feeds the session-auth revocationState
    // and the per-dispatch L5 guard; null ⇒ byte-identical path. (No caching — audit R4
    // L5 review removed the TTL warm cache; the name is retained as the wiring key.)
    accountAuthorityRevocationCache,
    // S2.5 S12: home-aggregated per-device prekey bundle store (multi-device
    // fan-out). Null ⇒ bundle/device-set handlers answer SERVICE_UNAVAILABLE.
    accountDeviceBundleStore,
    // E6 gate state (maxDevices > 1). Advertised in session.ready so the client
    // knows a proven device.bind is required for a cursor (Audit R2 #6). False on
    // fs/desktop and gate-closed pg nodes; the readiness interlock above guarantees
    // this can only be true when every release blocker is met.
    multiDeviceFanout: effectiveMultiDeviceFanout,
    // Predicate: is this inbox's durable home this cluster? (async Pg claim
    // lookup). Used by MailboxHandler.handleList to choose the durable branch.
    isHostedHere: typeof isHostedHere === "function" ? isHostedHere : null,
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
      // A durable home remains reachable while its browser is offline. The
      // signed registration expires on its own and is refreshed at reconnect;
      // socket teardown is not an administrative unhosting operation.
      if (durableInbox) return Promise.resolve();
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
