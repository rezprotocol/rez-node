import { createRelayRuntime } from "./createRelayRuntime.js";

/**
 * Builds the full node runtime by extending relay runtime with node-level services.
 *
 * Node-level additions: serverServices, serviceCache, storageProvider,
 * groupLookupClass, deliveryReceiptIndex.
 *
 * @param {object} opts - All options from createRelayRuntime plus node-level options
 * @param {object} opts.serverServices
 * @param {object} opts.serviceCache
 * @param {object} opts.storageProvider
 * @param {object} [opts.groupLookupClass]
 * @returns {object} full node runtime
 */
export function createNodeRuntime({
  relayStore,
  inboxStore,
  identity,
  serverServices,
  serviceCache,
  storageProvider,
  metrics,
  meshCoordinator = null,
  meshConfig = null,
  gatewayLoop = null,
  groupLookupClass = null,
  inboxRouter = null,
  hostedInboxRegistry = null,
  inboxClaimRegistry = null,
  depositPolicyStore = null,
  depositRateLimitStore = null,
  durableInbox = null,
  accountDeviceRegistry = null,
  accountMutationSerializer = null,
  accountAuthorityRevocationCache = null,
  multiDeviceFanout = false,
  isHostedHere = null,
} = {}) {
  const relay = createRelayRuntime({
    relayStore,
    inboxStore,
    identity,
    metrics,
    meshCoordinator,
    meshConfig,
    gatewayLoop,
    inboxRouter,
    hostedInboxRegistry,
    inboxClaimRegistry,
    depositPolicyStore,
    depositRateLimitStore,
    durableInbox,
    accountDeviceRegistry,
    accountMutationSerializer,
    accountAuthorityRevocationCache,
    multiDeviceFanout,
    isHostedHere,
  });

  return {
    ...relay,
    serverServices,
    serviceCache,
    storageProvider,
    groupLookupClass,
    deliveryReceiptIndex: new Map(),
  };
}
