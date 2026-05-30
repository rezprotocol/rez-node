import { PerAccountServiceCache } from "../ws/PerAccountServiceCache.js";
import { createServerServices, createPerAccountServices } from "../services/createServerServices.js";

/**
 * Bootstrap node infrastructure: server services + service cache.
 * Called when node.enabled = true.
 *
 * After Shape A, the node holds no per-account crypto state — invite
 * authority, peer-link state, ratchet sessions all live on chat-server.
 * This factory only owns the keystore blob store (used by clients to
 * round-trip their keystore through the node's storage).
 *
 * Does NOT create gateway or protocol factory — caller handles those
 * because they depend on both relay and node components.
 *
 * @param {object} opts
 * @param {object} opts.resolved - Validated config from NodeConfigValidator
 * @param {object} opts.stableIdentity - Node identity from ensureNodeIdentity
 * @param {object} opts.storageProvider - FsStorageProvider instance
 * @returns {object} { serverServices, serviceCache }
 */
export async function bootstrapNodeInfrastructure({
  resolved,
  stableIdentity,
  storageProvider,
}) {
  const serverServicesFactory = resolved.node.serverServicesFactory || createServerServices;
  const serverServices = serverServicesFactory({
    storageProvider,
    ownerAccountId: stableIdentity.accountId,
  });

  const serviceCacheFactory = resolved.node.serviceCacheFactory || createPerAccountServices;
  const serviceCache = new PerAccountServiceCache({
    storageProvider,
    clock: () => Date.now(),
    backup: {
      retentionDays: resolved.backup.retentionDays,
    },
    createServices: serviceCacheFactory,
  });

  return { serverServices, serviceCache };
}
