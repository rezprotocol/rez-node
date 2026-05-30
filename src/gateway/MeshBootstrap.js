import { NodeCryptoProvider } from "../crypto/NodeCryptoProvider.js";
import { GatewayLoop } from "./GatewayLoop.js";
import { GatewayPathPlanner } from "./GatewayPathPlanner.js";
import { GatewayRelaySelector } from "./GatewayRelaySelector.js";
import { GatewaySender } from "./GatewaySender.js";
import { MeshCoordinator } from "./MeshCoordinator.js";
import { PersistentOutboundQueue } from "./PersistentOutboundQueue.js";
import { RetryScheduler } from "./RetryScheduler.js";

/**
 * Constructs mesh coordinator, gateway loop, persistent outbound queue,
 * and retry scheduler for any node that has the required runtime primitives.
 *
 * Does not start anything; caller is responsible for:
 *   - meshCoordinator.start()
 *   - outboundQueue.loadAll() (async — load persisted entries)
 *   - retryScheduler.start()
 *
 * @param {{ meshConfig: object, relayStore: object, metrics: object, identity: { accountId: string }, relayConnectionPool: object | null, inboxRouter: object | null, inboxStore: object, keyValueStore: object | null }} opts
 * @returns {{ meshCoordinator: MeshCoordinator, gatewayLoop: GatewayLoop, outboundQueue: PersistentOutboundQueue, retryScheduler: RetryScheduler } | null}
 */
export function bootstrapMesh({
  meshConfig,
  relayStore,
  metrics,
  identity,
  relayConnectionPool,
  routeTable = null,
  inboxRouter,
  inboxStore,
  keyValueStore = null,
  routeResolver = null,
} = {}) {
  if (!relayStore || !metrics || !identity) {
    return null;
  }

  const meshCoordinator = new MeshCoordinator({
    relayStore,
    inboxRouter,
    metrics,
    meshConfig,
    relayConnectionPool,
  });

  // Persistent outbound queue — requires encrypted KV store for at-rest encryption.
  // Falls back to in-memory-only behavior if no KV store is provided (queue won't
  // survive restarts, but send-path still works).
  const outboundQueue = keyValueStore
    ? new PersistentOutboundQueue({
        keyValueStore,
        maxPerInbox: 100,
        maxTotal: 1000,
      })
    : null;

  const gatewayLoopOpts = {
    relaySelector: new GatewayRelaySelector(),
    pathPlanner: new GatewayPathPlanner(),
    sender: new GatewaySender({ endpointId: identity.accountId, pool: relayConnectionPool }),
    crypto: new NodeCryptoProvider(),
    relayStore,
    relayConnectionPool: relayConnectionPool || null,
    routeTable: routeTable || (inboxRouter ? inboxRouter.routeTable : null) || null,
    inboxRouter,
    inboxStore,
    routePolicy: {
      defaultHops: meshConfig && meshConfig.policy && meshConfig.policy.defaultHops != null ? meshConfig.policy.defaultHops : 1,
      forceOnionRouting: meshConfig && meshConfig.policy && meshConfig.policy.forceOnionRouting === true,
    },
    outboundQueue,
  };
  if (routeResolver) gatewayLoopOpts.routeResolver = routeResolver;
  const gatewayLoop = new GatewayLoop(gatewayLoopOpts);

  // Retry scheduler — polls the persistent queue and re-attempts delivery.
  const retryScheduler = outboundQueue
    ? new RetryScheduler({
        queue: outboundQueue,
        sendFn: async (entry) => {
          await gatewayLoop._sendToInboxInternal({
            innerBytes: entry.innerBytes,
            deliverInboxId: entry.deliverInboxId,
            receiptInboxId: entry.receiptInboxId || null,
          });
        },
      })
    : null;

  // Wire route-discovery flush: when a new route appears, immediately
  // retry any queued messages for those inboxes.
  const rt = routeTable || (inboxRouter ? inboxRouter.routeTable : null) || null;
  if (rt && typeof rt.setOnRouteAdded === "function") {
    rt.setOnRouteAdded((inboxIds) => {
      if (!retryScheduler) return;
      for (const id of inboxIds) {
        retryScheduler.flushForInbox(id);
      }
    });
  }

  return { meshCoordinator, gatewayLoop, outboundQueue, retryScheduler };
}
