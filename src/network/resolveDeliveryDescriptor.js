/**
 * Single place for resolving a route entry (from InboxRouter.getRouteTo) to the delivery relay descriptor.
 * ID-based resolution only: direct self, then delivery relay identity.
 * @param {object} routeEntry - { direct?, deliveryRelayKeyId?, relayKeyId? }
 * @param {{ descriptors?: object[], relayStore?: object, nowMs?: number }} opts
 * @returns {object|null} descriptor or null
 */
export function resolveDeliveryDescriptor(routeEntry, { descriptors = [], relayStore = null, nowMs = Date.now() } = {}) {
  if (!routeEntry || typeof routeEntry !== "object") return null;
  if (routeEntry.direct) return relayStore?.getSelfDescriptor?.({ nowMs }) ?? null;
  const id = normalizeRelayKeyId(routeEntry.deliveryRelayKeyId) || normalizeRelayKeyId(routeEntry.relayKeyId);
  if (id && relayStore && typeof relayStore.getDescriptor === "function") {
    const d = relayStore.getDescriptor(id, { nowMs });
    if (d) return d;
  }
  if (id && Array.isArray(descriptors)) {
    const match = descriptors.find((d) => normalizeRelayKeyId(d?.relayKeyId) === id);
    if (match) return match;
  }
  return null;
}

function normalizeRelayKeyId(value) {
  return typeof value === "string" && value.trim() ? value.trim() : "";
}
