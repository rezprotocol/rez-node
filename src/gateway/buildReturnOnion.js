/**
 * Builds the return-path spec for delivery receipts (SURB-style).
 * Forward path is [R1, R2, ..., DeliveryRelay]. Return path is [R(n-1), ..., R1] with
 * delivery to sender's inbox. The delivery relay will use this spec to build the actual
 * return onion with innerBytes = receipt.
 *
 * @param {{ plan: { hops: Array<{ relayKeyId: string }>, pathEntries: Array<{ relayKeyId: string, relayDescriptor: object }> }, normalizedSelected: Array<object>, senderDeliverInboxId: string }} opts
 * @returns {{ pathEntries: Array<object>, finalRelayKeyId: string, deliverInboxId: string, entryRelayKeyId: string } | null} null if path too short to have a return hop
 */
// The hop's onion key, or undefined when the descriptor names none. Extracted so the lookup reads
// as one intention instead of a chain of optional accesses.
function onionKeyFor(descriptor, onionKeyId) {
  const keys = Array.isArray(descriptor.onionKeys) ? descriptor.onionKeys : null;
  if (keys === null) return undefined;
  const match = keys.find((k) => k.onionKeyId === onionKeyId);
  return match ? match.publicKeyBytes : undefined;
}


export function buildReturnPathSpec({ plan, normalizedSelected, senderDeliverInboxId } = {}) {
  const planHops = plan && Array.isArray(plan.hops) ? plan.hops : null;
  if (!planHops || planHops.length === 0 || !normalizedSelected || normalizedSelected.length === 0 || !senderDeliverInboxId) {
    return null;
  }
  // Need at least 2 hops: one entry + one delivery. Return path = all but last, reversed.
  if (plan.hops.length < 2) return null;

  const returnHopCount = plan.hops.length - 1;
  const pathEntries = [];
  for (let i = returnHopCount - 1; i >= 0; i -= 1) {
    const descriptor = normalizedSelected[i];
    if (!descriptor || !descriptor.relayKeyId) continue;
    const hop = plan.hops[i];
    pathEntries.push({
      relayKeyId: descriptor.relayKeyId,
      relayDescriptor: descriptor,
      onionKeyId: hop.onionKeyId,
      onionPubKeyBytes: onionKeyFor(descriptor, hop.onionKeyId),
    });
  }
  if (pathEntries.length === 0) return null;

  const finalRelayKeyId = pathEntries[pathEntries.length - 1].relayKeyId;
  const entryRelayKeyId = pathEntries[0].relayKeyId;

  return {
    pathEntries,
    finalRelayKeyId,
    deliverInboxId: senderDeliverInboxId,
    entryRelayKeyId,
  };
}
