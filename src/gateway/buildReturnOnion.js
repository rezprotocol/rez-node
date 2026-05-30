/**
 * Builds the return-path spec for delivery receipts (SURB-style).
 * Forward path is [R1, R2, ..., DeliveryRelay]. Return path is [R(n-1), ..., R1] with
 * delivery to sender's inbox. The delivery relay will use this spec to build the actual
 * return onion with innerBytes = receipt.
 *
 * @param {{ plan: { hops: Array<{ relayKeyId: string }>, pathEntries: Array<{ relayKeyId: string, relayDescriptor: object }> }, normalizedSelected: Array<object>, senderDeliverInboxId: string }} opts
 * @returns {{ pathEntries: Array<object>, finalRelayKeyId: string, deliverInboxId: string, entryRelayKeyId: string } | null} null if path too short to have a return hop
 */
export function buildReturnPathSpec({ plan, normalizedSelected, senderDeliverInboxId } = {}) {
  if (!plan?.hops?.length || !normalizedSelected?.length || !senderDeliverInboxId) {
    return null;
  }
  // Need at least 2 hops: one entry + one delivery. Return path = all but last, reversed.
  if (plan.hops.length < 2) return null;

  const returnHopCount = plan.hops.length - 1;
  const pathEntries = [];
  for (let i = returnHopCount - 1; i >= 0; i -= 1) {
    const descriptor = normalizedSelected[i];
    if (!descriptor?.relayKeyId) continue;
    const hop = plan.hops[i];
    pathEntries.push({
      relayKeyId: descriptor.relayKeyId,
      relayDescriptor: descriptor,
      onionKeyId: hop.onionKeyId,
      onionPubKeyBytes: descriptor.onionKeys?.find((k) => k.onionKeyId === hop.onionKeyId)?.publicKeyBytes,
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
