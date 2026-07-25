/**
 * Relay-key-id normalization, in ONE place.
 *
 * There were three module-private `normalizeRelayKeyId` functions: two byte-identical string
 * normalizers (InboxRouter, resolveDeliveryDescriptor) and a third in RelayStore that took a
 * DESCRIPTOR OBJECT and read `.relayKeyId` off it. Same name, incompatible contracts — moving a
 * call site between those files would have compiled fine and silently normalized everything to "".
 *
 * The two are separated here by name rather than merged, because they answer different questions:
 * one cleans a key you already hold, the other extracts a key from a record.
 */

/** Trim a relay key id, or "" when it is absent/blank/not a string. */
export function normalizeRelayKeyId(value) {
  return typeof value === "string" && value.trim() ? value.trim() : "";
}

/** The normalized relay key id carried BY a descriptor/relay record, or "" when it has none. */
export function relayKeyIdOf(record) {
  return record && typeof record.relayKeyId === "string" && record.relayKeyId.trim()
    ? record.relayKeyId.trim()
    : "";
}
