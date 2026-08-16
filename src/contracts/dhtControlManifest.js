/**
 * DHT control-type manifest (ATLAS_PREREQUISITES P4.1).
 *
 * The WS wire-manifest architecture test governs REZ_CONTRACT_TYPES only —
 * DHT control messages dispatch through ControlMessageRegistry and got no
 * totality guardrail. This manifest is that guardrail's SSOT: every `dht.*`
 * control type a DHT protocol class registers or emits must appear here
 * exactly once, with its direction and the record class that validates it
 * (or "handler" when validation is inline field checks).
 *
 * architecture.dht-control-manifest.test.js enforces totality against the
 * actual source registrations.
 */
export const DHT_CONTROL_MANIFEST = Object.freeze({
  "dht.find_node": { direction: "request", validatedBy: "handler", owner: "DhtProtocol" },
  "dht.find_node.reply": { direction: "response", validatedBy: "handler", owner: "DhtProtocol" },
  "dht.find_value": { direction: "request", validatedBy: "handler", owner: "DhtProtocol" },
  "dht.find_value.reply": { direction: "response", validatedBy: "handler", owner: "DhtProtocol" },
  "dht.store": { direction: "request", validatedBy: "handler", owner: "DhtProtocol" },
  "dht.rec_store": { direction: "request", validatedBy: "DhtRecordStoreRequestV1", owner: "DurableRecordProtocol" },
  "dht.rec_store.ack": { direction: "response", validatedBy: "DhtRecordStoreAckV1", owner: "DurableRecordProtocol" },
  "dht.rec_find": { direction: "request", validatedBy: "handler", owner: "DurableRecordProtocol" },
  "dht.rec_find.reply": { direction: "response", validatedBy: "handler", owner: "DurableRecordProtocol" },
});
