import assert from "node:assert/strict";
import test from "node:test";

import {
  ContractRegistry,
  WS_CONTRACT_EXAMPLES,
  WsEnvelope,
  registerAllContracts,
} from "../src/contracts/index.js";

test("WS_CONTRACT_EXAMPLES covers every registered contract with valid records", () => {
  const registry = registerAllContracts(new ContractRegistry());
  const types = registry.listTypes();

  for (const type of types) {
    const factory = WS_CONTRACT_EXAMPLES[type];
    assert.equal(typeof factory, "function", type + " has an example factory");

    const Ctor = registry.get(type);
    const record = new Ctor(factory());
    const envelope = new WsEnvelope({
      id: "example:" + type,
      t: type,
      body: record,
    });
    const json = envelope.toJSON();

    assert.equal(json.t, type);
    assert.equal(json.id, "example:" + type);
    assert.equal(typeof json.body, "object");
    assert.equal(json.body.exampleUnavailable, undefined);
  }
});
