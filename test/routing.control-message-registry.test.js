import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { ControlMessageRegistry } from "../src/routing/ControlMessageRegistry.js";

describe("ControlMessageRegistry", () => {
  it("dispatches to registered handler", async () => {
    const registry = new ControlMessageRegistry();
    let received = null;
    registry.register("dht.find_node", (ctlObj, socket) => {
      received = { ctlObj, socket };
    });
    const socket = { id: "test" };
    const ctlObj = { _ctl: "dht.find_node", target: "abc" };
    const handled = await registry.dispatch("dht.find_node", ctlObj, socket);
    assert.equal(handled, true);
    assert.deepStrictEqual(received.ctlObj, ctlObj);
    assert.equal(received.socket, socket);
  });

  it("returns false for unregistered type", async () => {
    const registry = new ControlMessageRegistry();
    const handled = await registry.dispatch("unknown.type", {}, {});
    assert.equal(handled, false);
  });

  it("has() returns correct state", () => {
    const registry = new ControlMessageRegistry();
    assert.equal(registry.has("dht.store"), false);
    registry.register("dht.store", () => {});
    assert.equal(registry.has("dht.store"), true);
  });

  it("unregister removes handler", async () => {
    const registry = new ControlMessageRegistry();
    registry.register("dht.store", () => {});
    assert.equal(registry.has("dht.store"), true);
    registry.unregister("dht.store");
    assert.equal(registry.has("dht.store"), false);
    const handled = await registry.dispatch("dht.store", {}, {});
    assert.equal(handled, false);
  });

  it("throws on invalid ctlType", () => {
    const registry = new ControlMessageRegistry();
    assert.throws(
      () => registry.register("", () => {}),
      { message: /ctlType must be a non-empty string/ },
    );
    assert.throws(
      () => registry.register("  ", () => {}),
      { message: /ctlType must be a non-empty string/ },
    );
  });

  it("throws on non-function handler", () => {
    const registry = new ControlMessageRegistry();
    assert.throws(
      () => registry.register("dht.store", "not-a-function"),
      { message: /handler must be a function/ },
    );
  });

  it("handles async handlers", async () => {
    const registry = new ControlMessageRegistry();
    let called = false;
    registry.register("dht.find_value", async () => {
      await new Promise((r) => setTimeout(r, 1));
      called = true;
    });
    const handled = await registry.dispatch("dht.find_value", {}, {});
    assert.equal(handled, true);
    assert.equal(called, true);
  });

  it("later registration overwrites earlier", async () => {
    const registry = new ControlMessageRegistry();
    let firstCalled = false;
    let secondCalled = false;
    registry.register("dht.store", () => { firstCalled = true; });
    registry.register("dht.store", () => { secondCalled = true; });
    await registry.dispatch("dht.store", {}, {});
    assert.equal(firstCalled, false);
    assert.equal(secondCalled, true);
  });
});
