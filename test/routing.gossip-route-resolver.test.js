import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { GossipRouteResolver } from "../src/routing/GossipRouteResolver.js";

describe("GossipRouteResolver", () => {
  it("returns route from routeTable when available", async () => {
    const resolver = new GossipRouteResolver();
    const routeEntry = { direct: true, socket: null, hops: 0 };
    const routeTable = {
      get(id) { return id === "inbox:test" ? routeEntry : null; },
    };
    const result = await resolver.resolve("inbox:test", { routeTable, relayConnectionPool: null });
    assert.equal(result, routeEntry);
  });

  it("queries relayConnectionPool when routeTable has no route", async () => {
    const resolver = new GossipRouteResolver();
    const routeEntry = { direct: false, hops: 2, deliveryRelayKeyId: "relay-a" };
    let queryCalledWith = null;
    let callCount = 0;
    const routeTable = {
      get(id) {
        callCount += 1;
        // First call returns null, second call (after query) returns the route
        return callCount > 1 ? routeEntry : null;
      },
    };
    const relayConnectionPool = {
      async queryRoute(inboxId) {
        queryCalledWith = inboxId;
        return true;
      },
    };
    const result = await resolver.resolve("inbox:remote", { routeTable, relayConnectionPool });
    assert.equal(queryCalledWith, "inbox:remote");
    assert.equal(result, routeEntry);
  });

  it("returns null when no route found anywhere", async () => {
    const resolver = new GossipRouteResolver();
    const routeTable = { get() { return null; } };
    const relayConnectionPool = {
      async queryRoute() { return false; },
    };
    const result = await resolver.resolve("inbox:unknown", { routeTable, relayConnectionPool });
    assert.equal(result, null);
  });

  it("returns null when relayConnectionPool is null", async () => {
    const resolver = new GossipRouteResolver();
    const routeTable = { get() { return null; } };
    const result = await resolver.resolve("inbox:test", { routeTable, relayConnectionPool: null });
    assert.equal(result, null);
  });

  it("handles queryRoute throwing an error", async () => {
    const resolver = new GossipRouteResolver();
    const routeTable = { get() { return null; } };
    const relayConnectionPool = {
      async queryRoute() { throw new Error("connection failed"); },
    };
    const result = await resolver.resolve("inbox:test", { routeTable, relayConnectionPool });
    assert.equal(result, null);
  });

  it("works with null routeTable", async () => {
    const resolver = new GossipRouteResolver();
    const result = await resolver.resolve("inbox:test", { routeTable: null, relayConnectionPool: null });
    assert.equal(result, null);
  });
});
