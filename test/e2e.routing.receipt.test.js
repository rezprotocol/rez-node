import test from "node:test";
import { X25519_SUPPORTED } from "../src/crypto/dh/index.js";

test("Gateway relay store → onion send → forward → deposit → receipt", async (t) => {
  if (!X25519_SUPPORTED) {
    t.skip("X25519 not supported");
    return;
  }
  t.skip("TODO: rewrite e2e routing test to use TCP gossip instead of removed HTTP directory");
});
