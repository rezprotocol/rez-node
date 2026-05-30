import test from "node:test";
import assert from "node:assert/strict";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";


test("NodeCryptoProvider randomBytes length", () => {
  const provider = new NodeCryptoProvider();
  const bytes = provider.randomBytes(16);

  assert.equal(bytes.length, 16);
});

test("NodeCryptoProvider randomBytes varies", () => {
  const provider = new NodeCryptoProvider();

  const a = provider.randomBytes(32);
  const b = provider.randomBytes(32);

  // Not guaranteed, but extremely improbable to be equal.
  assert.notDeepEqual(a, b);
});
