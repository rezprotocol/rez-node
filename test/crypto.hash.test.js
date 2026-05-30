import test from "node:test";
import assert from "node:assert/strict";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

function toHex(bytes) {
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

test("NodeCryptoProvider hashSha256 uses test vector", async () => {
  const provider = new NodeCryptoProvider();
  const input = new TextEncoder().encode("abc");
  const hash = await provider.hashSha256(input);

  assert.equal(
    toHex(hash),
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
  );
});
