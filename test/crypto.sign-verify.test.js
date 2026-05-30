import test from "node:test";
import assert from "node:assert/strict";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

function tweak(bytes) {
  const out = new Uint8Array(bytes);
  out[0] = (out[0] + 1) % 256;
  return out;
}

test("NodeCryptoProvider sign/verify", async () => {
  const provider = new NodeCryptoProvider();
  const { publicKey, privateKey } = provider.generateSigningKeyPair();

  const bytes = provider.randomBytes(32);
  const signature = await provider.sign({ privateKey, msg: bytes });

  assert.equal(await provider.verify({ publicKey, msg: bytes, sig: signature }), true);
  assert.equal(await provider.verify({ publicKey, msg: tweak(bytes), sig: signature }), false);
  assert.equal(await provider.verify({ publicKey, msg: bytes, sig: tweak(signature) }), false);
});
