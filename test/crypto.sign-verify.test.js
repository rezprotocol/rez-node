import test from "node:test";
import assert from "node:assert/strict";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { SeedKeys } from "@rezprotocol/core/src/crypto/seedDerivation.js";

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

test("NodeCryptoProvider deterministic signing key matches the frozen SeedKeys encoding", () => {
  const provider = new NodeCryptoProvider();
  const seed = new Uint8Array(32).fill(23);
  const expected = SeedKeys.deriveEd25519({ seed, label: "rez:crypto-provider:test" });
  const rawPrivate = SeedKeys.deriveBytes({ seed, label: "rez:crypto-provider:test", length: 32 });
  const actual = provider.signingKeyPairFromSeed(rawPrivate);
  assert.equal(Buffer.from(actual.publicKey).toString("base64"), expected.publicKeyB64);
  assert.equal(Buffer.from(actual.privateKey).toString("base64"), expected.privateKeyB64);
});
