import test from "node:test";
import assert from "node:assert/strict";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";

function flipByte(bytes) {
  const out = new Uint8Array(bytes);
  out[0] = (out[0] + 1) % 256;
  return out;
}

test("NodeCryptoProvider AES-256-GCM roundtrip with AAD", async () => {
  const crypto = new NodeCryptoProvider();
  const key = crypto.randomBytes(32);
  const nonce = crypto.randomBytes(12);
  const plaintext = crypto.randomBytes(64);
  const aad = crypto.randomBytes(16);

  const ct = crypto.aeadEncrypt({ key, nonce, plaintext, aad });
  const pt = crypto.aeadDecrypt({ key, nonce, ciphertext: ct, aad });

  assert.deepEqual(pt, plaintext);

  assert.throws(
    () => crypto.aeadDecrypt({ key, nonce, ciphertext: ct, aad: flipByte(aad) }),
    /auth|decrypt|tag|bad|invalid/i
  );
});

test("NodeCryptoProvider HKDF output length", () => {
  const crypto = new NodeCryptoProvider();
  const ikm = crypto.randomBytes(32);
  const salt = crypto.randomBytes(16);
  const info = crypto.randomBytes(8);

  const out = crypto.hkdfSha256(ikm, { salt, info, length: 42 });
  assert.equal(out.length, 42);
});

test("NodeCryptoProvider X25519 shared secret match", () => {
  const crypto = new NodeCryptoProvider();
  const alice = crypto.dhGenerateKeyPair({ alg: "X25519", fmt: "spki" });
  const bob = crypto.dhGenerateKeyPair({ alg: "X25519", fmt: "spki" });

  const secretA = crypto.dhDerive({ privateKey: alice.privateKey, publicKey: bob.publicKey, alg: "X25519", fmt: "spki" });
  const secretB = crypto.dhDerive({ privateKey: bob.privateKey, publicKey: alice.publicKey, alg: "X25519", fmt: "spki" });

  assert.deepEqual(secretA, secretB);
});
