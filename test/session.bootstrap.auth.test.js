import test from "node:test";
import assert from "node:assert/strict";
import { bytesToBase64 } from "@rezprotocol/core";
import { CONTRACT_VERSION } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { handleSessionHello } from "../src/protocol/sessionBootstrap.js";

test("handleSessionHello enters pending authentication for a well-formed session.hello", () => {
  const crypto = new NodeCryptoProvider();
  const keyPair = crypto.generateSigningKeyPair();
  const accountIdentityPublicKeyB64 = bytesToBase64(keyPair.publicKey);
  const result = handleSessionHello({
    body: {
      contractVersion: CONTRACT_VERSION,
      deviceId: "dev:test",
      accountIdentityPublicKeyB64,
    },
  });

  assert.equal(result?.error, undefined);
  assert.equal(result?.sessionDeviceId, "dev:test");
  assert.equal(result?.accountIdentityPublicKeyB64, accountIdentityPublicKeyB64);
  assert.deepEqual(result?.pendingAuthentication, {
    sessionDeviceId: "dev:test",
    accountIdentityPublicKeyB64,
  });
});

test("handleSessionHello rejects missing identity key with an error", () => {
  const result = handleSessionHello({
    body: {
      contractVersion: CONTRACT_VERSION,
      deviceId: "dev:test",
    },
  });
  assert.ok(result?.error, "expected error result");
  assert.ok(["UNAUTHORIZED", "BAD_REQUEST"].includes(result.error.code), `unexpected error code ${result.error.code}`);
});

test("handleSessionHello rejects invalid base64 identity key", () => {
  const result = handleSessionHello({
    body: {
      contractVersion: CONTRACT_VERSION,
      deviceId: "dev:test",
      accountIdentityPublicKeyB64: "not-base64-!@#$%^",
    },
  });
  assert.equal(result?.error?.code, "UNAUTHORIZED");
});
