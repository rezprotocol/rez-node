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

test("handleSessionHello rejects a MISMATCHED contract version — the gate for wire-breaking changes", () => {
  // This is the primary defense for a changed request/response record: a peer on a different
  // contract version is refused at CONNECT, not at whichever later RPC happens to touch the changed
  // shape. It is an equality check on purpose — an older peer cannot satisfy a newly required field,
  // and a newer one cannot interpret its absence, so neither direction is "compatible enough".
  // Pinned here because the whole mixed-version story (see rez-sdk
  // compat.outbox-claim-mixed-version.test.js) rests on this assertion actually firing.
  const crypto = new NodeCryptoProvider();
  const accountIdentityPublicKeyB64 = bytesToBase64(crypto.generateSigningKeyPair().publicKey);
  for (const version of [CONTRACT_VERSION - 1, CONTRACT_VERSION + 1]) {
    const result = handleSessionHello({
      body: { contractVersion: version, deviceId: "dev:test", accountIdentityPublicKeyB64 },
    });
    assert.ok(result?.error, "contractVersion " + version + " must be refused");
    assert.ok(
      ["UNAUTHORIZED", "BAD_REQUEST"].includes(result.error.code),
      `unexpected error code ${result.error.code} for contractVersion ${version}`,
    );
  }
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
