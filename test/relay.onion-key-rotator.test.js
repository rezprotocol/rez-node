import test from "node:test";
import assert from "node:assert/strict";
import { OnionKeyringV1 } from "@rezprotocol/core";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import { OnionKeyRotator } from "../src/relay/OnionKeyRotator.js";

test("OnionKeyRotator requires cryptoProvider, keyring, onDescriptorUpdate, deviceId", () => {
  const crypto = new NodeCryptoProvider();
  const keyring = new OnionKeyringV1();
  assert.throws(
    () =>
      new OnionKeyRotator({
        keyring,
        onDescriptorUpdate: () => {},
        deviceId: "dev1",
      }),
    /cryptoProvider/
  );
  assert.throws(
    () =>
      new OnionKeyRotator({
        cryptoProvider: crypto,
        onDescriptorUpdate: () => {},
        deviceId: "dev1",
      }),
    /keyring/
  );
  assert.throws(
    () =>
      new OnionKeyRotator({
        cryptoProvider: crypto,
        keyring,
        deviceId: "dev1",
      }),
    /onDescriptorUpdate/
  );
  assert.throws(
    () =>
      new OnionKeyRotator({
        cryptoProvider: crypto,
        keyring,
        onDescriptorUpdate: () => {},
      }),
    /deviceId/
  );
});

test("OnionKeyRotator start() generates initial key and returns it from getActiveKeyRecords", () => {
  const crypto = new NodeCryptoProvider();
  const keyring = new OnionKeyringV1();
  const updates = [];
  const rotator = new OnionKeyRotator({
    cryptoProvider: crypto,
    keyring,
    onDescriptorUpdate: (records) => updates.push(records),
    deviceId: "node-dev1",
    ttlMs: 10_000,
    nowMs: () => 1000,
  });
  rotator.start();

  const records = rotator.getActiveKeyRecords({ nowMs: 2000 });
  assert.equal(records.length, 1);
  assert.equal(records[0].status, "active");
  assert.ok(records[0].onionKeyId.startsWith("node-dev1-"));
  assert.equal(records[0].notBefore, 1000 - 3_600_000);
  assert.equal(records[0].notAfter, 1000 + 10_000);
  assert.ok(records[0].publicKeyBytes instanceof Uint8Array && records[0].publicKeyBytes.length > 0);

  rotator.stop();
  assert.equal(updates.length, 0);
});

test("OnionKeyRotator getActiveKeyRecords filters by time window", () => {
  const crypto = new NodeCryptoProvider();
  const keyring = new OnionKeyringV1();
  const rotator = new OnionKeyRotator({
    cryptoProvider: crypto,
    keyring,
    onDescriptorUpdate: () => {},
    deviceId: "dev",
    ttlMs: 10_000,
    nowMs: () => 5000,
  });
  rotator.start();

  const notBefore = 5000 - 3_600_000;
  const notAfter = 5000 + 10_000;
  assert.equal(rotator.getActiveKeyRecords({ nowMs: notBefore - 1 }).length, 0);
  assert.equal(rotator.getActiveKeyRecords({ nowMs: 5000 }).length, 1);
  assert.equal(rotator.getActiveKeyRecords({ nowMs: notAfter }).length, 0);

  rotator.stop();
});

test("OnionKeyRotator rotation triggers at rotateAtFraction of TTL and calls onDescriptorUpdate", async () => {
  let nowMs = 1000;
  const crypto = new NodeCryptoProvider();
  const keyring = new OnionKeyringV1();
  const updates = [];
  const ttlMs = 1000;
  const rotateAtFraction = 0.3;

  const rotator = new OnionKeyRotator({
    cryptoProvider: crypto,
    keyring,
    onDescriptorUpdate: (records) => updates.push([...records]),
    deviceId: "dev",
    ttlMs,
    rotateAtFraction,
    nowMs: () => nowMs,
  });
  rotator.start();

  assert.equal(rotator.getActiveKeyRecords({ nowMs }).length, 1);
  assert.equal(updates.length, 0);

  await new Promise((resolve) => {
    const rotationTime = 1000 + 1000 * 0.3;
    const delay = rotationTime - nowMs + 10;
    setTimeout(() => {
      nowMs = rotationTime + 10;
      resolve();
    }, delay);
  });

  assert.equal(updates.length, 1);
  const recordsAfter = updates[0];
  assert.equal(recordsAfter.length, 2);
  const active = recordsAfter.filter((r) => r.status === "active");
  const draining = recordsAfter.filter((r) => r.status === "draining");
  assert.equal(active.length, 1);
  assert.equal(draining.length, 1);

  rotator.stop();
});

test("OnionKeyRotator stop() clears timers and getActiveKeyRecords still returns current keys", () => {
  const crypto = new NodeCryptoProvider();
  const keyring = new OnionKeyringV1();
  const rotator = new OnionKeyRotator({
    cryptoProvider: crypto,
    keyring,
    onDescriptorUpdate: () => {},
    deviceId: "dev",
    ttlMs: 100_000,
    nowMs: () => 5000,
  });
  rotator.start();
  assert.equal(rotator.getActiveKeyRecords({ nowMs: 6000 }).length, 1);
  rotator.stop();
  assert.equal(rotator.getActiveKeyRecords({ nowMs: 6000 }).length, 1);
});

test("OnionKeyRotator key can be used for decryption via keyring", () => {
  const crypto = new NodeCryptoProvider();
  const keyring = new OnionKeyringV1();
  const rotator = new OnionKeyRotator({
    cryptoProvider: crypto,
    keyring,
    onDescriptorUpdate: () => {},
    deviceId: "dev",
    ttlMs: 10_000,
    nowMs: () => 2000,
  });
  rotator.start();

  const records = rotator.getActiveKeyRecords({ nowMs: 2000 });
  assert.equal(records.length, 1);
  const onionKeyId = records[0].onionKeyId;
  const privKey = keyring.getKeyForDecrypt(onionKeyId, 2000);
  assert.ok(privKey instanceof Uint8Array && privKey.length > 0);

  assert.throws(
    () => keyring.getKeyForDecrypt(onionKeyId, 2000 + 20_000),
    /not in valid time window/
  );

  rotator.stop();
});

test("OnionKeyRotator double start is no-op", () => {
  const crypto = new NodeCryptoProvider();
  const keyring = new OnionKeyringV1();
  const rotator = new OnionKeyRotator({
    cryptoProvider: crypto,
    keyring,
    onDescriptorUpdate: () => {},
    deviceId: "dev",
    ttlMs: 10_000,
    nowMs: () => 1000,
  });
  rotator.start();
  rotator.start();
  assert.equal(rotator.getActiveKeyRecords({ nowMs: 2000 }).length, 1);
  rotator.stop();
});
