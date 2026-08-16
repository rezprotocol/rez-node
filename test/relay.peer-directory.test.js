import test from "node:test";
import assert from "node:assert/strict";
import { RelayPeerDirectory } from "../src/relay/RelayPeerDirectory.js";
import { makeRelayIdentity } from "./support/relayIdentity.js";

function createSocket({ destroyed = false } = {}) {
  return {
    destroyed,
    write() {},
  };
}

test("RelayPeerDirectory stores pending challenges per socket", () => {
  const dir = new RelayPeerDirectory();
  const socket = createSocket();
  const challenge = dir.issueChallenge(socket, {
    expectedRelayKeyId: "relay-1",
    presentedNodeKeyId: "node-1",
    presentedNodePublicKeyB64: "pub-1",
  });
  assert.ok(challenge && challenge.challengeId);
  assert.ok(challenge && challenge.nonceB64);
  const pending = dir.getPendingChallenge(socket);
  assert.equal(pending && pending.expectedRelayKeyId, "relay-1");
  assert.equal(dir.getAuth(socket), null);
});

test("RelayPeerDirectory keeps node-authenticated sockets out of relay lookups until promoted", () => {
  const dir = new RelayPeerDirectory();
  const socket = createSocket();
  const identity = makeRelayIdentity();
  const auth = dir.authenticate(socket, {
    relayKeyId: identity.relayKeyId,
    nodeKeyId: identity.nodeKeyId,
    nodePublicKeyB64: identity.nodePublicKeyB64,
    authLevel: "relay-provisional",
  });
  assert.equal(auth && auth.authLevel, "relay-provisional");
  assert.equal(dir.isAuthenticatedSocket(socket), true);
  assert.equal(dir.isAuthenticatedRelaySocket(socket), false);
  assert.equal(dir.getSocket(identity.relayKeyId), null);
});

test("RelayPeerDirectory promotes relay-verified sockets into authoritative routing lookups", () => {
  const dir = new RelayPeerDirectory();
  const socket = createSocket();
  const identity = makeRelayIdentity();
  dir.authenticate(socket, {
    relayKeyId: identity.relayKeyId,
    nodeKeyId: identity.nodeKeyId,
    nodePublicKeyB64: identity.nodePublicKeyB64,
    authLevel: "node",
  });
  const promoted = dir.promoteRelay(socket, {
    relayKeyId: identity.relayKeyId,
  });
  assert.equal(promoted && promoted.authLevel, "relay-verified");
  assert.equal(dir.getSocket(identity.relayKeyId), socket);
  assert.equal(dir.getRelayKeyIdForSocket(socket), identity.relayKeyId);
  assert.equal(dir.size, 1);
});

test("RelayPeerDirectory getSocket returns null for destroyed socket", () => {
  const dir = new RelayPeerDirectory();
  const socket = createSocket({ destroyed: true });
  const identity = makeRelayIdentity();
  dir.authenticate(socket, {
    relayKeyId: identity.relayKeyId,
    nodeKeyId: identity.nodeKeyId,
    nodePublicKeyB64: identity.nodePublicKeyB64,
    authLevel: "relay-verified",
  });
  assert.equal(dir.getSocket(identity.relayKeyId), null);
});

test("RelayPeerDirectory remove clears mappings", () => {
  const dir = new RelayPeerDirectory();
  const socket = createSocket();
  const identity = makeRelayIdentity();
  dir.authenticate(socket, {
    relayKeyId: identity.relayKeyId,
    nodeKeyId: identity.nodeKeyId,
    nodePublicKeyB64: identity.nodePublicKeyB64,
    authLevel: "relay-verified",
  });
  assert.equal(dir.getSocket(identity.relayKeyId), socket);
  dir.remove(socket);
  assert.equal(dir.getSocket(identity.relayKeyId), null);
  assert.equal(dir.getAuth(socket), null);
  assert.equal(dir.size, 0);
});
