import test from "node:test";
import assert from "node:assert/strict";
import { RouteTable } from "../src/routing/RouteTable.js";

test("RouteTable addLocal and get", () => {
  const rt = new RouteTable();
  const socket = { id: "s1" };
  rt.addLocal("inbox:a", socket, { selfRelayKeyId: "relay-1", nowMs: 1000 });

  const entry = rt.get("inbox:a");
  assert.ok(entry);
  assert.equal(entry.direct, true);
  assert.equal(entry.hops, 0);
  assert.equal(entry.socket, socket);
  assert.equal(entry.addedAtMs, 1000);
  assert.equal(entry.nextHopRelayKeyId, "relay-1");
  assert.equal(entry.deliveryRelayKeyId, "relay-1");
  assert.equal(entry.announceToPeers, true);
});

test("RouteTable addLocal with no socket (hosted inbox)", () => {
  const rt = new RouteTable();
  rt.addLocal("inbox:hosted", null, { selfRelayKeyId: "relay-1", nowMs: 2000 });

  assert.ok(rt.isLocalHosted("inbox:hosted"));
  assert.equal(rt.has("inbox:hosted"), true);
  assert.equal(rt.size, 1);
});

test("RouteTable addLocal replaces stale socket binding", () => {
  const rt = new RouteTable();
  const old = { id: "old" };
  const fresh = { id: "fresh" };
  rt.addLocal("inbox:x", old, { nowMs: 1 });
  rt.addLocal("inbox:x", fresh, { nowMs: 2 });

  const entry = rt.get("inbox:x");
  assert.equal(entry.socket, fresh);
  // Old socket should have no associated inboxes
  const withdrawn = rt.removeAllForSocket(old);
  assert.equal(withdrawn.length, 0);
});

test("RouteTable addRemote basic", () => {
  const rt = new RouteTable();
  const accepted = rt.addRemote("inbox:r", {
    hops: 2,
    nextHopRelayKeyId: "relay-a",
    deliveryRelayKeyId: "relay-b",
    nowMs: 5000,
  });
  assert.equal(accepted, true);
  const entry = rt.get("inbox:r");
  assert.ok(entry);
  assert.equal(entry.direct, false);
  assert.equal(entry.hops, 2);
  assert.equal(entry.nextHopRelayKeyId, "relay-a");
  assert.equal(entry.deliveryRelayKeyId, "relay-b");
});

test("RouteTable addRemote does not replace direct route", () => {
  const rt = new RouteTable();
  rt.addLocal("inbox:d", null, { nowMs: 1 });
  const accepted = rt.addRemote("inbox:d", {
    hops: 1,
    nextHopRelayKeyId: "relay-x",
    deliveryRelayKeyId: "relay-x",
  });
  assert.equal(accepted, false);
  assert.equal(rt.get("inbox:d").direct, true);
});

test("RouteTable addRemote prefers shorter hops", () => {
  const rt = new RouteTable();
  rt.addRemote("inbox:r", { hops: 3, nextHopRelayKeyId: "a", deliveryRelayKeyId: "a" });
  const replaced = rt.addRemote("inbox:r", { hops: 1, nextHopRelayKeyId: "b", deliveryRelayKeyId: "b" });
  assert.equal(replaced, true);
  assert.equal(rt.get("inbox:r").hops, 1);

  const notReplaced = rt.addRemote("inbox:r", { hops: 5, nextHopRelayKeyId: "c", deliveryRelayKeyId: "c" });
  assert.equal(notReplaced, false);
  assert.equal(rt.get("inbox:r").hops, 1);
});

test("RouteTable remove", () => {
  const rt = new RouteTable();
  rt.addLocal("inbox:del", null, { nowMs: 1 });
  assert.equal(rt.has("inbox:del"), true);
  rt.remove("inbox:del");
  assert.equal(rt.has("inbox:del"), false);
  assert.equal(rt.get("inbox:del"), null);
});

test("RouteTable removeAllForSocket returns withdrawn ids", () => {
  const rt = new RouteTable();
  const s = { id: "sock" };
  rt.addLocal("inbox:1", s, { nowMs: 1 });
  rt.addLocal("inbox:2", s, { nowMs: 2 });
  rt.addLocal("inbox:3", null, { nowMs: 3 }); // no socket

  assert.equal(rt.size, 3);
  const withdrawn = rt.removeAllForSocket(s);
  assert.deepEqual(withdrawn.sort(), ["inbox:1", "inbox:2"]);
  assert.equal(rt.size, 1);
  assert.ok(rt.has("inbox:3"));
});

test("RouteTable getAll returns snapshot", () => {
  const rt = new RouteTable();
  rt.addLocal("inbox:snap", null, { nowMs: 1 });
  const all = rt.getAll();
  assert.equal(all.size, 1);
  assert.ok(all.has("inbox:snap"));
  // Mutating snapshot should not affect original
  all.delete("inbox:snap");
  assert.equal(rt.size, 1);
});

test("RouteTable setOnRouteAdded and notifyRoutesAdded", () => {
  const rt = new RouteTable();
  const added = [];
  rt.setOnRouteAdded((ids) => added.push(...ids));

  rt.notifyRoutesAdded(["inbox:a", "inbox:b"]);
  assert.deepEqual(added, ["inbox:a", "inbox:b"]);

  // Clear callback
  rt.setOnRouteAdded(null);
  rt.notifyRoutesAdded(["inbox:c"]);
  assert.deepEqual(added, ["inbox:a", "inbox:b"]); // unchanged
});

test("RouteTable rejects empty/invalid inboxIds", () => {
  const rt = new RouteTable();
  rt.addLocal("", null);
  rt.addLocal("  ", null);
  assert.equal(rt.size, 0);
  assert.equal(rt.get(""), null);
  assert.equal(rt.has("  "), false);
  assert.equal(rt.isLocalHosted(""), false);

  const accepted = rt.addRemote("", { hops: 1, nextHopRelayKeyId: "a", deliveryRelayKeyId: "a" });
  assert.equal(accepted, false);
});

test("RouteTable removeAllForSocket with null returns empty", () => {
  const rt = new RouteTable();
  const result = rt.removeAllForSocket(null);
  assert.deepEqual(result, []);
});

test("RouteTable removeAllForSocket preserves direct routes with a registration (socket nulled)", () => {
  const rt = new RouteTable();
  const sock = { id: "owner" };
  const registration = { inboxId: "inbox:registered", claimantPublicKeyB64: "k", delegationSigB64: "s" };

  rt.addLocal("inbox:registered", sock, { nowMs: 1, registration, installerSocket: sock });
  rt.addLocal("inbox:bare", sock, { nowMs: 2 });

  const withdrawn = rt.removeAllForSocket(sock);
  assert.deepEqual(withdrawn, ["inbox:bare"],
    "only the unregistered direct route is reported withdrawn");

  const survived = rt.get("inbox:registered");
  assert.ok(survived, "registered direct route entry must survive socket close");
  assert.equal(survived.direct, true);
  assert.equal(survived.socket, null,
    "socket nulled so routeDelivery hits the inboxStore buffering branch");
  assert.equal(rt.isLocalHosted("inbox:registered"), true,
    "isLocalHosted reflects the buffering state");

  assert.equal(rt.has("inbox:bare"), false, "non-registered route still wiped");
});

test("RouteTable removeAllForInstallerSocket preserves registered direct routes", () => {
  const rt = new RouteTable();
  const installer = { id: "owner-installer" };
  const registration = { inboxId: "inbox:reg2", claimantPublicKeyB64: "k", delegationSigB64: "s" };

  rt.addLocal("inbox:reg2", installer, { nowMs: 1, registration, installerSocket: installer });
  rt.addLocal("inbox:plain", installer, { nowMs: 2, installerSocket: installer });

  const withdrawn = rt.removeAllForInstallerSocket(installer);
  assert.deepEqual(withdrawn.sort(), ["inbox:plain"],
    "only the unregistered entry is reported withdrawn");

  const survived = rt.get("inbox:reg2");
  assert.ok(survived, "registered installer-tracked entry must survive");
  assert.equal(survived.direct, true);
  assert.equal(survived.installerSocket, null,
    "installerSocket nulled so future inbox.withdraw from any socket fails the installer check");
});

test("RouteTable re-register after disconnect overwrites the preserved entry with the live socket", () => {
  const rt = new RouteTable();
  const oldSock = { id: "old" };
  const newSock = { id: "new" };
  const registration = { inboxId: "inbox:reconnect", claimantPublicKeyB64: "k", delegationSigB64: "s" };

  rt.addLocal("inbox:reconnect", oldSock, { nowMs: 1, registration, installerSocket: oldSock });
  rt.removeAllForSocket(oldSock);
  rt.removeAllForInstallerSocket(oldSock);

  const buffering = rt.get("inbox:reconnect");
  assert.equal(buffering.socket, null);
  assert.equal(buffering.installerSocket, null);

  // Owner reconnects and re-registers; addLocal overwrites with the new socket.
  rt.addLocal("inbox:reconnect", newSock, { nowMs: 2, registration, installerSocket: newSock });

  const live = rt.get("inbox:reconnect");
  assert.equal(live.socket, newSock, "live socket restored after re-register");
  assert.equal(live.installerSocket, newSock);
  assert.equal(rt.isLocalHosted("inbox:reconnect"), false,
    "isLocalHosted is false again because socket is live; deposits flow direct, drain handles backlog");
});
