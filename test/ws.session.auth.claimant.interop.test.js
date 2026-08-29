import test from "node:test";
import assert from "node:assert/strict";
import nodeCrypto, { randomBytes } from "node:crypto";
import { WebSocket } from "ws";
import {
  RMailbox,
  MemoryDataStore,
  MemoryStorageProvider,
  createDefaultRegistry,
  REZ_CONTRACT_TYPES,
  canonicalJSONStringify,
  relayKeyIdForNodePublicKeyB64,
  nodeKeyIdForNodePublicKeyB64,
} from "@rezprotocol/core";
import { UplinkPoolClient } from "@rezprotocol/sdk";
import { WsGatewayServer } from "../src/ws/WsGatewayServer.js";
import { PerAccountServiceCache } from "../src/ws/PerAccountServiceCache.js";
import { InboxClaimRegistry } from "../src/inbox/InboxClaimRegistry.js";
import { DepositPolicyStore } from "../src/inbox/DepositPolicyStore.js";
import { NodeCryptoProvider } from "../src/crypto/NodeCryptoProvider.js";
import {
  createServerServices,
  createPerAccountServices,
  createProtocolFactory,
  createDepositHandler,
} from "./helpers/nodeTestServices.js";
import { createNodeTestIdentity } from "./helpers/wsAuth.js";

// SESSION_AUTH_V5 slice 2 follow-up: the CROSS-REPO claimant interop test.
// The two wire halves are implemented independently (rez-sdk AuthStateMachine
// / rez-node GatewaySession) and unit-tested against mirrored payload shapes;
// this test proves the real halves agree on BYTES AND SEQUENCING — encoding,
// field presence/absence, domain-separated payload construction, version
// propagation, event order — by driving the real UplinkPoolClient in claimant
// mode through a real WsGatewayServer over a real loopback WebSocket.
//
// Scope is deliberately narrow (the proof matrix lives in the unit /
// handler-level suites): one happy path — v5 CLAIMANT handshake → claim KA →
// mailbox authorize — plus the cheap negative siblings: claim KB, an ACCOUNT
// op, and a re-auth attempt.

const T = REZ_CONTRACT_TYPES;
const CRYPTO = new NodeCryptoProvider();

// SDK-format Ed25519 keys (SPKI/PKCS8 DER, base64) — the same shape a real
// client holds; the node verifies against exactly these bytes.
function genSdkKey() {
  const { publicKey, privateKey } = nodeCrypto.generateKeyPairSync("ed25519");
  return {
    publicKeyB64: Buffer.from(publicKey.export({ format: "der", type: "spki" })).toString("base64"),
    privateKeyB64: Buffer.from(privateKey.export({ format: "der", type: "pkcs8" })).toString("base64"),
  };
}

function signWithSdkKey(privateKeyB64, payload) {
  const key = nodeCrypto.createPrivateKey({ key: Buffer.from(privateKeyB64, "base64"), format: "der", type: "pkcs8" });
  const msg = Buffer.from(new TextEncoder().encode(canonicalJSONStringify(payload)));
  return Buffer.from(nodeCrypto.sign(null, msg, key)).toString("base64");
}

function buildClaimBody({ inboxId, claimant, nodeIdentity, claimedAtMs = Date.now() }) {
  const signatureB64 = signWithSdkKey(claimant.privateKeyB64, {
    inboxId,
    claimantPublicKeyB64: claimant.publicKeyB64,
    claimedAtMs,
  });
  const issuedAtMs = Date.now();
  const expiresAtMs = issuedAtMs + 7 * 24 * 60 * 60 * 1000;
  const delegationSigB64 = signWithSdkKey(claimant.privateKeyB64, {
    kind: "inbox-node-delegation",
    inboxId,
    claimantPublicKeyB64: claimant.publicKeyB64,
    nodeKeyId: nodeIdentity.nodeKeyId,
    nodePublicKeyB64: nodeIdentity.nodePublicKeyB64,
    relayKeyId: nodeIdentity.relayKeyId,
    issuedAtMs,
    expiresAtMs,
  });
  return {
    inboxId,
    claimantPublicKeyB64: claimant.publicKeyB64,
    claimedAtMs,
    signatureB64,
    nodeDelegation: {
      nodeKeyId: nodeIdentity.nodeKeyId,
      nodePublicKeyB64: nodeIdentity.nodePublicKeyB64,
      relayKeyId: nodeIdentity.relayKeyId,
      issuedAtMs,
      expiresAtMs,
      delegationSigB64,
    },
  };
}

async function startNode(t) {
  const storageProvider = new MemoryStorageProvider();
  const identity = createNodeTestIdentity({
    accountId: "rez:node:claimant-interop:" + randomBytes(4).toString("hex"),
    deviceId: "dev:test",
    localInboxId: "inbox:test",
  });
  // The REAL SDK enforces ADR-RELAY-IDENTITY: relayKeyId/nodeKeyId must be the
  // self-certifying derivations of the node key — a free-string relay id (the
  // default harness shape, fine for raw-socket tests) is refused by the live
  // client. Interop means satisfying the client's actual validation.
  identity.nodeKeyId = nodeKeyIdForNodePublicKeyB64(identity.nodePublicKeyB64);
  identity.relayKeyId = relayKeyIdForNodePublicKeyB64(identity.nodePublicKeyB64);
  const inboxClaimRegistry = new InboxClaimRegistry({ storageProvider });
  await inboxClaimRegistry.hydrate();
  const depositPolicyStore = new DepositPolicyStore({ storageProvider });
  await depositPolicyStore.hydrate();
  const runtime = {
    depositPolicyStore,
    inboxStore: new RMailbox({ store: new MemoryDataStore(), registry: createDefaultRegistry() }),
    relayStore: null,
    metrics: null,
    inboxClaimRegistry,
    accountAuthorityRevocationCache: null,
    accountDeviceRegistry: {
      async isTerminallyRevoked() { return false; },
      async isTerminallyRevokedInTx() { return false; },
    },
    serverServices: createServerServices({ storageProvider, clock: () => Date.now(), ownerAccountId: identity.accountId }),
    serviceCache: new PerAccountServiceCache({ storageProvider, clock: () => Date.now(), createServices: createPerAccountServices }),
    getIdentity() {
      return { ...identity };
    },
    getMeshStatus() {
      return { enabled: true, mode: "seeded-gossip", participateInRouting: true, peerCount: 0 };
    },
    async stop() {},
  };
  const server = new WsGatewayServer({
    runtime,
    port: 0,
    protocolFactory: createProtocolFactory(),
    onInboundDeposit: createDepositHandler({ crypto: CRYPTO }),
  });
  await server.start();
  t.after(() => server.stop());
  return { server, identity };
}

test("real SDK claimant client ↔ real GatewaySession: v5 handshake, KA claim + mailbox authorize; KB claim / account op / re-auth all refused", async (t) => {
  const { server, identity } = await startNode(t);
  const address = server.address();
  const claimant = genSdkKey();

  // Count every session.hello the client EVER puts on the wire — the
  // no-oracle proof below asserts the count never grows after the initial
  // handshake (no identity-disclosing re-auth is ever attempted by transport
  // code on its own).
  let helloFramesSent = 0;
  const countingWsFactory = (url) => {
    const ws = new WebSocket(url);
    const realSend = ws.send.bind(ws);
    ws.send = (data, ...rest) => {
      try {
        const frame = JSON.parse(String(data));
        const type = String(frame.t || frame.type || "");
        if (type === T.SESSION_HELLO) helloFramesSent += 1;
      } catch { /* non-JSON frame: not a hello */ }
      return realSend(data, ...rest);
    };
    return ws;
  };

  const client = new UplinkPoolClient({
    uplinks: ["ws://127.0.0.1:" + address.port + "/ws"],
    claimantIdentity: {
      claimantPublicKeyB64: claimant.publicKeyB64,
      privateKeyB64: claimant.privateKeyB64,
    },
    wsFactory: countingWsFactory,
    warmSpareCount: 0,
  });
  t.after(async () => {
    try { await client.close(); } catch { /* socket may already be closed by the re-auth 1008 */ }
  });

  // v5 CLAIMANT handshake completes over the real wire.
  await client.connect();

  // Claim an inbox rooted at the SESSION's claimant key — accepted, and the
  // ready/claim sequencing means mailbox scope is now proof-backed.
  const inboxId = "inbox:interop-ka";
  const claimRes = await client.request(T.INBOX_CLAIM, buildClaimBody({ inboxId, claimant, nodeIdentity: identity }));
  assert.ok(claimRes, "claim answered");
  assert.equal(claimRes.inboxId, inboxId);

  // Mailbox authorize via the session binding: the bound inbox lists…
  const listRes = await client.request(T.MAILBOX_LIST, { mailboxId: inboxId });
  assert.ok(listRes, "bound inbox is readable through the claimant session");

  // …an unbound inbox does not (KA's proof grants nothing else).
  await assert.rejects(
    () => client.request(T.MAILBOX_LIST, { mailboxId: "inbox:never-claimed" }),
    (err) => err && err.code === "FORBIDDEN",
    "no implicit mailbox scope",
  );

  // ---- SESSION_AUTH_V5 slice 3: the NO-ORACLE proof ----
  // The recipient (this same claimant, KA) publishes an identity-bearing
  // deposit policy on its inbox; a claimant DEPOSIT against it must come back
  // as the explicit incompatibility — an ANSWER, not an authentication
  // transition.
  const hellosAfterHandshake = helloFramesSent;
  assert.ok(hellosAfterHandshake >= 1, "the handshake sent its hello");
  {
    const issuedAtMs = Date.now();
    const expiresAtMs = issuedAtMs + 60 * 60 * 1000;
    const allowed = ["some-account-pubkey-b64"];
    const policySigB64 = signWithSdkKey(claimant.privateKeyB64, {
      kind: "inbox-deposit-policy",
      inboxId,
      policyVersion: 1,
      blockedDepositorPubkeys: [],
      allowedDepositorPubkeys: [...allowed].sort(),
      issuedAtMs,
      expiresAtMs,
    });
    await client.request(T.INBOX_SET_DEPOSIT_POLICY, {
      policy: {
        v: 1,
        inboxId,
        policyVersion: 1,
        blockedDepositorPubkeys: [],
        allowedDepositorPubkeys: allowed,
        issuedAtMs,
        expiresAtMs,
        claimantPublicKeyB64: claimant.publicKeyB64,
        signatureB64: policySigB64,
      },
    });

    await assert.rejects(
      () => client.request(T.MAILBOX_DEPOSIT, { mailboxId: inboxId, ciphertextB64: "AQ==" }),
      (err) => err && err.code === "DEPOSITOR_IDENTITY_REQUIRED",
      "the incompatibility is explicit — no silent allow, no silent deny",
    );

    assert.equal(helloFramesSent, hellosAfterHandshake, "NO new session.hello left the client — no identity-disclosure oracle");
    const stillWorking = await client.request(T.MAILBOX_LIST, { mailboxId: inboxId });
    assert.ok(stillWorking, "the session stayed CLAIMANT, open, and fully functional — the refusal was an answer, not a close");
  }

  // A claim rooted at a DIFFERENT claimant key can never co-reside on this session.
  const kb = genSdkKey();
  await assert.rejects(
    () => client.request(T.INBOX_CLAIM, buildClaimBody({ inboxId: "inbox:interop-kb", claimant: kb, nodeIdentity: identity })),
    (err) => err && err.code === "FORBIDDEN",
    "one session, one claimant root",
  );

  // ACCOUNT-classified operations are structurally out of reach.
  await assert.rejects(
    () => client.request(T.ACCOUNT_AUTHORITY_STATE_GET, {}),
    (err) => err && err.code === "FORBIDDEN",
    "account control plane refused for a claimant principal",
  );

  // LAST (the node closes the socket 1008 afterwards): a further hello on the
  // committed v5 session is a protocol-state violation.
  await assert.rejects(
    () => client.request(T.SESSION_HELLO, {
      contractVersion: 5,
      authMode: "claimant",
      claimantPublicKeyB64: claimant.publicKeyB64,
    }),
    (err) => err && err.code === "ALREADY_AUTHENTICATED",
    "re-auth on a committed v5 session is ALREADY_AUTHENTICATED",
  );
});
