import { CONTRACT_VERSION } from "@rezprotocol/core";

const EXAMPLE_TIME_MS = 1772496000000;

/**
 * Factory functions for test data. Each returns a valid body object
 * for the corresponding record type.
 */
export const WS_CONTRACT_EXAMPLES = Object.freeze({
  "session.hello": () => ({
    contractVersion: CONTRACT_VERSION,
    clientName: "test-client",
    clientVersion: "0.0.1",
    deviceId: "dev:test",
    accountId: "rez:test:acct:example",
    accountIdentityPublicKeyB64: "dGVzdC1wdWJsaWMta2V5",
  }),
  "session.ready": () => ({
    serverTime: EXAMPLE_TIME_MS,
    accountId: "rez:test:acct:example",
    capabilities: {
      contractVersion: CONTRACT_VERSION,
      deviceId: "dev:test",
      localInboxId: "inbox:test",
      capabilities: [],
      bootstrapRelays: [],
      bootstrapSeeds: [],
      meshMode: "seeded-gossip",
    },
  }),
  "error": () => ({
    code: "BAD_REQUEST",
    message: "invalid request",
    detail: {
      retryable: false,
      appContextId: null,
      messageId: null,
    },
  }),
  "mailbox.deposit": () => ({
    mailboxId: "inbox:test",
    objectId: "obj_test_001",
    ciphertextB64: "Y2lwaGVydGV4dA==",
    metadata: { contentType: "application/octet-stream" },
  }),
  "mailbox.deposit.res": () => ({
    mailboxId: "inbox:test",
    eventId: "evt_001",
  }),
  "mailbox.list": () => ({
    mailboxId: "inbox:test",
    limit: 50,
  }),
  "mailbox.list.res": () => ({
    mailboxId: "inbox:test",
    items: [
      {
        eventId: "evt_001",
        objectId: "obj_test_001",
        createdAtMs: EXAMPLE_TIME_MS,
      },
    ],
    nextCursor: null,
  }),
  "mailbox.fetch": () => ({
    mailboxId: "inbox:test",
    eventId: "evt_001",
  }),
  "mailbox.fetch.res": () => ({
    mailboxId: "inbox:test",
    eventId: "evt_001",
    objectId: "obj_test_001",
    ciphertextB64: "Y2lwaGVydGV4dA==",
    metadata: { contentType: "application/octet-stream" },
    createdAtMs: EXAMPLE_TIME_MS,
  }),
  "mailbox.ack": () => ({
    mailboxId: "inbox:test",
    eventId: "evt_001",
  }),
  "mailbox.ack.res": () => ({
    mailboxId: "inbox:test",
    eventId: "evt_001",
    removed: true,
  }),
  "evt.mailbox.deposited": () => ({
    mailboxId: "inbox:test",
    eventId: "evt_001",
    objectId: "obj_test_001",
    createdAtMs: EXAMPLE_TIME_MS,
  }),
  "evt.outbound.status": () => ({
    queueId: "queue_test_001",
    deliverInboxId: "inbox:test",
    status: "queued",
    attemptedAtMs: EXAMPLE_TIME_MS,
  }),
  "inbox.claim": () => ({
    inboxId: "inbox:example_random",
    claimantPublicKeyB64: "cHVibGljLWtleQ==",
    claimedAtMs: EXAMPLE_TIME_MS,
    signatureB64: "c2lnbmF0dXJl",
  }),
  "inbox.claim.res": () => ({
    inboxId: "inbox:example_random",
    claimedAtMs: EXAMPLE_TIME_MS,
  }),
  "channel.open": () => ({
    channelId: "ch_test_001",
  }),
  "channel.open.res": () => ({
    channelId: "ch_test_001",
    code: "OK",
    message: "channel opened",
  }),
  "channel.close": () => ({
    channelId: "ch_test_001",
  }),
  "channel.close.res": () => ({
    channelId: "ch_test_001",
    code: "OK",
    message: "channel closed",
  }),
  "channel.signal": () => ({
    channelId: "ch_test_001",
    signal: "offer",
    data: { sdp: "example" },
  }),
  "node.status": () => ({}),
  "node.status.res": () => ({
    accountId: "rez:test:acct:example",
    meshEnabled: true,
    meshMode: "seeded-gossip",
    peerCount: 0,
    uptimeMs: 1000,
  }),
});
