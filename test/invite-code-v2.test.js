import test from "node:test";
import assert from "node:assert/strict";
import {
  encodeInviteCodeV2,
  parseInviteCodeV2,
  isInviteCodeV2,
} from "../src/services/invites/inviteCodeV2.js";

test("encodeInviteCodeV2 produces correct format", () => {
  const code = encodeInviteCodeV2({
    inviteId: "plinv_abc123",
    creatorInboxId: "inbox:deadbeef",
  });
  assert.equal(code, "rez:inv:v2:plinv_abc123.inbox:deadbeef");
});

test("parseInviteCodeV2 extracts inviteId and creatorInboxId", () => {
  const { inviteId, creatorInboxId } = parseInviteCodeV2(
    "rez:inv:v2:plinv_abc123.inbox:deadbeef",
  );
  assert.equal(inviteId, "plinv_abc123");
  assert.equal(creatorInboxId, "inbox:deadbeef");
});

test("isInviteCodeV2 detects v2 prefix", () => {
  assert.equal(isInviteCodeV2("rez:inv:v2:plinv_abc.inbox:def"), true);
  assert.equal(isInviteCodeV2("rez:invite:v1:abc.def"), false);
  assert.equal(isInviteCodeV2(""), false);
  assert.equal(isInviteCodeV2(null), false);
  assert.equal(isInviteCodeV2(undefined), false);
  assert.equal(isInviteCodeV2(42), false);
});

test("round-trip encode → parse", () => {
  const input = { inviteId: "plinv_xyz789qwerty", creatorInboxId: "inbox:a1b2c3d4" };
  const code = encodeInviteCodeV2(input);
  const parsed = parseInviteCodeV2(code);
  assert.equal(parsed.inviteId, input.inviteId);
  assert.equal(parsed.creatorInboxId, input.creatorInboxId);
});

test("encodeInviteCodeV2 throws on missing inviteId", () => {
  assert.throws(
    () => encodeInviteCodeV2({ creatorInboxId: "inbox:abc" }),
    /inviteId/,
  );
});

test("encodeInviteCodeV2 throws on missing creatorInboxId", () => {
  assert.throws(
    () => encodeInviteCodeV2({ inviteId: "plinv_abc" }),
    /creatorInboxId/,
  );
});

test("parseInviteCodeV2 throws on wrong prefix", () => {
  assert.throws(
    () => parseInviteCodeV2("rez:invite:v1:abc.def"),
    (err) => err.code === "INVITE_V2_INVALID_FORMAT",
  );
});

test("parseInviteCodeV2 throws on no dot separator", () => {
  assert.throws(
    () => parseInviteCodeV2("rez:inv:v2:plinv_abc"),
    (err) => err.code === "INVITE_V2_INVALID_FORMAT",
  );
});

test("parseInviteCodeV2 throws on empty fields", () => {
  assert.throws(
    () => parseInviteCodeV2("rez:inv:v2:.inbox:abc"),
    (err) => err.code === "INVITE_V2_INVALID_FORMAT",
  );
  assert.throws(
    () => parseInviteCodeV2("rez:inv:v2:plinv_abc."),
    (err) => err.code === "INVITE_V2_INVALID_FORMAT",
  );
});

test("parseInviteCodeV2 trims whitespace", () => {
  const { inviteId, creatorInboxId } = parseInviteCodeV2(
    "  rez:inv:v2:plinv_abc.inbox:def  ",
  );
  assert.equal(inviteId, "plinv_abc");
  assert.equal(creatorInboxId, "inbox:def");
});

test("v2 code is much shorter than v1 codes", () => {
  const code = encodeInviteCodeV2({
    inviteId: "plinv_aBcDeFgHiJkLmNoPqRsTuVwX",
    creatorInboxId: "inbox:a1b2c3d4",
  });
  assert.ok(code.length < 80, `v2 code should be under 80 chars, got ${code.length}`);
});
