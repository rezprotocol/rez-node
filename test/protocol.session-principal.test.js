import test from "node:test";
import assert from "node:assert/strict";

import { SessionPrincipal } from "../src/protocol/SessionPrincipal.js";
import { AuthorityRequirement } from "../src/protocol/AuthorityRequirement.js";

const OWNER = "owner-pub-b64";
const DEVICE = "rez:dev:" + "a".repeat(64);
const CLAIMANT_KEY = "claimant-pub-b64";

function directAuthority(owner = OWNER) {
  return { mode: "direct", accountIdentityPublicKeyB64: owner, signerPublicKeyB64: owner };
}

function delegatedAuthority(owner = OWNER) {
  return {
    mode: "delegated",
    accountIdentityPublicKeyB64: owner,
    signerPublicKeyB64: "device-pub-b64",
    leafCertId: "cert-1",
    grantedCapabilities: ["deviceSet.publish"],
    certChain: [{ certId: "cert-1" }],
  };
}

// ---- Constructor rejections: every invalid shape throws, nothing partial ----

test("ACCOUNT construction rejects every missing/invalid required field", () => {
  const good = { kind: "ACCOUNT", accountPublicKeyB64: OWNER, sessionDeviceId: DEVICE, authority: directAuthority() };
  assert.ok(new SessionPrincipal(good) instanceof SessionPrincipal, "the good shape constructs");

  for (const [label, bad] of [
    ["no kind", { ...good, kind: undefined }],
    ["unknown kind", { ...good, kind: "SUPERUSER" }],
    ["missing account key", { ...good, accountPublicKeyB64: undefined }],
    ["empty account key", { ...good, accountPublicKeyB64: "  " }],
    ["non-string account key", { ...good, accountPublicKeyB64: 42 }],
    ["missing device id", { ...good, sessionDeviceId: undefined }],
    ["empty device id", { ...good, sessionDeviceId: "" }],
    ["missing authority", { ...good, authority: undefined }],
    ["non-object authority", { ...good, authority: "direct" }],
    ["unknown authority mode", { ...good, authority: { mode: "root" } }],
    ["ACCOUNT carrying a claimant key", { ...good, claimantPublicKeyB64: CLAIMANT_KEY }],
  ]) {
    assert.throws(() => new SessionPrincipal(bad), Error, label + " must throw");
  }
});

test("CLAIMANT construction rejects account fields and missing/empty claimant key", () => {
  const good = { kind: "CLAIMANT", claimantPublicKeyB64: CLAIMANT_KEY };
  assert.ok(new SessionPrincipal(good) instanceof SessionPrincipal);

  for (const [label, bad] of [
    ["missing claimant key", { kind: "CLAIMANT" }],
    ["empty claimant key", { kind: "CLAIMANT", claimantPublicKeyB64: " " }],
    ["claimant carrying an account key", { ...good, accountPublicKeyB64: OWNER }],
    ["claimant carrying a device id", { ...good, sessionDeviceId: DEVICE }],
    ["claimant carrying an authority", { ...good, authority: directAuthority() }],
  ]) {
    assert.throws(() => new SessionPrincipal(bad), Error, label + " must throw");
  }
});

// ---- Immutability: the principal and its authority are frozen ----

test("a constructed principal is frozen — mutation throws, never silently succeeds", () => {
  const p = SessionPrincipal.accountDirect({ accountPublicKeyB64: OWNER, sessionDeviceId: DEVICE, authority: directAuthority() });
  assert.ok(Object.isFrozen(p));
  assert.throws(() => { p.kind = "CLAIMANT"; }, TypeError, "kind is immutable");
  assert.throws(() => { p.accountPublicKeyB64 = "other"; }, TypeError, "identity key is immutable");
  assert.throws(() => { p.elevate = () => {}; }, TypeError, "no method can be grafted on (no session.elevate, ever)");
});

test("the delegated authority (and its grant/chain arrays) is deep-frozen at construction (leaf-3c F2)", () => {
  const authority = delegatedAuthority();
  const p = SessionPrincipal.accountDelegated({ accountPublicKeyB64: OWNER, sessionDeviceId: DEVICE, authority });
  assert.ok(Object.isFrozen(p.authority));
  assert.ok(Object.isFrozen(p.authority.grantedCapabilities));
  assert.ok(Object.isFrozen(p.authority.certChain));
  assert.throws(() => { p.authority.grantedCapabilities.push("device.add"); }, TypeError, "grants cannot grow after admission");
});

// ---- Factories produce the declared shapes and enforce their mode ----

test("factories construct their declared kind and refuse a mismatched authority mode", () => {
  const direct = SessionPrincipal.accountDirect({ accountPublicKeyB64: OWNER, sessionDeviceId: DEVICE, authority: directAuthority() });
  assert.equal(direct.kind, SessionPrincipal.KINDS.ACCOUNT);
  assert.equal(direct.isAccount(), true);
  assert.equal(direct.isClaimant(), false);
  assert.equal(direct.claimantPublicKeyB64, null);

  const delegated = SessionPrincipal.accountDelegated({ accountPublicKeyB64: OWNER, sessionDeviceId: DEVICE, authority: delegatedAuthority() });
  assert.equal(delegated.authority.mode, "delegated");

  const claimant = SessionPrincipal.claimant({ claimantPublicKeyB64: CLAIMANT_KEY });
  assert.equal(claimant.kind, SessionPrincipal.KINDS.CLAIMANT);
  assert.equal(claimant.isClaimant(), true);
  assert.equal(claimant.accountPublicKeyB64, null);
  assert.equal(claimant.authority, null);

  assert.throws(() => SessionPrincipal.accountDirect({ accountPublicKeyB64: OWNER, sessionDeviceId: DEVICE, authority: delegatedAuthority() }));
  assert.throws(() => SessionPrincipal.accountDelegated({ accountPublicKeyB64: OWNER, sessionDeviceId: DEVICE, authority: directAuthority() }));
});

// ---- admitsClaimantBinding truth table (Phase 0 §3 cardinality invariant) ----

test("ACCOUNT admits any well-formed claimant binding (legacy v4 multi-key claim preserved this slice)", () => {
  const p = SessionPrincipal.accountDirect({ accountPublicKeyB64: OWNER, sessionDeviceId: DEVICE, authority: directAuthority() });
  assert.equal(p.admitsClaimantBinding("K-alpha"), true);
  assert.equal(p.admitsClaimantBinding("K-beta"), true);
  assert.equal(p.admitsClaimantBinding(""), false, "an empty key is never a binding");
  assert.equal(p.admitsClaimantBinding(null), false);
});

test("CLAIMANT admits ONLY its own trust root — one session, one claimant root", () => {
  const p = SessionPrincipal.claimant({ claimantPublicKeyB64: "KA" });
  assert.equal(p.admitsClaimantBinding("KA"), true);
  assert.equal(p.admitsClaimantBinding("KB"), false, "an unrelated claimant root is rejected (F1b co-residency)");
  assert.equal(p.admitsClaimantBinding(""), false);
});

// ---- AuthorityRequirement admission ----

test("AuthorityRequirement admits by principal class, and only SessionPrincipal instances", () => {
  const account = SessionPrincipal.accountDirect({ accountPublicKeyB64: OWNER, sessionDeviceId: DEVICE, authority: directAuthority() });
  const claimant = SessionPrincipal.claimant({ claimantPublicKeyB64: "KA" });

  assert.equal(AuthorityRequirement.admits(AuthorityRequirement.ACCOUNT, account), true);
  assert.equal(AuthorityRequirement.admits(AuthorityRequirement.ACCOUNT, claimant), false, "no CLAIMANT < ACCOUNT hierarchy");
  assert.equal(AuthorityRequirement.admits(AuthorityRequirement.ANY_PRINCIPAL, account), true);
  assert.equal(AuthorityRequirement.admits(AuthorityRequirement.ANY_PRINCIPAL, claimant), true);
  assert.equal(AuthorityRequirement.admits(AuthorityRequirement.ANY_PRINCIPAL, null), false, "no principal is never admitted");
  assert.equal(
    AuthorityRequirement.admits(AuthorityRequirement.ANY_PRINCIPAL, { kind: "ACCOUNT" }),
    false,
    "a duck-typed principal-shaped object is not a principal",
  );
  assert.equal(AuthorityRequirement.isValid("ACCOUNT"), true);
  assert.equal(AuthorityRequirement.isValid("ANY_PRINCIPAL"), true);
  assert.equal(AuthorityRequirement.isValid("CLAIMANT"), false, "no claimant-only requirement exists in slice 1");
  assert.equal(AuthorityRequirement.isValid(undefined), false);
});
