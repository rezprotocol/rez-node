import test from "node:test";
import assert from "node:assert/strict";
import {
  resolveTrustedProxyClientIp,
  validateTrustedProxyCidrs,
} from "../src/util/trustedProxyClientIp.js";

function request(remoteAddress, forwardedFor = null) {
  const headers = {};
  if (forwardedFor !== null) headers["x-forwarded-for"] = forwardedFor;
  return { socket: { remoteAddress }, headers };
}

test("untrusted peers cannot spoof X-Forwarded-For", () => {
  const ip = resolveTrustedProxyClientIp({
    request: request("203.0.113.9", "198.51.100.7"),
    trustedProxyCidrs: ["10.0.0.0/8"],
  });
  assert.equal(ip, "203.0.113.9");
});

test("trusted proxy chain resolves the first external client", () => {
  const ip = resolveTrustedProxyClientIp({
    request: request("172.20.0.4", "198.51.100.7, 172.20.0.3"),
    trustedProxyCidrs: ["172.16.0.0/12"],
  });
  assert.equal(ip, "198.51.100.7");
});

test("trusted proxy parsing preserves the IPv6 /64 rate-limit policy", () => {
  const ip = resolveTrustedProxyClientIp({
    request: request("::1", "2001:db8:abcd:12::99"),
    trustedProxyCidrs: ["::1/128"],
  });
  assert.equal(ip, "2001:0db8:abcd:0012::/64");
});

test("malformed forwarding chains fail back to the trusted socket address", () => {
  const ip = resolveTrustedProxyClientIp({
    request: request("172.20.0.4", "attacker-controlled"),
    trustedProxyCidrs: ["172.16.0.0/12"],
  });
  assert.equal(ip, "172.20.0.4");
});

test("trusted proxy configuration fails closed on malformed entries", () => {
  assert.deepEqual(validateTrustedProxyCidrs(["172.16.0.0/12", "172.16.0.0/12"]), ["172.16.0.0/12"]);
  assert.throws(() => validateTrustedProxyCidrs(["everywhere"]), /valid IP\/CIDR/);
  assert.throws(() => validateTrustedProxyCidrs("172.16.0.0/12"), /requires array/);
});
