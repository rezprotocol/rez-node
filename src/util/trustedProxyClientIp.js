import net from "node:net";
import { peerIpKey } from "./peerIpKey.js";

function normalizeIp(raw) {
  if (typeof raw !== "string") return "";
  let value = raw.trim();
  if (!value) return "";
  if (value.startsWith("[")) {
    const end = value.indexOf("]");
    if (end < 0) return "";
    value = value.slice(1, end);
  } else if (net.isIP(value) === 0 && /^\d+\.\d+\.\d+\.\d+:\d+$/.test(value)) {
    value = value.slice(0, value.lastIndexOf(":"));
  }
  const zone = value.indexOf("%");
  if (zone >= 0) value = value.slice(0, zone);
  return net.isIP(value) === 0 ? "" : value.toLowerCase();
}

function ipv4Value(ip) {
  const parts = ip.split(".");
  let value = 0n;
  for (const part of parts) {
    const octet = Number(part);
    if (!Number.isInteger(octet) || octet < 0 || octet > 255) return null;
    value = (value << 8n) | BigInt(octet);
  }
  return value;
}

function ipv6Value(ip) {
  let input = ip;
  if (input.includes(".")) {
    const colon = input.lastIndexOf(":");
    const v4 = ipv4Value(input.slice(colon + 1));
    if (colon < 0 || v4 === null) return null;
    const hi = ((v4 >> 16n) & 0xffffn).toString(16);
    const lo = (v4 & 0xffffn).toString(16);
    input = input.slice(0, colon) + ":" + hi + ":" + lo;
  }
  const halves = input.split("::");
  if (halves.length > 2) return null;
  const head = halves[0] ? halves[0].split(":") : [];
  const tail = halves.length === 2 && halves[1] ? halves[1].split(":") : [];
  const fill = halves.length === 2 ? 8 - head.length - tail.length : 0;
  if (fill < 0 || (halves.length === 1 && head.length !== 8)) return null;
  const groups = [...head, ...new Array(fill).fill("0"), ...tail];
  if (groups.length !== 8) return null;
  let value = 0n;
  for (const group of groups) {
    if (!/^[0-9a-f]{1,4}$/i.test(group)) return null;
    value = (value << 16n) | BigInt("0x" + group);
  }
  return value;
}

function parseCidr(raw) {
  if (typeof raw !== "string") return null;
  const parts = raw.trim().split("/");
  const ip = normalizeIp(parts[0]);
  const version = net.isIP(ip);
  if (version === 0 || parts.length > 2) return null;
  const bits = version === 4 ? 32 : 128;
  const prefix = parts.length === 2 ? Number(parts[1]) : bits;
  if (!Number.isInteger(prefix) || prefix < 0 || prefix > bits) return null;
  const value = version === 4 ? ipv4Value(ip) : ipv6Value(ip);
  if (value === null) return null;
  return { version, bits, prefix, value };
}

function matchesCidr(ip, rawCidr) {
  const candidate = normalizeIp(ip);
  const version = net.isIP(candidate);
  const cidr = parseCidr(rawCidr);
  if (!cidr || version !== cidr.version) return false;
  const value = version === 4 ? ipv4Value(candidate) : ipv6Value(candidate);
  if (value === null) return false;
  const shift = BigInt(cidr.bits - cidr.prefix);
  return (value >> shift) === (cidr.value >> shift);
}

export function validateTrustedProxyCidrs(value) {
  if (value === undefined) return [];
  if (!Array.isArray(value)) {
    throw new Error("rez-node requires array config.node.ws.trustedProxyCidrs when provided");
  }
  const out = [];
  for (const entry of value) {
    const text = typeof entry === "string" ? entry.trim() : "";
    if (!text || !parseCidr(text)) {
      throw new Error("rez-node requires valid IP/CIDR entries in config.node.ws.trustedProxyCidrs");
    }
    if (!out.includes(text)) out.push(text);
  }
  return out;
}

/**
 * Resolve the public client address without trusting an attacker-supplied forwarding header.
 * X-Forwarded-For is consulted only when the immediate socket peer is explicitly trusted, then
 * walked right-to-left so every trusted proxy hop is removed before selecting the first external
 * address. The returned value is already normalized for the node's IPv4/IPv6 rate-limit buckets.
 */
export function resolveTrustedProxyClientIp({ request, trustedProxyCidrs = [] } = {}) {
  const socketIp = normalizeIp(request && request.socket ? request.socket.remoteAddress : "");
  if (!socketIp) return "";
  const trusted = (ip) => trustedProxyCidrs.some((cidr) => matchesCidr(ip, cidr));
  if (!trusted(socketIp)) return peerIpKey(socketIp);

  const header = request && request.headers ? request.headers["x-forwarded-for"] : null;
  if (typeof header !== "string" || header.trim() === "") return peerIpKey(socketIp);
  const forwarded = header.split(",").map(normalizeIp);
  if (forwarded.length === 0 || forwarded.some((ip) => ip === "")) {
    return peerIpKey(socketIp);
  }

  let candidate = socketIp;
  for (let i = forwarded.length - 1; i >= 0; i -= 1) {
    if (!trusted(candidate)) return peerIpKey(candidate);
    candidate = forwarded[i];
  }
  return peerIpKey(candidate);
}
