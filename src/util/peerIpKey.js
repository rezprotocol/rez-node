/**
 * Reduce a peer's remote-address string to a stable key for per-source
 * rate limiting.
 *
 * SECURITY_AUDIT MED-14: an IPv6 /128 is not a useful rate-limit key
 * because a single subscriber typically holds a /64 (or a /48). Rotating
 * the lower 64 bits is free, so we must bucket at /64 instead.
 *
 * For IPv4 we return the address as-is (a /32 is one host on the public
 * internet, the granularity we want).
 *
 * For IPv6:
 *   - Plain ::1, ::, etc. stay as-is (they collapse to the same key)
 *   - IPv4-mapped (::ffff:a.b.c.d) returns the embedded v4 address
 *   - Everything else truncates to /64 (first four 16-bit groups, expanded)
 *
 * Returns "" for unparseable input — callers must treat empty as
 * "no IP-keyed bucket," NOT as a default bucket-everyone identity.
 */
export function peerIpKey(rawAddress) {
  if (typeof rawAddress !== "string") return "";
  const addr = rawAddress.trim();
  if (!addr) return "";
  // IPv4 dotted-quad
  if (/^\d+\.\d+\.\d+\.\d+$/.test(addr)) return addr;
  // IPv4-mapped IPv6 (::ffff:a.b.c.d)
  const v4Mapped = addr.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/i);
  if (v4Mapped) return v4Mapped[1];
  // IPv6: expand and take the first four 16-bit groups (the /64 prefix).
  if (addr.includes(":")) {
    const groups = expandIpv6ToGroups(addr);
    if (groups.length < 4) return addr;
    return groups.slice(0, 4).join(":") + "::/64";
  }
  return addr;
}

/**
 * Expand any IPv6 textual form to an 8-element array of 16-bit hex
 * groups (with leading zeros). Handles `::` compression.
 *
 * Returns [] when the input is not a parseable IPv6.
 */
function expandIpv6ToGroups(raw) {
  const lower = raw.toLowerCase();
  // Split on "::" — at most one occurrence in canonical form.
  const halves = lower.split("::");
  if (halves.length > 2) return [];
  const head = halves[0] === "" ? [] : halves[0].split(":");
  const tail = halves.length === 2 && halves[1] !== "" ? halves[1].split(":") : [];
  if (halves.length === 1 && head.length !== 8) return [];
  if (head.length + tail.length > 8) return [];
  const fill = 8 - head.length - tail.length;
  const middle = halves.length === 2 ? new Array(fill).fill("0") : [];
  const full = [...head, ...middle, ...tail];
  if (full.length !== 8) return [];
  for (const g of full) {
    if (!/^[0-9a-f]{1,4}$/i.test(g)) return [];
  }
  return full.map((g) => g.padStart(4, "0"));
}
