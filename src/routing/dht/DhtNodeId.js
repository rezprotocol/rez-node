import { createHash } from "node:crypto";

/**
 * 256-bit Kademlia node identifier.
 *
 * Derived from a relayKeyId via SHA-256 so every relay in the mesh maps
 * deterministically to a position in the DHT keyspace.
 */
export class DhtNodeId {
  /** @type {Uint8Array} 32 bytes */
  #bytes;

  /**
   * @param {Uint8Array} bytes - exactly 32 bytes
   */
  constructor(bytes) {
    if (!(bytes instanceof Uint8Array) || bytes.length !== 32) {
      throw new Error("DhtNodeId requires exactly 32 bytes");
    }
    this.#bytes = bytes;
  }

  /**
   * Derive a DhtNodeId from a relayKeyId string via SHA-256.
   * @param {string} relayKeyId
   * @returns {DhtNodeId}
   */
  static fromRelayKeyId(relayKeyId) {
    if (typeof relayKeyId !== "string" || relayKeyId.trim().length === 0) {
      throw new Error("DhtNodeId.fromRelayKeyId requires a non-empty string");
    }
    const hash = createHash("sha256").update(relayKeyId).digest();
    return new DhtNodeId(new Uint8Array(hash));
  }

  /**
   * Construct from raw bytes.
   * @param {Uint8Array} bytes
   * @returns {DhtNodeId}
   */
  static fromBytes(bytes) {
    return new DhtNodeId(new Uint8Array(bytes));
  }

  /**
   * Construct from hex string.
   * @param {string} hex - 64-character hex string
   * @returns {DhtNodeId}
   */
  static fromHex(hex) {
    if (typeof hex !== "string" || hex.length !== 64) {
      throw new Error("DhtNodeId.fromHex requires a 64-character hex string");
    }
    const bytes = new Uint8Array(32);
    for (let i = 0; i < 32; i += 1) {
      bytes[i] = parseInt(hex.substring(i * 2, i * 2 + 2), 16);
    }
    return new DhtNodeId(bytes);
  }

  /**
   * XOR distance between this node and another.
   * @param {DhtNodeId} other
   * @returns {Uint8Array} 32 bytes
   */
  xorDistance(other) {
    const a = this.#bytes;
    const b = other.#bytes;
    const result = new Uint8Array(32);
    for (let i = 0; i < 32; i += 1) {
      result[i] = a[i] ^ b[i];
    }
    return result;
  }

  /**
   * Bucket index = position of the highest differing bit between
   * this node and another. Returns 0-255 where 255 means the most
   * significant bit differs (maximum distance) and 0 means only the
   * least significant bit differs (minimum distance).
   *
   * Returns -1 if the IDs are identical (same node).
   *
   * @param {DhtNodeId} other
   * @returns {number}
   */
  bucketIndex(other) {
    const a = this.#bytes;
    const b = other.#bytes;
    for (let i = 0; i < 32; i += 1) {
      const xor = a[i] ^ b[i];
      if (xor === 0) continue;
      // Find position of highest set bit in this byte
      const bitPos = 7 - Math.clz32(xor) + 24; // clz32 counts from 32 bits
      return (31 - i) * 8 + bitPos;
    }
    return -1;
  }

  /**
   * Compare XOR distance from this node to two targets.
   * Returns negative if a is closer to this, positive if b is closer.
   *
   * @param {DhtNodeId} a
   * @param {DhtNodeId} b
   * @returns {number}
   */
  compareDistanceTo(a, b) {
    const selfBytes = this.#bytes;
    const aBytes = a.#bytes;
    const bBytes = b.#bytes;
    for (let i = 0; i < 32; i += 1) {
      const da = selfBytes[i] ^ aBytes[i];
      const db = selfBytes[i] ^ bBytes[i];
      if (da < db) return -1;
      if (da > db) return 1;
    }
    return 0;
  }

  /**
   * @param {DhtNodeId} other
   * @returns {boolean}
   */
  equals(other) {
    if (!(other instanceof DhtNodeId)) return false;
    const a = this.#bytes;
    const b = other.#bytes;
    for (let i = 0; i < 32; i += 1) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }

  /** @returns {Uint8Array} copy of the underlying bytes */
  get bytes() {
    return new Uint8Array(this.#bytes);
  }

  /** @returns {string} 64-character hex string */
  get hex() {
    let out = "";
    for (const b of this.#bytes) {
      out += b.toString(16).padStart(2, "0");
    }
    return out;
  }
}
