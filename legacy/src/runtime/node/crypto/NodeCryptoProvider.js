import crypto from "node:crypto";
import CryptoProvider from "../../../core/crypto/CryptoProvider.js";

function toBuf(x) {
  if (x == null) return Buffer.alloc(0);
  if (typeof x === "string") return Buffer.from(x, "utf8");
  return Buffer.from(x);
}

/**
 * NodeCryptoProvider
 *
 * Node.js implementation using node:crypto.
 * Keys are encoded as DER:
 * - publicKey: SPKI DER
 * - privateKey: PKCS8 DER
 */
export default class NodeCryptoProvider extends CryptoProvider {
  async init() {
    this._initialized = true;
  }

  capabilities() {
    return {
      x25519: true,
      hkdf: true,
      pbkdf2: true,
      scrypt: true,
      aes256gcm: true,
      ed25519: true,
      p256: true,
    };
  }

  randomBytes(length) {
    return new Uint8Array(crypto.randomBytes(length));
  }

  async hkdf(ikm, length, salt, info) {
    const out = crypto.hkdfSync("sha256", Buffer.from(ikm), toBuf(salt), toBuf(info), length);
    return new Uint8Array(out);
  }

  async pbkdf2(password, salt, iterations, length, hash = "SHA-256") {
    const digest = hash.toLowerCase().replace("-", "");
    const pwBuf = typeof password === "string" ? Buffer.from(password, "utf8") : Buffer.from(password);
    const out = crypto.pbkdf2Sync(pwBuf, Buffer.from(salt), iterations, length, digest);
    return new Uint8Array(out);
  }

  async aesGcmEncrypt(plaintext, key, aad = null) {
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv("aes-256-gcm", Buffer.from(key), iv);
    if (aad) cipher.setAAD(Buffer.from(aad));
    const ptBuf = typeof plaintext === "string" ? Buffer.from(plaintext, "utf8") : Buffer.from(plaintext);
    const ciphertext = Buffer.concat([cipher.update(ptBuf), cipher.final()]);
    const authTag = cipher.getAuthTag();
    return { ciphertext: new Uint8Array(ciphertext), iv: new Uint8Array(iv), authTag: new Uint8Array(authTag) };
  }

  async aesGcmDecrypt(ciphertext, key, iv, authTag, aad = null) {
    const decipher = crypto.createDecipheriv("aes-256-gcm", Buffer.from(key), Buffer.from(iv));
    if (aad) decipher.setAAD(Buffer.from(aad));
    decipher.setAuthTag(Buffer.from(authTag));
    const pt = Buffer.concat([decipher.update(Buffer.from(ciphertext)), decipher.final()]);
    return new Uint8Array(pt);
  }

  async generateX25519KeyPair() {
    const { publicKey, privateKey } = crypto.generateKeyPairSync("x25519", {
      publicKeyEncoding: { format: "der", type: "spki" },
      privateKeyEncoding: { format: "der", type: "pkcs8" },
    });
    return { publicKey: new Uint8Array(publicKey), privateKey: new Uint8Array(privateKey) };
  }

  async x25519(privateKeyDer, publicKeyDer) {
    const privateKey = crypto.createPrivateKey({ key: Buffer.from(privateKeyDer), format: "der", type: "pkcs8" });
    const publicKey = crypto.createPublicKey({ key: Buffer.from(publicKeyDer), format: "der", type: "spki" });
    const secret = crypto.diffieHellman({ privateKey, publicKey });
    return new Uint8Array(secret);
  }

  async generateSigningKeyPair(algorithmId) {
    if (algorithmId === "ed25519") {
      const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519", {
        publicKeyEncoding: { format: "der", type: "spki" },
        privateKeyEncoding: { format: "der", type: "pkcs8" },
      });
      return { publicKey: new Uint8Array(publicKey), privateKey: new Uint8Array(privateKey) };
    }
    if (algorithmId === "p256") {
      const { publicKey, privateKey } = crypto.generateKeyPairSync("ec", {
        namedCurve: "prime256v1",
        publicKeyEncoding: { format: "der", type: "spki" },
        privateKeyEncoding: { format: "der", type: "pkcs8" },
      });
      return { publicKey: new Uint8Array(publicKey), privateKey: new Uint8Array(privateKey) };
    }
    throw new Error(`Unsupported signing algorithmId: ${algorithmId}`);
  }

  async sign(bytes, privateKeyDer, algorithmId) {
    const key = crypto.createPrivateKey({ key: Buffer.from(privateKeyDer), format: "der", type: "pkcs8" });
    const data = Buffer.from(bytes);
    if (algorithmId === "ed25519") {
      const sig = crypto.sign(null, data, key);
      return new Uint8Array(sig);
    }
    if (algorithmId === "p256") {
      const sig = crypto.sign("sha256", data, key);
      return new Uint8Array(sig);
    }
    throw new Error(`Unsupported signing algorithmId: ${algorithmId}`);
  }

  async sha256(bytes) {
    const buf = typeof bytes === "string" ? Buffer.from(bytes, "utf8") : Buffer.from(bytes);
    return new Uint8Array(crypto.createHash("sha256").update(buf).digest());
  }

  async verify(bytes, signature, publicKeyDer, algorithmId) {
    const key = crypto.createPublicKey({ key: Buffer.from(publicKeyDer), format: "der", type: "spki" });
    const data = Buffer.from(bytes);
    const sig = Buffer.from(signature);
    if (algorithmId === "ed25519") {
      return crypto.verify(null, data, key, sig);
    }
    if (algorithmId === "p256") {
      return crypto.verify("sha256", data, key, sig);
    }
    throw new Error(`Unsupported signing algorithmId: ${algorithmId}`);
  }

  async scrypt(password, salt, cost, blockSize, parallelism, length) {
    const pwBuf = typeof password === "string" ? Buffer.from(password, "utf8") : Buffer.from(password);
    return new Promise((resolve, reject) => {
      crypto.scrypt(pwBuf, Buffer.from(salt), length, { N: cost, r: blockSize, p: parallelism }, (err, derivedKey) => {
        if (err) reject(err);
        else resolve(new Uint8Array(derivedKey));
      });
    });
  }
}
