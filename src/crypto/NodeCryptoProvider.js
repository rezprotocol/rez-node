import { createCipheriv, createDecipheriv, createHash, createHmac, createPrivateKey, createPublicKey, diffieHellman, generateKeyPairSync, randomBytes as nodeRandomBytes, sign as nodeSign, verify as nodeVerify } from "node:crypto";
import { RCryptoProvider, isBytes } from "@rezprotocol/core";
import { concatBytes } from "./util/bytes.js";

const AES_GCM_TAG_BYTES = 16;

function hmacSha256(key, data) {
  return new Uint8Array(createHmac("sha256", key).update(data).digest());
}

export class NodeCryptoProvider extends RCryptoProvider {
  randomBytes(len) {
    if (!Number.isInteger(len) || len <= 0) {
      throw new Error("NodeCryptoProvider.randomBytes(len) requires positive integer");
    }
    return new Uint8Array(nodeRandomBytes(len));
  }

  hashSha256(bytes) {
    if (!isBytes(bytes)) {
      throw new Error("NodeCryptoProvider.hashSha256(bytes) requires Uint8Array");
    }
    return new Uint8Array(createHash("sha256").update(bytes).digest());
  }

  hkdfSha256(ikm, { salt = new Uint8Array(0), info = new Uint8Array(0), length = 32 } = {}) {
    if (!isBytes(ikm) || !isBytes(salt) || !isBytes(info)) {
      throw new Error("NodeCryptoProvider.hkdfSha256 requires Uint8Array inputs");
    }
    if (!Number.isInteger(length) || length <= 0) {
      throw new Error("NodeCryptoProvider.hkdfSha256 length must be positive integer");
    }

    const prk = hmacSha256(salt.length ? salt : new Uint8Array(32), ikm);
    const out = new Uint8Array(length);
    let t = new Uint8Array(0);
    let offset = 0;
    let counter = 1;

    while (offset < length) {
      const block = hmacSha256(prk, concatBytes(t, info, new Uint8Array([counter])));
      const chunk = block.subarray(0, Math.min(block.length, length - offset));
      out.set(chunk, offset);
      offset += chunk.length;
      t = block;
      counter += 1;
    }

    return out;
  }

  aeadEncrypt({ key, nonce, plaintext, aad } = {}) {
    if (!isBytes(key) || key.length !== 32) {
      throw new Error("NodeCryptoProvider.aeadEncrypt requires 32-byte key");
    }
    if (!isBytes(nonce) || nonce.length !== 12) {
      throw new Error("NodeCryptoProvider.aeadEncrypt requires 12-byte nonce");
    }
    if (!isBytes(plaintext)) {
      throw new Error("NodeCryptoProvider.aeadEncrypt requires plaintext Uint8Array");
    }
    if (!isBytes(aad)) {
      throw new Error("NodeCryptoProvider.aeadEncrypt requires aad Uint8Array");
    }

    const cipher = createCipheriv("aes-256-gcm", key, nonce);
    cipher.setAAD(aad);
    const c1 = cipher.update(plaintext);
    const c2 = cipher.final();
    const tag = cipher.getAuthTag();
    return new Uint8Array(Buffer.concat([c1, c2, tag]));
  }

  aeadDecrypt({ key, nonce, ciphertext, aad } = {}) {
    if (!isBytes(key) || key.length !== 32) {
      throw new Error("NodeCryptoProvider.aeadDecrypt requires 32-byte key");
    }
    if (!isBytes(nonce) || nonce.length !== 12) {
      throw new Error("NodeCryptoProvider.aeadDecrypt requires 12-byte nonce");
    }
    if (!isBytes(ciphertext) || ciphertext.length < AES_GCM_TAG_BYTES) {
      throw new Error("NodeCryptoProvider.aeadDecrypt requires ciphertext+tag Uint8Array");
    }
    if (!isBytes(aad)) {
      throw new Error("NodeCryptoProvider.aeadDecrypt requires aad Uint8Array");
    }

    const tag = ciphertext.subarray(ciphertext.length - AES_GCM_TAG_BYTES);
    const body = ciphertext.subarray(0, ciphertext.length - AES_GCM_TAG_BYTES);

    const decipher = createDecipheriv("aes-256-gcm", key, nonce);
    decipher.setAAD(aad);
    decipher.setAuthTag(tag);
    const p1 = decipher.update(body);
    const p2 = decipher.final();
    return new Uint8Array(Buffer.concat([p1, p2]));
  }

  sign({ privateKey, msg } = {}) {
    if (!isBytes(privateKey) || !isBytes(msg)) {
      throw new Error("NodeCryptoProvider.sign requires Uint8Array inputs");
    }
    const key = createPrivateKey({ key: privateKey, format: "der", type: "pkcs8" });
    return new Uint8Array(nodeSign(null, msg, key));
  }

  verify({ publicKey, msg, sig } = {}) {
    if (!isBytes(publicKey) || !isBytes(msg) || !isBytes(sig)) {
      throw new Error("NodeCryptoProvider.verify requires Uint8Array inputs");
    }
    const key = createPublicKey({ key: publicKey, format: "der", type: "spki" });
    return nodeVerify(null, msg, key, sig);
  }

  dhGenerateKeyPair({ alg = "X25519", fmt = "spki" } = {}) {
    const normalized = String(alg).toLowerCase();
    if (normalized !== "x25519") {
      throw new Error(`NodeCryptoProvider.dhGenerateKeyPair unsupported alg ${alg}`);
    }

    if (fmt !== "spki" && fmt !== "raw") {
      throw new Error("NodeCryptoProvider.dhGenerateKeyPair requires fmt 'spki' or 'raw'");
    }

    const { publicKey, privateKey } = generateKeyPairSync("x25519", {
      publicKeyEncoding: { format: "der", type: "spki" },
      privateKeyEncoding: { format: "der", type: "pkcs8" },
    });

    const publicKeyBytes = new Uint8Array(publicKey);
    const privateKeyBytes = new Uint8Array(privateKey);

    return { publicKey: publicKeyBytes, privateKey: privateKeyBytes };
  }

  dhDerive({ privateKey, publicKey, alg = "X25519", fmt = "spki" } = {}) {
    const normalized = String(alg).toLowerCase();
    if (normalized !== "x25519") {
      throw new Error(`NodeCryptoProvider.dhDerive unsupported alg ${alg}`);
    }
    if (!isBytes(privateKey) || !isBytes(publicKey)) {
      throw new Error("NodeCryptoProvider.dhDerive requires Uint8Array keys");
    }
    if (fmt !== "spki" && fmt !== "raw") {
      throw new Error("NodeCryptoProvider.dhDerive requires fmt 'spki' or 'raw'");
    }

    const privateKeyObj = createPrivateKey({ key: privateKey, format: "der", type: "pkcs8" });
    const publicKeyObj = createPublicKey({ key: publicKey, format: "der", type: "spki" });
    const secret = diffieHellman({ privateKey: privateKeyObj, publicKey: publicKeyObj });
    return new Uint8Array(secret);
  }

  generateSigningKeyPair() {
    const { publicKey, privateKey } = generateKeyPairSync("ed25519", {
      publicKeyEncoding: { format: "der", type: "spki" },
      privateKeyEncoding: { format: "der", type: "pkcs8" },
    });
    return {
      publicKey: new Uint8Array(publicKey),
      privateKey: new Uint8Array(privateKey),
    };
  }

  signingKeyPairFromSeed(seed) {
    if (!isBytes(seed) || seed.length !== 32) {
      throw new Error("NodeCryptoProvider.signingKeyPairFromSeed requires a 32-byte seed");
    }
    const prefix = Buffer.from("302e020100300506032b657004220420", "hex");
    const privateKeyObject = createPrivateKey({
      key: Buffer.concat([prefix, Buffer.from(seed)]),
      format: "der",
      type: "pkcs8",
    });
    const publicKeyObject = createPublicKey(privateKeyObject);
    return {
      publicKey: new Uint8Array(publicKeyObject.export({ format: "der", type: "spki" })),
      privateKey: new Uint8Array(privateKeyObject.export({ format: "der", type: "pkcs8" })),
    };
  }
}
