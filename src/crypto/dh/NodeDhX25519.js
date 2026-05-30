import { createPrivateKey, createPublicKey, diffieHellman, generateKeyPairSync } from "node:crypto";
import { RDh, isBytes } from "@rezprotocol/core";

const ALG_ID = "X25519";

export const X25519_SUPPORTED = (() => {
  try {
    generateKeyPairSync("x25519", {
      publicKeyEncoding: { format: "der", type: "spki" },
      privateKeyEncoding: { format: "der", type: "pkcs8" },
    });
    return true;
  } catch {
    return false;
  }
})();

export class NodeDhX25519 extends RDh {
  constructor() {
    super();
    if (!X25519_SUPPORTED) {
      throw new Error("X25519 is not supported in this Node runtime");
    }
  }

  getAlgId() {
    return ALG_ID;
  }

  generateKeyPair() {
    const { publicKey, privateKey } = generateKeyPairSync("x25519", {
      publicKeyEncoding: { format: "der", type: "spki" },
      privateKeyEncoding: { format: "der", type: "pkcs8" },
    });

    return {
      publicKeyBytes: new Uint8Array(publicKey),
      privateKeyBytes: new Uint8Array(privateKey),
    };
  }

  deriveSecret(privateKeyBytes, publicKeyBytes) {
    if (!isBytes(privateKeyBytes) || !isBytes(publicKeyBytes)) {
      throw new Error("NodeDhX25519.deriveSecret(privateKeyBytes, publicKeyBytes) requires Uint8Array");
    }

    const privateKey = createPrivateKey({ key: privateKeyBytes, format: "der", type: "pkcs8" });
    const publicKey = createPublicKey({ key: publicKeyBytes, format: "der", type: "spki" });
    const secret = diffieHellman({ privateKey, publicKey });
    return new Uint8Array(secret);
  }
}
