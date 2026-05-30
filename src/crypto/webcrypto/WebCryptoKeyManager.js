import { RKeyManager, RPublicKey, RPrivateKey, isBytes } from "@rezprotocol/core";
import { getAlgorithms } from "./algorithms.js";

export class WebCryptoKeyManager extends RKeyManager {
  constructor({ subtle } = {}) {
    super();
    this.subtle = subtle;
  }

  async exportPublicKey(publicKey) {
    if (!(publicKey instanceof RPublicKey)) {
      throw new Error("WebCryptoKeyManager.exportPublicKey(publicKey) requires RPublicKey");
    }
    const spki = await this.subtle.exportKey("spki", publicKey.raw);
    return new Uint8Array(spki);
  }

  async exportPrivateKey(privateKey) {
    if (!(privateKey instanceof RPrivateKey)) {
      throw new Error("WebCryptoKeyManager.exportPrivateKey(privateKey) requires RPrivateKey");
    }
    const pkcs8 = await this.subtle.exportKey("pkcs8", privateKey.raw);
    return new Uint8Array(pkcs8);
  }

  async importPublicKey(bytes) {
    if (!isBytes(bytes)) {
      throw new Error("WebCryptoKeyManager.importPublicKey(bytes) requires Uint8Array");
    }
    const { algName, keyAlgorithm } = await getAlgorithms();
    const key = await this.subtle.importKey(
      "spki",
      bytes,
      keyAlgorithm,
      true,
      ["verify"]
    );
    return new RPublicKey({ alg: algName, raw: key });
  }

  async importPrivateKey(bytes) {
    if (!isBytes(bytes)) {
      throw new Error("WebCryptoKeyManager.importPrivateKey(bytes) requires Uint8Array");
    }
    const { algName, keyAlgorithm } = await getAlgorithms();
    const key = await this.subtle.importKey(
      "pkcs8",
      bytes,
      keyAlgorithm,
      true,
      ["sign"]
    );
    return new RPrivateKey({ alg: algName, raw: key });
  }
}
