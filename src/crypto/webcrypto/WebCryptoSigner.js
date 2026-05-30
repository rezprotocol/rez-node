import { RSigner, RPublicKey, RPrivateKey, isBytes } from "@rezprotocol/core";
import { getAlgorithms } from "./algorithms.js";

export class WebCryptoSigner extends RSigner {
  constructor({ subtle } = {}) {
    super();
    this.subtle = subtle;
  }

  async generateSigningKeyPair() {
    const { algName, keyAlgorithm } = await getAlgorithms();
    const keyPair = await this.subtle.generateKey(
      keyAlgorithm,
      true,
      ["sign", "verify"]
    );

    return {
      publicKey: new RPublicKey({ alg: algName, raw: keyPair.publicKey }),
      privateKey: new RPrivateKey({ alg: algName, raw: keyPair.privateKey }),
    };
  }

  async sign(privateKey, bytes) {
    if (!(privateKey instanceof RPrivateKey)) {
      throw new Error("WebCryptoSigner.sign(privateKey, bytes) requires RPrivateKey");
    }
    if (!isBytes(bytes)) {
      throw new Error("WebCryptoSigner.sign(privateKey, bytes) requires Uint8Array");
    }

    const { signAlgorithm } = await getAlgorithms();
    const signature = await this.subtle.sign(signAlgorithm, privateKey.raw, bytes);
    return new Uint8Array(signature);
  }

  async verify(publicKey, bytes, signature) {
    if (!(publicKey instanceof RPublicKey)) {
      throw new Error("WebCryptoSigner.verify(publicKey, bytes, signature) requires RPublicKey");
    }
    if (!isBytes(bytes) || !isBytes(signature)) {
      throw new Error("WebCryptoSigner.verify(publicKey, bytes, signature) requires Uint8Array inputs");
    }

    const { signAlgorithm } = await getAlgorithms();
    return this.subtle.verify(signAlgorithm, publicKey.raw, signature, bytes);
  }
}
