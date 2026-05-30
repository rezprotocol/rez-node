import { webcrypto } from "node:crypto";

export function getAlgorithms() {
  return algorithmsPromise;
}

const algorithmsPromise = (async (subtle) => {
  try {
    await subtle.generateKey({ name: "Ed25519" }, true, ["sign", "verify"]);
    return {
      algName: "Ed25519",
      keyAlgorithm: { name: "Ed25519" },
      signAlgorithm: { name: "Ed25519" },
    };
  } catch (_err) {
    return {
      algName: "ECDSA",
      keyAlgorithm: { name: "ECDSA", namedCurve: "P-256" },
      signAlgorithm: { name: "ECDSA", hash: "SHA-256" },
    };
  }
})(webcrypto.subtle);
