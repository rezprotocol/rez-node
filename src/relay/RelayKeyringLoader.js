import { OnionKeyringV1 } from "@rezprotocol/core";

export function loadOnionKeyringV1({ keys = [] } = {}) {
  const keyring = new OnionKeyringV1();
  for (const key of keys) {
    keyring.addKey({
      onionKeyId: key.onionKeyId,
      privateKeyBytes: key.privateKeyBytes,
      notBefore: key.notBefore,
      notAfter: key.notAfter,
      status: key.status,
    });
  }
  return keyring;
}
