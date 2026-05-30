import { StorageProvider } from "@rezprotocol/core";
import { FsObjectStore } from "./FsObjectStore.js";
import { FsMailboxStore } from "./FsMailboxStore.js";
import { FsKeyValueStore } from "./FsKeyValueStore.js";
import { EncryptedKeyValueStore } from "./EncryptedKeyValueStore.js";
import { EncryptedObjectStore } from "./EncryptedObjectStore.js";
import { EncryptedMailboxStore } from "./EncryptedMailboxStore.js";
import { NodeCryptoProvider } from "../../crypto/NodeCryptoProvider.js";
import { createKeyValueBackedPeerLinkStorage } from "@rezprotocol/sdk/peer-link";

export class FsStorageProvider extends StorageProvider {
  constructor({ rootDir, encryptionKey = null } = {}) {
    super();
    if (!rootDir) {
      throw new Error("FsStorageProvider requires rootDir");
    }
    this.rootDir = rootDir;
    const rawObjectStore = new FsObjectStore({ rootDir });
    const rawMailboxStore = new FsMailboxStore({ rootDir });
    const rawKv = new FsKeyValueStore({ rootDir });
    if (encryptionKey instanceof Uint8Array && encryptionKey.length === 32) {
      const crypto = new NodeCryptoProvider();
      this.objectStore = new EncryptedObjectStore({ inner: rawObjectStore, crypto, key: encryptionKey });
      this.mailboxStore = new EncryptedMailboxStore({ inner: rawMailboxStore, crypto, key: encryptionKey });
      this.keyValueStore = new EncryptedKeyValueStore({ inner: rawKv, crypto, key: encryptionKey });
    } else {
      this.objectStore = rawObjectStore;
      this.mailboxStore = rawMailboxStore;
      this.keyValueStore = rawKv;
    }
    this.peerLinkStorage = createKeyValueBackedPeerLinkStorage({ keyValueStore: this.keyValueStore });
  }

  getObjectStore() {
    return this.objectStore;
  }

  getMailboxStore() {
    return this.mailboxStore;
  }

  getKeyValueStore() {
    return this.keyValueStore;
  }

  getPeerLinkStorage() {
    return this.peerLinkStorage;
  }
}
