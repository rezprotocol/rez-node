import { startRezNode } from "./startRezNode.js";

/**
 * RezNode — class wrapper around the rez-node runtime, following the
 * canonical SDK lifecycle (`start`, `listen`, `stop`) per
 * `docs/architecture.md` § "SDK Lifecycle and Lexicon".
 *
 * Usage:
 *   const node = new RezNode(config);  // sync, inert
 *   await node.start();                 // open storage, init runtime
 *   await node.listen();                // bind WS gateway, connect mesh peers
 *   ...
 *   await node.stop();
 *
 * Today `start()` does both "open storage" and "listen" because the
 * underlying `startRezNode()` function conflates them. The two-step shape
 * is exposed now so callers can move to it; the internal split will
 * happen when there's a concrete need for prepared-but-not-listening
 * state (e.g. gating, supervision).
 *
 * The legacy `startRezNode(config)` function is preserved as a thin
 * convenience and continues to work for tests and existing call sites.
 */
export class RezNode {
  #config;
  #app;
  #started;

  constructor(config) {
    if (!config || typeof config !== "object") {
      throw new Error("RezNode requires a config object");
    }
    this.#config = config;
    this.#app = null;
    this.#started = false;
  }

  async start() {
    if (this.#started) return;
    this.#app = await startRezNode(this.#config);
    this.#started = true;
  }

  async listen() {
    if (!this.#started) await this.start();
    // The underlying startRezNode currently starts the gateway and connects
    // the mesh inside its setup flow. Once that's split into prepare/listen
    // phases, this method will become non-trivial.
  }

  async stop() {
    if (!this.#started || !this.#app) return;
    const app = this.#app;
    this.#app = null;
    this.#started = false;
    if (app && typeof app.stop === "function") {
      await app.stop();
    }
  }

  // --- Accessors for runtime components ---

  get started() {
    return this.#started;
  }

  get runtime() {
    return this.#app ? this.#app.runtime : null;
  }

  get gateway() {
    return this.#app ? this.#app.gateway : null;
  }

  get storageProvider() {
    return this.#app ? this.#app.storageProvider : null;
  }

  get relayStore() {
    return this.#app ? this.#app.relayStore : null;
  }

  get serverServices() {
    return this.#app ? this.#app.serverServices : null;
  }

  get metrics() {
    return this.#app ? this.#app.metrics : null;
  }

  get relayAddress() {
    return this.#app ? this.#app.relayAddress : null;
  }

  get resolvedConfig() {
    return this.#app ? this.#app.config : null;
  }
}
