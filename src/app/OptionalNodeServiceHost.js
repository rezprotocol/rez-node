/**
 * Optional-subsystem lifecycle host (ATLAS_PREREQUISITES P6.1).
 *
 * A small composition helper — NOT a plugin loader. It does not discover
 * packages, load modules, resolve dependencies by string name, or own timers
 * for its services. Each service is a constructed class instance with:
 *
 *   name          stable, non-user-derived string
 *   start()       async ok
 *   stop()        async ok
 *   getStatus()   local detail object (merged under `detail`)
 *   isEnabled()   optional; false → the service is never started
 *
 * Failure policy (R5): an optional service failing to start, tick, or stop is
 * reported in the host's own namespaced status and NEVER aborts mesh startup,
 * changes core readiness, or affects routing. Optional status is never merged
 * into MeshCoordinator.getStatus() or /ready.
 */
import { RRecord } from "@rezprotocol/core";
import { raceWithDeadline } from "../util/raceWithDeadline.js";

export const OPTIONAL_SERVICE_STATES = Object.freeze([
  "disabled", "starting", "ready", "degraded", "failed", "stopped",
]);
const STATE_SET = new Set(OPTIONAL_SERVICE_STATES);

export class OptionalServiceStatusV1 extends RRecord {
  static type = "OptionalServiceStatusV1";

  constructor({ name, state, error = null } = {}) {
    super();
    this.name = typeof name === "string" ? name : "";
    this.state = state;
    this.error = error == null ? null : String(error).slice(0, 256);
    if (this.constructor === OptionalServiceStatusV1) this._seal();
  }

  validate() {
    this.assert(this.name.length > 0 && this.name.length <= 64, "OptionalServiceStatusV1.name invalid");
    this.assert(STATE_SET.has(this.state), "OptionalServiceStatusV1.state invalid", { state: this.state });
  }
}

const VALID_NAME_RE = /^[a-z][a-z0-9-]{0,63}$/;

/**
 * Bounded human-readable description of ANY thrown/rejected value (re-audit
 * R5): services can reject with strings, objects, or null — isolation must
 * hold for every value, so nothing here throws and nothing is rethrown.
 */
function describeThrown(value) {
  if (value instanceof Error) return value.message || value.name || "Error";
  if (typeof value === "string") return value;
  try {
    const json = JSON.stringify(value);
    return json === undefined ? String(value) : json;
  } catch (stringifyErr) {
    return "unstringifiable thrown value (" + typeof value + "): " + stringifyErr.message;
  }
}

export class OptionalNodeServiceHost {
  #services;
  #states;
  #startTimeoutMs;
  #stopTimeoutMs;

  /**
   * @param {{ services?: object[], startTimeoutMs?: number, stopTimeoutMs?: number }} [opts]
   */
  constructor({ services = [], startTimeoutMs = 5_000, stopTimeoutMs = 5_000 } = {}) {
    this.#services = [];
    this.#states = new Map();
    this.#startTimeoutMs = startTimeoutMs;
    this.#stopTimeoutMs = stopTimeoutMs;
    for (const service of services) {
      this.register(service);
    }
  }

  register(service) {
    if (!service || typeof service !== "object") {
      throw new Error("OptionalNodeServiceHost.register requires a service instance");
    }
    const name = typeof service.name === "string" ? service.name : "";
    if (!VALID_NAME_RE.test(name)) {
      throw new Error("optional service name must be stable kebab-case (got " + JSON.stringify(name) + ")");
    }
    if (this.#states.has(name)) {
      throw new Error("optional service name already registered: " + name);
    }
    if (typeof service.start !== "function" || typeof service.stop !== "function" || typeof service.getStatus !== "function") {
      throw new Error("optional service " + name + " must implement start(), stop(), getStatus()");
    }
    this.#services.push(service);
    this.#states.set(name, { state: "disabled", error: null, started: false });
  }

  /**
   * Start every enabled service independently, AFTER the core runtime is
   * constructed and started. One service's failure is recorded and isolated —
   * it never throws out of this method and never stops the others.
   */
  async startAll() {
    for (const service of this.#services) {
      const state = this.#states.get(service.name);
      let enabled = true;
      if (typeof service.isEnabled === "function") {
        try {
          enabled = service.isEnabled() === true;
        } catch (err) {
          state.state = "failed";
          state.error = "isEnabled threw: " + describeThrown(err);
          continue;
        }
      }
      if (!enabled) {
        state.state = "disabled";
        continue;
      }
      state.state = "starting";
      try {
        // Re-audit R5: start() is bounded like stop() — a hung optional
        // service must not stall startRezNode() resolution forever. The core
        // node is already up; a start-timeout records "failed" and moves on.
        const startWork = Promise.resolve(service.start()).then(() => "started");
        const outcome = await raceWithDeadline(startWork, this.#startTimeoutMs, "start-timeout");
        if (outcome === "start-timeout") {
          state.state = "failed";
          state.error = "start timed out after " + this.#startTimeoutMs + "ms";
          // The service may still complete its start in the background:
          // mark it started so stopAll() attempts a bounded, isolated
          // cleanup at shutdown rather than leaking it.
          state.started = true;
        } else {
          state.state = "ready";
          state.error = null;
          state.started = true;
        }
      } catch (err) {
        state.state = "failed";
        state.error = "start failed: " + describeThrown(err);
      }
    }
  }

  /**
   * Stop successfully-started services in REVERSE start order, each bounded
   * by the stop timeout. Stop failures are recorded, never thrown.
   */
  async stopAll() {
    for (let i = this.#services.length - 1; i >= 0; i -= 1) {
      const service = this.#services[i];
      const state = this.#states.get(service.name);
      if (!state.started) continue;
      try {
        // Shutdown is exactly when the event loop is draining, so this is the
        // bound that suffers most from a timer that does not hold it open —
        // see raceWithDeadline, which owns that invariant for both lifecycle
        // hooks.
        const stopWork = Promise.resolve(service.stop()).then(() => "stopped");
        const outcome = await raceWithDeadline(stopWork, this.#stopTimeoutMs, "stop-timeout");
        if (outcome === "stop-timeout") {
          state.state = "failed";
          state.error = "stop timed out after " + this.#stopTimeoutMs + "ms";
        } else {
          state.state = "stopped";
        }
      } catch (err) {
        state.state = "failed";
        state.error = "stop failed: " + describeThrown(err);
      }
      state.started = false;
    }
  }

  /** A service reporting itself degraded/failed updates only its own row. */
  refreshStates() {
    for (const service of this.#services) {
      const state = this.#states.get(service.name);
      if (state.state !== "ready" && state.state !== "degraded") continue;
      try {
        const detail = service.getStatus();
        if (detail && detail.degraded === true) {
          state.state = "degraded";
          state.error = typeof detail.error === "string" ? detail.error : state.error;
        } else if (detail && detail.failed === true) {
          state.state = "failed";
          state.error = typeof detail.error === "string" ? detail.error : state.error;
        } else {
          state.state = "ready";
        }
      } catch (err) {
        state.state = "degraded";
        state.error = "getStatus threw: " + describeThrown(err);
      }
    }
  }

  /**
   * Namespaced status map — NEVER merged into mesh status or readiness.
   * @returns {Record<string, OptionalServiceStatusV1>}
   */
  getStatuses() {
    this.refreshStates();
    const out = {};
    for (const service of this.#services) {
      const state = this.#states.get(service.name);
      out[service.name] = new OptionalServiceStatusV1({
        name: service.name,
        state: state.state,
        error: state.error,
      });
    }
    return out;
  }

  get serviceCount() {
    return this.#services.length;
  }
}
