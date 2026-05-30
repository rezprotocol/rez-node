import { RRecord } from "@rezprotocol/core";

export class ContractRegistry {
  constructor() {
    this._types = new Map();
  }

  register(type, ctor) {
    if (typeof type !== "string" || type.trim().length === 0) {
      throw new Error("ContractRegistry.register requires type");
    }
    if (typeof ctor !== "function") {
      throw new Error("ContractRegistry.register requires constructor");
    }
    if (!(ctor.prototype instanceof RRecord)) {
      throw new Error(`Contract constructor for ${type} must extend RRecord`);
    }
    if (this._types.has(type)) {
      throw new Error(`Contract type already registered: ${type}`);
    }
    this._types.set(type, ctor);
  }

  get(type) {
    return this._types.get(type) || null;
  }

  has(type) {
    return this._types.has(type);
  }

  listTypes() {
    return Array.from(this._types.keys()).sort();
  }
}
