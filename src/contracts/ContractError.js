export class ContractError extends Error {
  constructor({ code, message, path = null } = {}) {
    super(message || code || "CONTRACT_ERROR");
    this.name = "ContractError";
    this.code = code || "CONTRACT_ERROR";
    if (path) this.path = path;
  }
}
