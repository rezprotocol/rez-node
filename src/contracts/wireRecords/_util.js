export function isPlainObject(value) {
  if (!value || typeof value !== "object") return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

export function asNullableString(value) {
  return value == null ? null : String(value);
}

export function asOptionalString(value) {
  return value == null ? undefined : String(value);
}

export function asEpochMs(value, fallback = null) {
  if (value == null && fallback != null) return Number(fallback);
  return Number(value);
}

export function coerceNestedRecord(value, Ctor, fieldPath, { allowNull = false, allowUndefined = false } = {}) {
  if (value === undefined) {
    if (allowUndefined) return undefined;
    throwInvariant(`${fieldPath} must be ${Ctor.name} or plain object`);
  }
  if (value === null) {
    if (allowNull) return null;
    throwInvariant(`${fieldPath} must be ${Ctor.name} or plain object`);
  }
  if (value instanceof Ctor) return value;
  if (isPlainObject(value)) return new Ctor(value);
  throwInvariant(`${fieldPath} must be ${Ctor.name} or plain object`);
}

function throwInvariant(message) {
  const err = new Error(message);
  err.name = "RezInvariantError";
  throw err;
}
