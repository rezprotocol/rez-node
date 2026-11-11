import { gunzipSync } from "node:zlib";

export function unpackGzip(bytes) {
  return bytes[0] === 31 && bytes[1] === 139 ? gunzipSync(bytes) : bytes;
}

function text(bytes, start, length) {
  return bytes.subarray(start, start + length).toString("utf8").replace(/\0.*$/s, "");
}

function paxFields(bytes) {
  const fields = new Map();
  let offset = 0;
  while (offset < bytes.length) {
    const space = bytes.indexOf(32, offset);
    const size = Number(bytes.subarray(offset, space).toString());
    if (space < offset || !Number.isSafeInteger(size) || size <= space - offset + 1 || offset + size > bytes.length) {
      throw new Error("Malformed PAX record");
    }
    const record = bytes.subarray(space + 1, offset + size - 1).toString("utf8");
    const equals = record.indexOf("=");
    if (equals < 1) throw new Error("Malformed PAX field");
    fields.set(record.slice(0, equals), record.slice(equals + 1));
    offset += size;
  }
  return fields;
}

export function isTar(bytes) {
  if (bytes.length < 512) return false;
  if (bytes.length % 512 === 0 && bytes.every((byte) => byte === 0)) return true;
  const header = bytes.subarray(0, 512);
  const expected = parseInt(text(header, 148, 8).trim(), 8);
  let sum = 0;
  for (let i = 0; i < 512; i += 1) sum += i >= 148 && i < 156 ? 32 : header[i];
  return Number.isSafeInteger(expected) && expected === sum;
}

// Entries are positional tuples: path, type, bytes, symlink target. Nothing is
// extracted, so image-supplied paths and symlinks never reach the local disk.
export function* readTar(bytes) {
  let offset = 0;
  let extended = new Map();
  let longPath = "";
  let longLink = "";
  while (offset + 512 <= bytes.length) {
    const header = bytes.subarray(offset, offset + 512);
    if (header.every((byte) => byte === 0)) {
      if (!bytes.subarray(offset).every((byte) => byte === 0)) throw new Error("Unexpected bytes after tar terminator");
      return;
    }
    if (!isTar(header)) throw new Error("Invalid tar header checksum");
    if (header[124] & 128) throw new Error("Unsupported binary tar length");
    const size = parseInt(text(header, 124, 12).trim() || "0", 8);
    if (!Number.isSafeInteger(size) || size < 0 || offset + 512 + size > bytes.length) throw new Error("Truncated tar entry");
    const data = bytes.subarray(offset + 512, offset + 512 + size);
    offset += 512 + Math.ceil(size / 512) * 512;
    const type = text(header, 156, 1) || "0";
    if (type === "x" || type === "g") {
      if (type === "g") throw new Error("Global PAX fields are unsupported");
      extended = paxFields(data);
      continue;
    }
    if (type === "L" || type === "K") {
      if (type === "L") longPath = text(data, 0, data.length);
      else longLink = text(data, 0, data.length);
      continue;
    }
    const prefix = text(header, 345, 155);
    let name = extended.get("path") || longPath || (prefix ? prefix + "/" : "") + text(header, 0, 100);
    const link = extended.get("linkpath") || longLink || text(header, 157, 100);
    if (extended.has("size") && Number(extended.get("size")) !== size) throw new Error("Unsupported PAX length override");
    name = name.replace(/^(?:\.\/)+/, "");
    if (name.startsWith("/") || name.split("/").includes("..")) throw new Error("Unsafe archive path");
    extended = new Map();
    longPath = "";
    longLink = "";
    yield [name, type, data, link];
  }
  if (offset !== bytes.length || extended.size || longPath || longLink) throw new Error("Truncated tar footer");
}
