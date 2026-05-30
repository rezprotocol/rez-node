import { randomBytes } from "node:crypto";

/**
 * Generate a unique hex ID for settlement records (receipts, escrows, challenges).
 * @returns {string} 32-character hex string
 */
export function generateSettlementId() {
  return Buffer.from(randomBytes(16)).toString("hex");
}
