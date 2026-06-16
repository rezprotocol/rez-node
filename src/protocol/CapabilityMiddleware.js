import { CapabilityValidator, RResource } from "@rezprotocol/core";

/**
 * CapabilityMiddleware — validates a capability chain presented on a request.
 *
 * The sole authz primitive after the v1 cap rework. Performs:
 *   1. `validator.validateChain` — verifies every signature, parent-child
 *      linkage, scope narrowing, and (when `presenterPublicKeyB64` is set)
 *      that the leaf was granted to the presenter.
 *   2. Trust-root anchor — for inbox/mailbox-scoped resources the root
 *      cap's `signerPublicKeyB64` MUST equal the claimant pubkey stored in
 *      `InboxClaimRegistry`. Without this, an attacker could mint and sign
 *      a perfectly-shaped chain rooted at any key they happen to control.
 *      Closes docs/SECURITY_AUDIT.md MED-3.
 *   3. Leaf-resource + action match against the request, plus expiry.
 *
 * The legacy id-based `resolve()` + `sessionCapabilities` path was removed
 * after audit pass 2: the node mints no caps, so the path had no production
 * caller, but it sat alongside the chain primitive and was the wired-up
 * authz entry. Keeping only one primitive eliminates the risk of new
 * callers picking the unsafe one.
 */
export class CapabilityMiddleware {
  #validator;
  #inboxClaimRegistry;

  /**
   * @param {{
   *   validator: CapabilityValidator,
   *   inboxClaimRegistry?: { getClaimantPublicKey: (inboxId: string) => (string|null|Promise<string|null>) } | null,
   * }} opts
   */
  constructor({ validator, inboxClaimRegistry = null }) {
    if (!validator) throw new Error("CapabilityMiddleware requires validator");
    this.#validator = validator;
    this.#inboxClaimRegistry = inboxClaimRegistry || null;
  }

  /**
   * Validate a capability chain presented on a request.
   *
   * @param {{
   *   capabilityChain: import("@rezprotocol/core").RCapability[],
   *   requiredAction: string,
   *   requiredResource: string,
   *   presenterPublicKeyB64?: string|null,
   * }} opts
   * @returns {Promise<{ ok: boolean, capability?: import("@rezprotocol/core").RCapability, error?: string }>}
   */
  async resolveChain({ capabilityChain, requiredAction, requiredResource, presenterPublicKeyB64 = null } = {}) {
    if (!Array.isArray(capabilityChain) || capabilityChain.length === 0) {
      return { ok: false, error: "capabilityChain required" };
    }
    if (typeof requiredAction !== "string" || !requiredAction) {
      return { ok: false, error: "requiredAction required" };
    }
    if (typeof requiredResource !== "string" || !requiredResource) {
      return { ok: false, error: "requiredResource required" };
    }

    const chainResult = await this.#validator.validateChain(capabilityChain, { presenterPublicKeyB64 });
    if (!chainResult.ok) {
      return { ok: false, error: `chain invalid: ${chainResult.reason}` };
    }

    // Anchor the root signer to the inbox claimant when the chain governs
    // an inbox/mailbox resource. Skip the anchor for resource kinds with
    // no per-inbox trust root (channel/object); those will land in their
    // own audit follow-ups.
    const resource = parseResourceSafely(requiredResource);
    if (!resource) {
      return { ok: false, error: `unparseable resource: ${requiredResource}` };
    }
    if (resource.kind === RResource.KINDS.INBOX || resource.kind === RResource.KINDS.MAILBOX) {
      if (!this.#inboxClaimRegistry || typeof this.#inboxClaimRegistry.getClaimantPublicKey !== "function") {
        return { ok: false, error: "inbox claim registry unavailable for trust-root anchor" };
      }
      const claimantPubKey = await this.#inboxClaimRegistry.getClaimantPublicKey(resource.id);
      if (!claimantPubKey) {
        return { ok: false, error: `inbox ${resource.id} is not claimed; no trust root` };
      }
      const rootSigner = capabilityChain[0].signerPublicKeyB64;
      if (rootSigner !== claimantPubKey) {
        return { ok: false, error: "root cap signer does not match inbox claimant" };
      }
    }

    const leaf = chainResult.leaf;
    if (leaf.resource !== requiredResource) {
      return { ok: false, error: `leaf cap does not cover resource ${requiredResource}` };
    }
    if (!Array.isArray(leaf.actions) || !leaf.actions.includes(requiredAction)) {
      return { ok: false, error: `leaf cap does not include action ${requiredAction}` };
    }
    const constraintResult = this.#validator.checkConstraints(leaf, { nowMs: Date.now() });
    if (!constraintResult.ok) {
      return { ok: false, error: constraintResult.reason };
    }
    return { ok: true, capability: leaf };
  }
}

function parseResourceSafely(resourceString) {
  try {
    return RResource.parse(resourceString);
  } catch {
    return null;
  }
}
