/**
 * ServiceGate — single enforcement point for capability + pricing + settlement.
 *
 * Sits between handlers and CapabilityMiddleware. For every service request:
 * 1. Resolve capability chain (CapabilityMiddleware.resolveChain())
 * 2. Resolve price (PricingResolver.resolve())
 * 3. Check balance / debit (SettlementProvider.debit())
 * 4. Return authorization or rejection
 *
 * Handlers call serviceGate.authorize() instead of middleware.resolveChain()
 * directly so pricing and settlement are inseparable from authz.
 *
 * Free services (no serviceId, or cost=0) skip pricing and settlement entirely.
 */
export class ServiceGate {
  #capabilityMiddleware;
  #pricingResolver;
  #settlementProvider;

  /**
   * @param {object} opts
   * @param {CapabilityMiddleware} opts.capabilityMiddleware
   * @param {PricingResolver} opts.pricingResolver
   * @param {SettlementProvider} opts.settlementProvider
   */
  constructor({ capabilityMiddleware, pricingResolver, settlementProvider }) {
    if (!capabilityMiddleware) throw new Error("ServiceGate requires capabilityMiddleware");
    if (!pricingResolver) throw new Error("ServiceGate requires pricingResolver");
    if (!settlementProvider) throw new Error("ServiceGate requires settlementProvider");
    this.#capabilityMiddleware = capabilityMiddleware;
    this.#pricingResolver = pricingResolver;
    this.#settlementProvider = settlementProvider;
  }

  /**
   * Authorize a service request.
   *
   * @param {object} opts
   * @param {import("@rezprotocol/core").RCapability[]} opts.capabilityChain — cap chain rooted at the resource's trust root
   * @param {string} opts.requiredAction — e.g. "post", "read", "write"
   * @param {string} opts.requiredResource — e.g. "mailbox:abc123"
   * @param {string|null} [opts.presenterPublicKeyB64] — pubkey of the entity presenting the chain (for leaf-grantee check)
   * @param {string} opts.ownerPublicKeyB64 — wallet identity (claimant pubkey) to debit
   * @param {string} [opts.serviceId] — pricing service ID (omit for free services)
   * @param {object} [opts.serviceParams] — service-specific params for pricing (quantity, size, etc.)
   * @returns {Promise<{ok: boolean, capability?: object, receipt?: DebitReceiptV1, error?: string, code?: string}>}
   */
  async authorize({
    capabilityChain,
    requiredAction,
    requiredResource,
    presenterPublicKeyB64 = null,
    ownerPublicKeyB64,
    serviceId = null,
    serviceParams = {},
  }) {
    // Step 1: Capability chain check
    if (!this.#capabilityMiddleware) {
      return { ok: false, error: "Capability middleware not initialized", code: "UNAUTHORIZED" };
    }

    const capResult = await this.#capabilityMiddleware.resolveChain({
      capabilityChain,
      requiredAction,
      requiredResource,
      presenterPublicKeyB64,
    });
    if (!capResult.ok) {
      return { ok: false, error: capResult.error, code: "FORBIDDEN" };
    }

    // Step 2: Pricing (skip if no serviceId — free service)
    if (!serviceId) {
      return { ok: true, capability: capResult.capability };
    }

    const pricing = this.#pricingResolver.resolve(serviceId, serviceParams);
    if (pricing.cost <= 0) {
      return { ok: true, capability: capResult.capability };
    }

    // Step 3: Settlement (debit). Settlement keys wallets by the claimant
    // pubkey — same identity the rest of the relay uses for routing.
    if (!ownerPublicKeyB64) {
      return { ok: false, error: "ownerPublicKeyB64 required for paid service", code: "BAD_REQUEST" };
    }

    const balance = await this.#settlementProvider.balance(ownerPublicKeyB64);
    if (balance.available < pricing.cost) {
      return {
        ok: false,
        error: `Insufficient balance: available=${balance.available}, required=${pricing.cost} ${pricing.currency}`,
        code: "PAYMENT_REQUIRED",
      };
    }

    let receipt;
    try {
      receipt = await this.#settlementProvider.debit(ownerPublicKeyB64, pricing.cost, {
        serviceId,
        serviceRef: requiredResource,
      });
    } catch (err) {
      return { ok: false, error: `Settlement failed: ${err.message}`, code: "PAYMENT_FAILED" };
    }

    return { ok: true, capability: capResult.capability, receipt };
  }

  /**
   * Access the underlying pricing resolver (for listing services, etc.)
   * @returns {PricingResolver}
   */
  get pricingResolver() {
    return this.#pricingResolver;
  }

  /**
   * Access the underlying settlement provider (for balance checks, credits, etc.)
   * @returns {SettlementProvider}
   */
  get settlementProvider() {
    return this.#settlementProvider;
  }
}
