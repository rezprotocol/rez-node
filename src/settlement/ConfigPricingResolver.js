import { PricingResolver, ServicePricingV1 } from "@rezprotocol/core";

/**
 * Reads fixed prices from relay config.
 *
 * This is the permanent default pricing resolver. Relay operators
 * set their own prices in the relay config. Even chain-mode relays
 * use this as the base (GovernancePricingResolver wraps it to enforce floors).
 */
export class ConfigPricingResolver extends PricingResolver {
  static type = "ConfigPricingResolver";

  #services;

  /**
   * @param {object} opts
   * @param {object} opts.services — map of serviceId → { costPerUnit, unit, currency?, description? }
   */
  constructor({ services }) {
    super();
    if (!services || typeof services !== "object") {
      throw new Error("ConfigPricingResolver requires services config");
    }

    this.#services = new Map();
    for (const [serviceId, config] of Object.entries(services)) {
      if (!config || typeof config !== "object") continue;
      const pricing = new ServicePricingV1({
        serviceId,
        costPerUnit: config.costPerUnit,
        unit: config.unit,
        currency: config.currency || "REZ",
        description: config.description || "",
      });
      this.#services.set(serviceId, pricing);
    }
  }

  resolve(serviceId, params) {
    const pricing = this.#services.get(serviceId);
    if (!pricing) {
      return { cost: 0, currency: "REZ", breakdown: { serviceId, status: "free" } };
    }

    let quantity = 1;
    if (params && typeof params.quantity === "number" && params.quantity > 0) {
      quantity = params.quantity;
    }

    const cost = pricing.costPerUnit * quantity;
    return {
      cost,
      currency: pricing.currency,
      breakdown: {
        serviceId,
        costPerUnit: pricing.costPerUnit,
        unit: pricing.unit,
        quantity,
      },
    };
  }

  listServices() {
    return [...this.#services.values()];
  }
}
