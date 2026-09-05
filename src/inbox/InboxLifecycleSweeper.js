/**
 * InboxLifecycleSweeper — reclamation (portable inbox lease L2,
 * plans/PORTABLE_INBOX_LEASE_SPEC.md §3).
 *
 * DELIBERATELY TIMER-FREE. The registry's lifecycle verdict is a pure
 * function of durable state + now, so reclamation is an idempotent sweep any
 * caller may run at any time — after a restart, after a year of downtime,
 * twice in a row — and nothing depends on a timer having fired before a
 * shutdown. `markReclaimed` re-derives the verdict inside the registry's
 * write mutex, so a stale work list can never reclaim an inbox whose lease
 * was renewed between reclaimDue() and the write.
 *
 * Order per inbox: tombstone/claim state FIRST (admission dies durably),
 * THEN the ciphertext purge — a crash between the two leaves orphaned mail
 * that the next sweep's purge pass removes, never a reclaimed-looking inbox
 * that still accepts anything.
 */
export class InboxLifecycleSweeper {
  #registry;
  #inboxStore;
  #now;

  constructor({ registry, inboxStore, now = Date.now } = {}) {
    if (!registry || typeof registry.reclaimDue !== "function" || typeof registry.markReclaimed !== "function") {
      throw new Error("InboxLifecycleSweeper requires a lease-capable InboxClaimRegistry");
    }
    if (!inboxStore || typeof inboxStore.list !== "function" || typeof inboxStore.ack !== "function") {
      throw new Error("InboxLifecycleSweeper requires the inbox store (list + ack)");
    }
    if (typeof now !== "function") {
      throw new Error("InboxLifecycleSweeper requires now() to be a function");
    }
    this.#registry = registry;
    this.#inboxStore = inboxStore;
    this.#now = now;
  }

  /**
   * One idempotent pass: reclaim every inbox whose verdict is RECLAIMABLE at
   * now(), purging its stored ciphertext through the store's public surface
   * (list + ack keeps the store's own caps/counters coherent).
   * @returns {Promise<{ reclaimed: string[] }>}
   */
  async sweepOnce() {
    const nowMs = Number(this.#now());
    if (!Number.isFinite(nowMs)) {
      throw new Error("InboxLifecycleSweeper: now() returned a non-finite value");
    }
    const reclaimed = [];
    for (const inboxId of this.#registry.reclaimDue(nowMs)) {
      const result = await this.#registry.markReclaimed(inboxId, nowMs, () => this.#purgeMailbox(inboxId));
      if (result.reclaimed !== true) continue; // renewed between list and write — the mutex re-check won
      reclaimed.push(inboxId);
    }
    return { reclaimed };
  }

  async #purgeMailbox(inboxId) {
    // Drain in pages until empty. `ack` is the store's own removal verb, so
    // item/byte counters stay coherent with the caps machinery.
    for (;;) {
      const page = await this.#inboxStore.list(inboxId, { limit: 100 });
      const items = page && Array.isArray(page.items) ? page.items : [];
      if (items.length === 0) return;
      for (const item of items) {
        await this.#inboxStore.ack(inboxId, item.eventId);
      }
    }
  }
}
