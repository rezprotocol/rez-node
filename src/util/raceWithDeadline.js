/**
 * Race an awaited operation against a wall-clock budget.
 *
 * SSOT for every deadline that BOUNDS AN AWAITED OPERATION. Before this
 * existed the same six-line race was hand-copied across DhtLookup,
 * DhtCandidateResolver, DhtNode, RouteAdvisor, and both OptionalNodeServiceHost
 * lifecycle bounds — and every copy carried the same defect (rez-node#5): the
 * timer was unref'd, so it stopped firing the moment the event loop would
 * otherwise drain and the "bound" operation waited forever. One mistake became
 * systemic because the mechanism was duplicated instead of shared.
 *
 * Two invariants this owns, and the reason it is a function rather than a
 * convention:
 *
 * 1. The timer is NEVER unref'd. An unref'd timer does not hold the event loop
 *    open, which makes the bound conditional on unrelated work existing.
 * 2. The timer is ALWAYS cleared once the work settles, so it can outlive that
 *    work by at most the deadline window. That is what makes (1) affordable.
 *
 * This is deliberately mechanical: it does not decide what an expired budget
 * means. A caller with no budget left may want to skip the work entirely
 * (DhtLookup) or may want a 0 ms timer so already-resolved work can still
 * flush its microtasks and be counted (DhtNode's ack window). Those are policy
 * and stay at the call site.
 *
 * Not for background or recurring timers. Pruners, retry pollers, TCP idle
 * timers, and status tickers SHOULD be unref'd — they must never keep the
 * process alive. The rule here applies only where a caller awaits the result.
 *
 * @template T
 * @template S
 * @param {Promise<T>|{then: Function}} work - the operation being bounded
 * @param {number} timeoutMs - budget in ms; must be finite and >= 0
 * @param {S} timeoutValue - resolved value when the clock wins
 * @returns {Promise<T|S>} work's value, or timeoutValue if the budget expires.
 *   Rejects if `work` rejects — a deadline bounds duration, not failure.
 */
export function raceWithDeadline(work, timeoutMs, timeoutValue) {
  if (work === null || typeof work !== "object" || typeof work.then !== "function") {
    throw new TypeError("raceWithDeadline: work must be a promise");
  }
  // Validated rather than coerced: setTimeout treats NaN as 0, which would
  // turn a broken clock reading into an instant, silent deadline expiry
  // instead of a loud failure.
  if (typeof timeoutMs !== "number" || !Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw new RangeError("raceWithDeadline: timeoutMs must be a finite number >= 0, got " + String(timeoutMs));
  }
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => resolve(timeoutValue), timeoutMs);
    work.then(
      (value) => { clearTimeout(timer); resolve(value); },
      (err) => { clearTimeout(timer); reject(err); },
    );
  });
}
