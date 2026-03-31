// Shared retry utilities: sleep + exponential backoff.

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Exponential backoff with optional jitter.
 * @param {number} attempt - 0-based attempt index
 * @param {object} opts
 * @param {number} [opts.base=2000] - base delay in ms
 * @param {number} [opts.max=10000] - max delay in ms
 * @param {boolean} [opts.jitter=false] - add random jitter (up to 50% of delay)
 */
export function backoffMs(attempt, { base = 2000, max = 10000, jitter = false } = {}) {
  const delay = Math.min(base * Math.pow(2, attempt), max)
  if (!jitter) return delay
  return Math.round(delay + Math.random() * delay * 0.5)
}
