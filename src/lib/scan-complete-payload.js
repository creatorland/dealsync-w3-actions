/**
 * Map SxT row shapes → ScanCompleteWebhookDto-compatible JSON body.
 * @see backend/src/dtos/dealsync-v2.webhooks.dto.ts
 */

/**
 * @param {Record<string, unknown>} row
 * @param {string} key
 * @returns {unknown}
 */
function pick(row, key) {
  const upper = key.toUpperCase()
  if (row[key] !== undefined && row[key] !== null) return row[key]
  if (row[upper] !== undefined && row[upper] !== null) return row[upper]
  const lower = key.toLowerCase()
  if (row[lower] !== undefined && row[lower] !== null) return row[lower]
  return undefined
}

/**
 * @param {unknown} v
 * @returns {number}
 */
export function coerceNumber(v) {
  if (v === null || v === undefined) return 0
  const n = Number(v)
  return Number.isFinite(n) ? n : 0
}

/**
 * @param {Record<string, unknown>} row
 * @returns {string}
 */
export function getRowUserId(row) {
  const v = pick(row, 'user_id') ?? pick(row, 'USER_ID')
  if (v === undefined || v === null || String(v).trim() === '') {
    throw new Error('Eligible row missing user_id')
  }
  return String(v)
}

/**
 * @param {Record<string, unknown>} row
 * @returns {{ userId: string, eventType: 'scan_complete', eventData: { dealCounts: object, contactsAdded: number } }}
 */
export function rowToScanCompleteWebhookBody(row) {
  const userId = getRowUserId(row)
  const dealCounts = {
    new: coerceNumber(pick(row, 'db_new')),
    inProgress: coerceNumber(pick(row, 'db_in_progress')),
    completed: coerceNumber(pick(row, 'db_completed')),
    likelyScam: coerceNumber(pick(row, 'db_likely_scam')),
    lowConfidence: coerceNumber(pick(row, 'db_low_confidence')),
    notInterested: coerceNumber(pick(row, 'db_not_interested')),
  }
  const contactsAdded = coerceNumber(pick(row, 'contacts_added'))
  return {
    userId,
    eventType: 'scan_complete',
    eventData: {
      dealCounts,
      contactsAdded,
    },
  }
}
