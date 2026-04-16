/**
 * POST scan_complete lifecycle webhook (same auth as daily digest automation).
 * @see backend/src/controllers/dealsync-v2.webhooks.controller.ts
 */

/**
 * @param {string} baseUrl — backend base URL, no trailing slash
 * @param {string} sharedSecret — DEALSYNC_V2_SHARED_SECRET
 * @param {{ userId: string, eventType: string, eventData: object }} body
 * @returns {Promise<{ ok: boolean, status: number, text?: string }>}
 */
export async function postScanCompleteWebhook(baseUrl, sharedSecret, body) {
  const root = baseUrl.replace(/\/+$/, '')
  const url = `${root}/dealsync-v2/webhooks`
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-shared-secret': sharedSecret,
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(120000),
  })
  const text = await resp.text()
  if (!resp.ok) {
    return { ok: false, status: resp.status, text }
  }
  return { ok: true, status: resp.status, text }
}
