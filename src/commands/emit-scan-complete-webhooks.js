import * as core from '@actions/core'
import { authenticate, executeSql } from '../lib/db.js'
import { sanitizeSchema } from '../lib/sql/index.js'
import { buildScanCompleteEligibilitySql } from '../lib/scan-complete-sql.js'
import { rowToScanCompleteWebhookBody, getRowUserId } from '../lib/scan-complete-payload.js'
import { userHasScanCompleteSentAt } from '../lib/scan-complete-firestore.js'
import { postScanCompleteWebhook } from '../lib/scan-complete-webhook.js'

/**
 * Cron: eligible first LOOKBACK completions → Firestore dedupe → POST /dealsync-v2/webhooks (scan_complete).
 * @see docs/plans/2026-04-16-scan-complete-w3-cron-tech-spec.md
 */
export async function runEmitScanCompleteWebhooks() {
  const authUrl = core.getInput('sxt-auth-url', { required: true })
  const authSecret = core.getInput('sxt-auth-secret', { required: true })
  const apiUrl = core.getInput('sxt-api-url', { required: true })
  const biscuit = core.getInput('sxt-biscuit', { required: true })
  const dealsyncSchema = sanitizeSchema(core.getInput('sxt-schema', { required: true }))
  const emailCoreSchema = sanitizeSchema(core.getInput('email-core-schema') || 'EMAIL_CORE_STAGING')

  const backendBaseUrl = core.getInput('dealsync-backend-base-url', { required: true })
  const sharedSecret = core.getInput('dealsync-v2-shared-secret', { required: true })
  const saJsonRaw = core.getInput('firestore-service-account-json', { required: true })
  let firestoreProjectId = core.getInput('firestore-project-id') || ''
  const concurrency = Math.max(
    1,
    parseInt(core.getInput('scan-complete-webhook-concurrency') || '5', 10),
  )

  let credentials
  try {
    credentials = JSON.parse(saJsonRaw)
  } catch {
    throw new Error('firestore-service-account-json must be valid JSON')
  }
  if (!firestoreProjectId && typeof credentials.project_id === 'string') {
    firestoreProjectId = credentials.project_id
  }
  if (!firestoreProjectId) {
    throw new Error('firestore-project-id is required (or project_id in service account JSON)')
  }

  const sql = buildScanCompleteEligibilitySql(emailCoreSchema, dealsyncSchema)
  const jwt = await authenticate(authUrl, authSecret)
  const exec = (q) => executeSql(apiUrl, jwt, biscuit, q)

  console.log('[emit-scan-complete-webhooks] executing eligibility query')
  const result = await exec(sql)
  const rows = Array.isArray(result) ? result : []
  console.log(`[emit-scan-complete-webhooks] eligibility rows=${rows.length}`)

  let scanned = rows.length
  let skippedDeduped = 0
  let posted = 0
  let errors = 0

  for (let i = 0; i < rows.length; i += concurrency) {
    const chunk = rows.slice(i, i + concurrency)
    await Promise.all(
      chunk.map(async (row) => {
        let userId
        try {
          userId = getRowUserId(row)
        } catch (err) {
          console.log(`[emit-scan-complete-webhooks] skip invalid row: ${err.message}`)
          errors++
          return
        }

        try {
          const alreadySent = await userHasScanCompleteSentAt({
            projectId: firestoreProjectId,
            userId,
            credentials,
          })
          if (alreadySent) {
            skippedDeduped++
            console.log(`[emit-scan-complete-webhooks] skip dedupe userId=${userId}`)
            return
          }

          const body = rowToScanCompleteWebhookBody(row)
          const res = await postScanCompleteWebhook(backendBaseUrl, sharedSecret, body)
          if (!res.ok) {
            errors++
            console.log(
              `[emit-scan-complete-webhooks] POST failed userId=${userId} status=${res.status} body=${(res.text || '').slice(0, 500)}`,
            )
            return
          }
          posted++
          console.log(`[emit-scan-complete-webhooks] posted userId=${userId}`)
        } catch (err) {
          errors++
          console.log(`[emit-scan-complete-webhooks] error userId=${userId ?? '?'}: ${err.message}`)
        }
      }),
    )
  }

  const summary = { scanned, skippedDeduped, posted, errors }
  console.log(`[emit-scan-complete-webhooks] done ${JSON.stringify(summary)}`)
  return summary
}
