import * as core from '@actions/core'
import { authenticate, executeSql } from '../lib/db.js'
import { parseAndValidate } from '../lib/ai.js'
import { sanitizeSchema } from '../lib/sql/sanitize.js'
import { deals as dealsSql } from '../lib/sql/deals.js'
import { audits as auditsSql } from '../lib/sql/audits.js'

export async function runSyncDealValues() {
  const authUrl = core.getInput('sxt-auth-url')
  const authSecret = core.getInput('sxt-auth-secret')
  const apiUrl = core.getInput('sxt-api-url')
  const biscuit = core.getInput('sxt-biscuit')
  const schema = sanitizeSchema(core.getInput('sxt-schema'))
  const startDate = core.getInput('backfill-start-date') || '2026-03-31'
  const batchSize = parseInt(core.getInput('backfill-batch-size') || '500', 10)
  const dryRun = core.getInput('backfill-dry-run') === 'true'

  console.log(
    `[sync-deal-values] starting startDate=${startDate} batchSize=${batchSize} dryRun=${dryRun}`,
  )

  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  const summary = {
    recovered: 0,
    skipped: { auditMissing: 0, threadNotFound: 0, valueNull: 0, parseError: 0 },
    totalScanned: 0,
  }

  let cursorId = ''
  while (true) {
    const page = await exec(
      dealsSql.findAffectedForBackfill(schema, { startDate, cursorId, limit: batchSize }),
    )
    if (!page || page.length === 0) break

    for (const row of page) {
      summary.totalScanned++
      const dealId = row.ID
      const threadId = row.THREAD_ID

      const auditRows = await exec(auditsSql.findByThread(schema, threadId))
      if (!auditRows || auditRows.length === 0) {
        console.warn(
          `[sync-deal-values] skip deal_id=${dealId} thread_id=${threadId} reason=audit_missing`,
        )
        summary.skipped.auditMissing++
        continue
      }

      let parsed
      try {
        parsed = parseAndValidate(auditRows[0].AI_EVALUATION)
      } catch (err) {
        console.warn(
          `[sync-deal-values] skip deal_id=${dealId} thread_id=${threadId} reason=parse_error err=${err.message}`,
        )
        summary.skipped.parseError++
        continue
      }

      const entry = parsed.find((t) => t.thread_id === threadId)
      if (!entry) {
        console.warn(
          `[sync-deal-values] skip deal_id=${dealId} thread_id=${threadId} reason=thread_not_in_audit`,
        )
        summary.skipped.threadNotFound++
        continue
      }
      if (entry.deal_value == null) {
        console.warn(
          `[sync-deal-values] skip deal_id=${dealId} thread_id=${threadId} reason=deal_value_null`,
        )
        summary.skipped.valueNull++
        continue
      }

      const value = Number(entry.deal_value)
      const currency = entry.deal_currency || 'USD'
      if (!dryRun) {
        await exec(dealsSql.backfillValue(schema, { dealId, value, currency }))
      }
      console.log(
        `[sync-deal-values] ${dryRun ? 'would-recover' : 'recovered'} deal_id=${dealId} thread_id=${threadId} value=${value} currency=${currency}`,
      )
      summary.recovered++
    }

    cursorId = page[page.length - 1].ID
    if (page.length < batchSize) break
  }

  console.log(
    `[sync-deal-values] done recovered=${summary.recovered} skipped_audit_missing=${summary.skipped.auditMissing} skipped_thread_not_found=${summary.skipped.threadNotFound} skipped_value_null=${summary.skipped.valueNull} skipped_parse_error=${summary.skipped.parseError} scanned=${summary.totalScanned}`,
  )

  return summary
}
