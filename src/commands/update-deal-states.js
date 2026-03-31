import * as core from '@actions/core'
import { readAuditThreads, sanitizeId, sanitizeSchema, STATUS } from '../lib/constants.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { dealStates as dealStatesSql } from '../lib/sql/index.js'

/**
 * Step 4: Read audit by batch_id → update deal_states to terminal status.
 * Batched: collects all deal/not_deal email IDs, then issues exactly 2 UPDATEs.
 */
export async function runUpdateDealStates() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchId = sanitizeId(core.getInput('batch-id'))

  if (!batchId) throw new Error('batch-id is required')

  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  const threads = await readAuditThreads(exec, schema, batchId)
  if (!threads) {
    console.log('[update-states] no audit found — skipping')
    return { deal: 0, not_deal: 0 }
  }

  // Get metadata to map thread → email_metadata_ids (by batch_id)
  const metadataRows = await exec(dealStatesSql.selectEmailAndThreadIdsByBatch(schema, batchId))

  const metadataByThread = {}
  for (const row of metadataRows) {
    if (!metadataByThread[row.THREAD_ID]) metadataByThread[row.THREAD_ID] = []
    metadataByThread[row.THREAD_ID].push(row)
  }

  // Collect all deal and not_deal email IDs
  const dealEmailIds = []
  const notDealEmailIds = []

  for (const thread of threads) {
    const threadId = sanitizeId(thread.thread_id)
    const threadEmails = metadataByThread[threadId] || []
    if (threadEmails.length === 0) continue

    const emailIds = threadEmails.map((e) => e.EMAIL_METADATA_ID)
    if (thread.is_deal) {
      dealEmailIds.push(...emailIds)
    } else {
      notDealEmailIds.push(...emailIds)
    }
  }

  // Issue exactly 2 UPDATEs (one for deals, one for not_deals)
  if (dealEmailIds.length > 0) {
    await exec(dealStatesSql.updateStatusByIds(schema, dealEmailIds.map(id => `'${sanitizeId(id)}'`), STATUS.DEAL))
  }
  if (notDealEmailIds.length > 0) {
    await exec(dealStatesSql.updateStatusByIds(schema, notDealEmailIds.map(id => `'${sanitizeId(id)}'`), STATUS.NOT_DEAL))
  }

  const queries = (dealEmailIds.length > 0 ? 1 : 0) + (notDealEmailIds.length > 0 ? 1 : 0)
  console.log(
    `[update-states] ${dealEmailIds.length} → deal, ${notDealEmailIds.length} → not_deal (${queries} queries)`,
  )
  return { deal: dealEmailIds.length, not_deal: notDealEmailIds.length }
}
