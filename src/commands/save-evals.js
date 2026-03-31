import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { readAuditThreads, sanitizeId, sanitizeString, sanitizeSchema } from '../lib/constants.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { evaluations as evalSql } from '../lib/sql/index.js'

/**
 * Step 2: Read audit by batch_id → upsert thread evaluations.
 * Batched: single multi-row INSERT ... ON CONFLICT for all threads.
 */
export async function runSaveEvals() {
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
    console.log('[save-evals] no audit found — skipping')
    return { upserted: 0 }
  }

  if (threads.length === 0) {
    console.log('[save-evals] no threads in audit')
    return { upserted: 0 }
  }

  // Build batched VALUES for all threads
  const valueTuples = threads.map((thread) => {
    const threadId = sanitizeId(thread.thread_id)
    const evalId = uuidv7()
    const category = sanitizeString(thread.category || '')
    const aiSummary = sanitizeString(thread.ai_summary || '')
    const isDeal = thread.is_deal ? 'true' : 'false'
    const isLikelyScam =
      (thread.category || '').toLowerCase() === 'likely_scam' ? 'true' : 'false'
    const aiScore = typeof thread.ai_score === 'number' ? thread.ai_score : 0
    return `('${evalId}', '${threadId}', '', '${category}', '${aiSummary}', ${isDeal}, ${isLikelyScam}, ${aiScore}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
  })

  await exec(evalSql.upsert(schema, valueTuples))

  console.log(`[save-evals] upserted ${threads.length} thread evaluations (1 query)`)
  return { upserted: threads.length, total: threads.length }
}
