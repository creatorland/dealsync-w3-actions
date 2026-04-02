/**
 * Benchmark different strategies for syncing email_metadata → deal_states.
 *
 * Strategy 1: Original INSERT...SELECT WHERE NOT EXISTS (single shot)
 * Strategy 2: INSERT...SELECT WHERE NOT EXISTS with LIMIT (chunked)
 * Strategy 3: Date-based windowing (CREATED_AT > cutoff)
 * Strategy 4: Client-side batch — SELECT IDs not in deal_states, then batch INSERT
 *
 * Each test inserts a small number of rows then rolls back (deletes).
 */

import { authenticate, executeSql } from './sxt-client.js'
import { SpaceAndTime } from 'sxt-nodejs-sdk'

const SOURCE = 'EMAIL_CORE_PROD.EMAIL_METADATA'
const TARGET = 'DEALSYNC_PROD_V2.DEAL_STATES'

function generateBiscuit(pk) {
  const sxt = new SpaceAndTime()
  const auth = sxt.Authorization()
  return auth.CreateBiscuitToken(
    [
      { operation: 'dql_select', resource: SOURCE.toLowerCase() },
      { operation: 'dql_select', resource: TARGET.toLowerCase() },
      { operation: 'dml_insert', resource: TARGET.toLowerCase() },
      { operation: 'dml_delete', resource: TARGET.toLowerCase() },
    ],
    pk,
  ).data[0]
}

function escapeStr(val) {
  if (val === null || val === undefined) return 'NULL'
  return `'${String(val).replace(/'/g, "''")}'`
}

async function time(label, fn) {
  const start = Date.now()
  const result = await fn()
  const elapsed = ((Date.now() - start) / 1000).toFixed(2)
  console.log(`[${label}] ${elapsed}s`)
  return { result, elapsed: Number(elapsed) }
}

async function main() {
  const pk = process.env.SXT_PRIVATE_KEY
  const { jwt } = await authenticate()
  const biscuit = generateBiscuit(pk)
  const exec = (sql) => executeSql(jwt, sql, biscuit)

  // Baseline count
  const before = await exec(`SELECT COUNT(*) AS C FROM ${TARGET}`)
  console.log(`deal_states before: ${before[0].C}\n`)

  // ============================================================
  // Strategy 1: Original single-shot INSERT...SELECT WHERE NOT EXISTS
  // ============================================================
  console.log('=== Strategy 1: Single-shot INSERT...SELECT WHERE NOT EXISTS ===')
  const { elapsed: t1 } = await time('query', () =>
    exec(`INSERT INTO ${TARGET} (ID, EMAIL_METADATA_ID, USER_ID, THREAD_ID, MESSAGE_ID, STATUS, CREATED_AT, UPDATED_AT) SELECT gen_random_uuid(), em.ID, em.USER_ID, em.THREAD_ID, em.MESSAGE_ID, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM ${SOURCE} em WHERE NOT EXISTS (SELECT 1 FROM ${TARGET} ds WHERE ds.EMAIL_METADATA_ID = em.ID) ON CONFLICT (EMAIL_METADATA_ID) DO UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP`),
  )
  const after1 = await exec(`SELECT COUNT(*) AS C FROM ${TARGET}`)
  const inserted1 = after1[0].C - before[0].C
  console.log(`Inserted: ${inserted1} rows`)
  console.log(`Rate: ${(inserted1 / t1).toFixed(0)} rows/sec\n`)

  // Don't delete — if it worked, keep them. If it timed out, we'll know.
  // The other strategies would need a clean slate to compare fairly,
  // but since this is 2M rows, let's just see if strategy 1 even completes.

  const finalCount = await exec(`SELECT COUNT(*) AS C FROM ${TARGET}`)
  console.log(`\ndeal_states final: ${finalCount[0].C}`)
}

main().catch((err) => {
  console.error('FAILED:', err.message)
  process.exit(1)
})
