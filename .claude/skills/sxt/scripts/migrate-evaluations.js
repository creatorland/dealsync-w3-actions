/**
 * One-time migration: dealsync_v1.email_thread_evaluations → dealsync_prod_v2.email_thread_evaluations
 *
 * Cross-schema INSERT...SELECT silently returns 0 on SxT, so we fetch rows
 * client-side and batch-insert them as VALUES.
 *
 * Usage:
 *   node --experimental-wasm-modules migrate-evaluations.js
 *
 * Env vars (set before running):
 *   SXT_AUTH_URL, SXT_AUTH_SECRET, SXT_PRIVATE_KEY
 */

import { authenticate, executeSql } from './sxt-client.js'
import { SpaceAndTime } from 'sxt-nodejs-sdk'

const SOURCE = 'DEALSYNC_V1.EMAIL_THREAD_EVALUATIONS'
const TARGET = 'DEALSYNC_PROD_V2.EMAIL_THREAD_EVALUATIONS'
const BATCH_SIZE = 1000

function generateBiscuit(privateKey) {
  const sxt = new SpaceAndTime()
  const auth = sxt.Authorization()
  return auth.CreateBiscuitToken(
    [
      { operation: 'dql_select', resource: SOURCE.toLowerCase() },
      { operation: 'dql_select', resource: TARGET.toLowerCase() },
      { operation: 'dml_insert', resource: TARGET.toLowerCase() },
      { operation: 'dml_update', resource: TARGET.toLowerCase() },
    ],
    privateKey,
  ).data[0]
}

function escapeStr(val) {
  if (val === null || val === undefined) return 'NULL'
  return `'${String(val).replace(/'/g, "''")}'`
}

function rowToValues(row) {
  return `(${escapeStr(row.ID)}, ${escapeStr(row.THREAD_ID)}, ${escapeStr(row.AI_EVALUATION_AUDIT_ID)}, ${escapeStr(row.AI_INSIGHT)}, ${escapeStr(row.AI_SUMMARY)}, ${row.IS_DEAL}, ${row.LIKELY_SCAM === null ? 'NULL' : row.LIKELY_SCAM}, ${row.AI_SCORE === null ? 'NULL' : row.AI_SCORE}, ${escapeStr(row.CREATED_AT)}, ${escapeStr(row.UPDATED_AT)})`
}

async function main() {
  const pk = process.env.SXT_PRIVATE_KEY
  if (!pk) {
    console.error('SXT_PRIVATE_KEY not set')
    process.exit(1)
  }

  console.log(`[migrate] Authenticating...`)
  const { jwt } = await authenticate()
  const biscuit = generateBiscuit(pk)

  // Count both tables
  const [srcCount, tgtCount] = await Promise.all([
    executeSql(jwt, `SELECT COUNT(*) AS C FROM ${SOURCE}`, biscuit),
    executeSql(jwt, `SELECT COUNT(*) AS C FROM ${TARGET}`, biscuit),
  ])
  const total = Number(srcCount[0].C)
  console.log(`[migrate] Source: ${total} rows`)
  console.log(`[migrate] Target: ${tgtCount[0].C} rows`)

  let offset = 0
  let inserted = 0
  const start = Date.now()

  while (offset < total) {
    const rows = await executeSql(
      jwt,
      `SELECT ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT FROM ${SOURCE} LIMIT ${BATCH_SIZE} OFFSET ${offset}`,
      biscuit,
    )

    if (!rows || rows.length === 0) break

    const values = rows.map(rowToValues).join(', ')
    const sql = `INSERT INTO ${TARGET} (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT) VALUES ${values}`

    await executeSql(jwt, sql, biscuit)
    inserted += rows.length
    const elapsed = ((Date.now() - start) / 1000).toFixed(0)
    console.log(`[migrate] ${inserted}/${total} (${elapsed}s)`)
    offset += BATCH_SIZE
  }

  // Final count
  const finalCount = await executeSql(jwt, `SELECT COUNT(*) AS C FROM ${TARGET}`, biscuit)
  console.log(`[migrate] Done. Target now has ${finalCount[0].C} rows`)
}

main().catch((err) => {
  console.error(`[migrate] FAILED:`, err.message)
  process.exit(1)
})
