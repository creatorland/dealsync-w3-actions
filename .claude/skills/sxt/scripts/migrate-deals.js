/**
 * One-time migration: dealsync_v1.deals → dealsync_prod_v2.deals
 *
 * Key transform: ID = THREAD_ID (v2 uses thread_id as deals PK)
 * Dedup: keeps latest UPDATED_AT per THREAD_ID
 *
 * Usage:
 *   cd .claude/skills/sxt/scripts
 *   node --experimental-wasm-modules migrate-deals.js
 */

import { authenticate, executeSql } from './sxt-client.js'
import { SpaceAndTime } from 'sxt-nodejs-sdk'

const SOURCE = 'DEALSYNC_V1.DEALS'
const TARGET = 'DEALSYNC_PROD_V2.DEALS'
const BATCH_SIZE = 1000
const FETCH_SIZE = 5000

function generateBiscuit(privateKey) {
  const sxt = new SpaceAndTime()
  const auth = sxt.Authorization()
  return auth.CreateBiscuitToken(
    [
      { operation: 'dql_select', resource: SOURCE.toLowerCase() },
      { operation: 'dql_select', resource: TARGET.toLowerCase() },
      { operation: 'dml_insert', resource: TARGET.toLowerCase() },
    ],
    privateKey,
  ).data[0]
}

function escapeStr(val) {
  if (val === null || val === undefined) return 'NULL'
  return `'${String(val).replace(/'/g, "''")}'`
}

function rowToValues(row) {
  return `(${escapeStr(row.THREAD_ID)}, ${escapeStr(row.USER_ID)}, ${escapeStr(row.THREAD_ID)}, ${escapeStr(row.EMAIL_THREAD_EVALUATION_ID)}, ${escapeStr(row.DEAL_NAME)}, ${escapeStr(row.DEAL_TYPE)}, ${escapeStr(row.CATEGORY)}, ${escapeStr(row.VALUE)}, ${escapeStr(row.CURRENCY)}, ${escapeStr(row.BRAND)}, ${row.IS_AI_SORTED}, ${escapeStr(row.CREATED_AT)}, ${escapeStr(row.UPDATED_AT)})`
}

async function main() {
  const pk = process.env.SXT_PRIVATE_KEY
  if (!pk) {
    console.error('SXT_PRIVATE_KEY not set')
    process.exit(1)
  }

  console.log(`[migrate-deals] Authenticating...`)
  const { jwt } = await authenticate()
  const biscuit = generateBiscuit(pk)

  const [srcCount, tgtCount] = await Promise.all([
    executeSql(jwt, `SELECT COUNT(*) AS C FROM ${SOURCE}`, biscuit),
    executeSql(jwt, `SELECT COUNT(*) AS C FROM ${TARGET}`, biscuit),
  ])
  const total = Number(srcCount[0].C)
  console.log(`[migrate-deals] Source: ${total} rows`)
  console.log(`[migrate-deals] Target: ${tgtCount[0].C} rows`)

  // Fetch all rows (32K is small enough)
  console.log(`[migrate-deals] Fetching all source rows...`)
  const allRows = []
  let offset = 0
  while (offset < total) {
    const rows = await executeSql(
      jwt,
      `SELECT USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT FROM ${SOURCE} LIMIT ${FETCH_SIZE} OFFSET ${offset}`,
      biscuit,
    )
    if (!rows || rows.length === 0) break
    allRows.push(...rows)
    offset += FETCH_SIZE
  }
  console.log(`[migrate-deals] Fetched ${allRows.length} rows`)

  // Dedup: keep latest UPDATED_AT per THREAD_ID
  const byThread = new Map()
  for (const row of allRows) {
    const existing = byThread.get(row.THREAD_ID)
    if (!existing || row.UPDATED_AT > existing.UPDATED_AT) {
      byThread.set(row.THREAD_ID, row)
    }
  }
  const deduped = [...byThread.values()]
  console.log(`[migrate-deals] After dedup: ${deduped.length} unique threads (dropped ${allRows.length - deduped.length} dupes)`)

  // Batch insert
  let inserted = 0
  const start = Date.now()

  for (let i = 0; i < deduped.length; i += BATCH_SIZE) {
    const batch = deduped.slice(i, i + BATCH_SIZE)
    const values = batch.map(rowToValues).join(', ')
    const sql = `INSERT INTO ${TARGET} (ID, USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT) VALUES ${values}`

    await executeSql(jwt, sql, biscuit)
    inserted += batch.length
    const elapsed = ((Date.now() - start) / 1000).toFixed(0)
    console.log(`[migrate-deals] ${inserted}/${deduped.length} (${elapsed}s)`)
  }

  const finalCount = await executeSql(jwt, `SELECT COUNT(*) AS C FROM ${TARGET}`, biscuit)
  console.log(`[migrate-deals] Done. Target now has ${finalCount[0].C} rows`)
}

main().catch((err) => {
  console.error(`[migrate-deals] FAILED:`, err.message)
  process.exit(1)
})
