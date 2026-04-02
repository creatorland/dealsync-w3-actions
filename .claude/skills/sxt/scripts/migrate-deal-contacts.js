/**
 * One-time migration: dealsync_v1.deal_contacts → dealsync_prod_v2.deal_contacts
 *
 * Source (contact_id schema): ID, DEAL_ID, CONTACT_ID, CONTACT_TYPE, IS_FAVORITE
 * Target (email schema): DEAL_ID, USER_ID, EMAIL, CONTACT_TYPE
 *
 * Resolves: DEAL_ID → thread_id (via deals), CONTACT_ID → EMAIL (via contacts),
 *           USER_ID (via deals)
 *
 * Usage:
 *   cd .claude/skills/sxt/scripts
 *   node --experimental-wasm-modules migrate-deal-contacts.js
 */

import { authenticate, executeSql } from './sxt-client.js'
import { SpaceAndTime } from 'sxt-nodejs-sdk'

const TARGET = 'DEALSYNC_PROD_V2.DEAL_CONTACTS'
const BATCH_SIZE = 1000
const FETCH_SIZE = 5000

function generateBiscuit(privateKey) {
  const sxt = new SpaceAndTime()
  const auth = sxt.Authorization()
  return auth.CreateBiscuitToken(
    [
      { operation: 'dql_select', resource: 'dealsync_v1.deal_contacts' },
      { operation: 'dql_select', resource: 'dealsync_v1.contacts' },
      { operation: 'dql_select', resource: 'dealsync_v1.deals' },
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
  return `(${escapeStr(row.DEAL_ID)}, ${escapeStr(row.USER_ID)}, ${escapeStr(row.EMAIL)}, ${escapeStr(row.CONTACT_TYPE)}, ${escapeStr(row.CREATED_AT)}, ${escapeStr(row.UPDATED_AT)})`
}

// Join: deal_contacts → deals (thread_id, user_id) + contacts (email)
const SELECT_SQL = `SELECT d.THREAD_ID AS DEAL_ID, d.USER_ID, c.EMAIL, dc.CONTACT_TYPE, dc.CREATED_AT, dc.UPDATED_AT FROM DEALSYNC_V1.DEAL_CONTACTS dc INNER JOIN DEALSYNC_V1.DEALS d ON d.ID = dc.DEAL_ID INNER JOIN DEALSYNC_V1.CONTACTS c ON c.ID = dc.CONTACT_ID`

async function main() {
  const pk = process.env.SXT_PRIVATE_KEY
  if (!pk) {
    console.error('SXT_PRIVATE_KEY not set')
    process.exit(1)
  }

  console.log(`[migrate-deal-contacts] Authenticating...`)
  const { jwt } = await authenticate()
  const biscuit = generateBiscuit(pk)

  const [srcCount, tgtCount] = await Promise.all([
    executeSql(jwt, 'SELECT COUNT(*) AS C FROM DEALSYNC_V1.DEAL_CONTACTS', biscuit),
    executeSql(jwt, `SELECT COUNT(*) AS C FROM ${TARGET}`, biscuit),
  ])
  console.log(`[migrate-deal-contacts] Source: ${srcCount[0].C} rows`)
  console.log(`[migrate-deal-contacts] Target: ${tgtCount[0].C} rows`)

  // Fetch all with JOINs
  console.log(`[migrate-deal-contacts] Fetching with JOINs...`)
  const allRows = []
  let offset = 0
  while (true) {
    const rows = await executeSql(jwt, `${SELECT_SQL} LIMIT ${FETCH_SIZE} OFFSET ${offset}`, biscuit)
    if (!rows || rows.length === 0) break
    allRows.push(...rows)
    offset += FETCH_SIZE
    console.log(`[migrate-deal-contacts] Fetched ${allRows.length} rows...`)
  }
  console.log(`[migrate-deal-contacts] Total fetched: ${allRows.length} rows`)

  // Dedup by PK (DEAL_ID, USER_ID, EMAIL)
  const seen = new Map()
  for (const row of allRows) {
    const key = `${row.DEAL_ID}|${row.USER_ID}|${row.EMAIL}`
    const existing = seen.get(key)
    if (!existing || row.UPDATED_AT > existing.UPDATED_AT) {
      seen.set(key, row)
    }
  }
  const deduped = [...seen.values()]
  const dropped = allRows.length - deduped.length
  if (dropped > 0) {
    console.log(`[migrate-deal-contacts] Deduped: ${deduped.length} unique (dropped ${dropped})`)
  }

  // Batch insert
  let inserted = 0
  const start = Date.now()

  for (let i = 0; i < deduped.length; i += BATCH_SIZE) {
    const batch = deduped.slice(i, i + BATCH_SIZE)
    const values = batch.map(rowToValues).join(', ')
    const sql = `INSERT INTO ${TARGET} (DEAL_ID, USER_ID, EMAIL, CONTACT_TYPE, CREATED_AT, UPDATED_AT) VALUES ${values}`

    await executeSql(jwt, sql, biscuit)
    inserted += batch.length
    const elapsed = ((Date.now() - start) / 1000).toFixed(0)
    console.log(`[migrate-deal-contacts] ${inserted}/${deduped.length} (${elapsed}s)`)
  }

  const finalCount = await executeSql(jwt, `SELECT COUNT(*) AS C FROM ${TARGET}`, biscuit)
  console.log(`[migrate-deal-contacts] Done. Target now has ${finalCount[0].C} rows`)
}

main().catch((err) => {
  console.error(`[migrate-deal-contacts] FAILED:`, err.message)
  process.exit(1)
})
