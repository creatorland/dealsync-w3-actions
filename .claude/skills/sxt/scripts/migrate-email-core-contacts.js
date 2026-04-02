/**
 * One-time migration: dealsync_prod_v2.deal_contacts + dealsync_v1.contacts → email_core_prod.contacts
 *
 * 1. Get unique (USER_ID, EMAIL) from dealsync_prod_v2.deal_contacts
 * 2. Join dealsync_v1.contacts on EMAIL to get NAME, COMPANY_NAME, TITLE, PHONE_NUMBER
 * 3. Insert into email_core_prod.contacts
 */

import { authenticate, executeSql } from './sxt-client.js'
import { SpaceAndTime } from 'sxt-nodejs-sdk'

const TARGET = 'EMAIL_CORE_PROD.CONTACTS'
const BATCH_SIZE = 1000
const FETCH_SIZE = 5000

function generateBiscuit(pk) {
  const sxt = new SpaceAndTime()
  const auth = sxt.Authorization()
  return auth.CreateBiscuitToken(
    [
      { operation: 'dql_select', resource: 'dealsync_prod_v2.deal_contacts' },
      { operation: 'dql_select', resource: 'dealsync_v1.contacts' },
      { operation: 'dql_select', resource: TARGET.toLowerCase() },
      { operation: 'dml_insert', resource: TARGET.toLowerCase() },
    ],
    pk,
  ).data[0]
}

function escapeStr(val) {
  if (val === null || val === undefined) return 'NULL'
  return `'${String(val).replace(/'/g, "''")}'`
}

function rowToValues(row) {
  return `(${escapeStr(row.USER_ID)}, ${escapeStr(row.EMAIL)}, ${escapeStr(row.NAME)}, ${escapeStr(row.COMPANY_NAME)}, ${escapeStr(row.TITLE)}, ${escapeStr(row.PHONE_NUMBER)}, ${escapeStr(row.CREATED_AT)}, ${escapeStr(row.UPDATED_AT)})`
}

async function main() {
  const pk = process.env.SXT_PRIVATE_KEY
  if (!pk) {
    console.error('SXT_PRIVATE_KEY not set')
    process.exit(1)
  }

  console.log('[migrate-contacts] Authenticating...')
  const { jwt } = await authenticate()
  const biscuit = generateBiscuit(pk)

  // 1. Get unique (USER_ID, EMAIL) pairs from deal_contacts
  console.log('[migrate-contacts] Fetching unique (user_id, email) from deal_contacts...')
  const allPairs = []
  let offset = 0
  while (true) {
    const rows = await executeSql(
      jwt,
      `SELECT DISTINCT USER_ID, EMAIL FROM DEALSYNC_PROD_V2.DEAL_CONTACTS LIMIT ${FETCH_SIZE} OFFSET ${offset}`,
      biscuit,
    )
    if (!rows || rows.length === 0) break
    allPairs.push(...rows)
    offset += FETCH_SIZE
    console.log(`[migrate-contacts] Fetched ${allPairs.length} pairs...`)
  }

  // Dedup by (user_id, lower(email))
  const seen = new Map()
  for (const row of allPairs) {
    const email = (row.EMAIL || '').trim().toLowerCase()
    if (!email || !row.USER_ID) continue
    const key = `${row.USER_ID}|${email}`
    if (!seen.has(key)) {
      seen.set(key, { USER_ID: row.USER_ID, EMAIL: email })
    }
  }
  const uniquePairs = [...seen.values()]
  console.log(`[migrate-contacts] ${uniquePairs.length} unique (user_id, email) pairs`)

  // 2. Build a lookup of email → contact metadata from dealsync_v1.contacts
  console.log('[migrate-contacts] Fetching contact metadata from dealsync_v1.contacts...')
  const contactMap = new Map()
  offset = 0
  while (true) {
    const rows = await executeSql(
      jwt,
      `SELECT EMAIL, NAME, COMPANY_NAME, TITLE, PHONE_NUMBER, CREATED_AT, UPDATED_AT FROM DEALSYNC_V1.CONTACTS LIMIT ${FETCH_SIZE} OFFSET ${offset}`,
      biscuit,
    )
    if (!rows || rows.length === 0) break
    for (const row of rows) {
      const email = (row.EMAIL || '').trim().toLowerCase()
      if (email) contactMap.set(email, row)
    }
    offset += FETCH_SIZE
    console.log(`[migrate-contacts] Loaded ${contactMap.size} contacts...`)
  }

  // 3. Merge pairs with metadata
  const finalRows = uniquePairs.map((pair) => {
    const meta = contactMap.get(pair.EMAIL)
    return {
      USER_ID: pair.USER_ID,
      EMAIL: pair.EMAIL,
      NAME: meta?.NAME || null,
      COMPANY_NAME: meta?.COMPANY_NAME || null,
      TITLE: meta?.TITLE || null,
      PHONE_NUMBER: meta?.PHONE_NUMBER || null,
      CREATED_AT: meta?.CREATED_AT || null,
      UPDATED_AT: meta?.UPDATED_AT || null,
    }
  })

  const withMeta = finalRows.filter((r) => r.NAME !== null).length
  console.log(`[migrate-contacts] ${withMeta}/${finalRows.length} have contact metadata`)

  // 4. Batch insert
  const tgtCount = await executeSql(jwt, `SELECT COUNT(*) AS C FROM ${TARGET}`, biscuit)
  console.log(`[migrate-contacts] Target currently: ${tgtCount[0].C} rows`)

  let inserted = 0
  const start = Date.now()

  for (let i = 0; i < finalRows.length; i += BATCH_SIZE) {
    const batch = finalRows.slice(i, i + BATCH_SIZE)
    const values = batch.map(rowToValues).join(', ')
    const sql = `INSERT INTO ${TARGET} (USER_ID, EMAIL, NAME, COMPANY_NAME, TITLE, PHONE_NUMBER, CREATED_AT, UPDATED_AT) VALUES ${values}`

    await executeSql(jwt, sql, biscuit)
    inserted += batch.length
    const elapsed = ((Date.now() - start) / 1000).toFixed(0)
    console.log(`[migrate-contacts] ${inserted}/${finalRows.length} (${elapsed}s)`)
  }

  const finalCount = await executeSql(jwt, `SELECT COUNT(*) AS C FROM ${TARGET}`, biscuit)
  console.log(`[migrate-contacts] Done. Target now has ${finalCount[0].C} rows`)
}

main().catch((err) => {
  console.error('[migrate-contacts] FAILED:', err.message)
  process.exit(1)
})
