/**
 * One-time migration: dealsync_v1.user_sync_settings → dealsync_prod_v2.user_sync_settings
 *
 * Email resolution:
 *   1. dealsync_v1.email_ingestion_flags (EMAIL_ADDRESS_OF_INBOX) — covers ~693/892
 *   2. Firestore production dealsync-accounts (emailAddressOfInbox) — covers the rest
 *   All emails lowercased.
 *
 * Usage:
 *   cd .claude/skills/sxt/scripts
 *   node --experimental-wasm-modules migrate-user-sync-settings.js
 */

import { authenticate, executeSql } from './sxt-client.js'
import { SpaceAndTime } from 'sxt-nodejs-sdk'
import { execSync } from 'child_process'

const SOURCE = 'DEALSYNC_V1.USER_SYNC_SETTINGS'
const TARGET = 'DEALSYNC_PROD_V2.USER_SYNC_SETTINGS'
const BATCH_SIZE = 100 // smaller batches — rows have many columns
const FIRESTORE_PROJECT = 'profilepagev1'
const FIRESTORE_BASE = `https://firestore.googleapis.com/v1/projects/${FIRESTORE_PROJECT}/databases/(default)/documents`

function generateBiscuit(privateKey) {
  const sxt = new SpaceAndTime()
  const auth = sxt.Authorization()
  return auth.CreateBiscuitToken(
    [
      { operation: 'dql_select', resource: 'dealsync_v1.user_sync_settings' },
      { operation: 'dql_select', resource: 'dealsync_v1.email_ingestion_flags' },
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
  return `(${escapeStr(row.ID)}, ${escapeStr(row.USER_ID)}, ${escapeStr(row.EMAIL)}, ${escapeStr(row.TIMEZONE)}, ${escapeStr(row.TIME_OF_DAY)}, ${escapeStr(row.FREQUENCY)}, ${escapeStr(row.NEXT_SYNC_AT)}, ${row.EMAILS_PROCESSED_SINCE_LAST_SYNC ?? 0}, ${escapeStr(row.LAST_SYNCED_AT)}, ${escapeStr(row.LAST_SYNC_REQUESTED_AT)}, ${escapeStr(row.SYNC_STATUS)}, ${row.SKIP_INBOX}, ${row.DAILY_DIGEST}, ${escapeStr(row.CREATED_AT)}, ${escapeStr(row.UPDATED_AT)})`
}

function getGcloudToken() {
  return execSync(`gcloud auth print-access-token --project=${FIRESTORE_PROJECT}`, {
    encoding: 'utf-8',
  }).trim()
}

async function fetchEmailFromFirestore(token, userId) {
  try {
    const resp = await fetch(`${FIRESTORE_BASE}/dealsync-accounts/${userId}`, {
      headers: { Authorization: `Bearer ${token}` },
    })
    if (!resp.ok) return ''
    const doc = await resp.json()
    return doc?.fields?.emailAddressOfInbox?.stringValue || ''
  } catch {
    return ''
  }
}

async function main() {
  const pk = process.env.SXT_PRIVATE_KEY
  if (!pk) {
    console.error('SXT_PRIVATE_KEY not set')
    process.exit(1)
  }

  console.log(`[migrate-uss] Authenticating...`)
  const { jwt } = await authenticate()
  const biscuit = generateBiscuit(pk)

  // Fetch all user_sync_settings with email_ingestion_flags LEFT JOIN
  console.log(`[migrate-uss] Fetching source data with email flags...`)
  const allRows = []
  let offset = 0
  while (true) {
    const rows = await executeSql(
      jwt,
      `SELECT uss.ID, uss.USER_ID, eif.EMAIL_ADDRESS_OF_INBOX, uss.TIMEZONE, uss.TIME_OF_DAY, uss.FREQUENCY, uss.NEXT_SYNC_AT, uss.EMAILS_PROCESSED_SINCE_LAST_SYNC, uss.LAST_SYNCED_AT, uss.LAST_SYNC_REQUESTED_AT, uss.SYNC_STATUS, uss.SKIP_INBOX, uss.DAILY_DIGEST, uss.CREATED_AT, uss.UPDATED_AT FROM ${SOURCE} uss LEFT JOIN DEALSYNC_V1.EMAIL_INGESTION_FLAGS eif ON eif.USER_ID = uss.USER_ID LIMIT 1000 OFFSET ${offset}`,
      biscuit,
    )
    if (!rows || rows.length === 0) break
    allRows.push(...rows)
    offset += 1000
  }
  console.log(`[migrate-uss] Fetched ${allRows.length} rows`)

  // Identify users missing email
  const missingEmail = allRows.filter(
    (r) => !r.EMAIL_ADDRESS_OF_INBOX || r.EMAIL_ADDRESS_OF_INBOX.trim() === '',
  )
  console.log(
    `[migrate-uss] ${allRows.length - missingEmail.length} have email from flags, ${missingEmail.length} need Firestore lookup`,
  )

  // Fetch missing emails from Firestore
  if (missingEmail.length > 0) {
    console.log(`[migrate-uss] Fetching emails from Firestore (production)...`)
    const token = getGcloudToken()
    let found = 0
    for (let i = 0; i < missingEmail.length; i++) {
      const row = missingEmail[i]
      const email = await fetchEmailFromFirestore(token, row.USER_ID)
      if (email) {
        row.EMAIL_ADDRESS_OF_INBOX = email
        found++
      }
      if ((i + 1) % 50 === 0) {
        console.log(`[migrate-uss] Firestore: ${i + 1}/${missingEmail.length} checked, ${found} found`)
      }
    }
    console.log(
      `[migrate-uss] Firestore: ${found}/${missingEmail.length} emails resolved`,
    )
  }

  // Build final rows with EMAIL field (lowercased)
  const finalRows = allRows.map((row) => ({
    ...row,
    EMAIL: (row.EMAIL_ADDRESS_OF_INBOX || '').trim().toLowerCase(),
  }))

  const withEmail = finalRows.filter((r) => r.EMAIL !== '').length
  const withoutEmail = finalRows.filter((r) => r.EMAIL === '').length
  console.log(`[migrate-uss] Final: ${withEmail} with email, ${withoutEmail} without`)

  // Check target
  const tgtCount = await executeSql(jwt, `SELECT COUNT(*) AS C FROM ${TARGET}`, biscuit)
  console.log(`[migrate-uss] Target currently: ${tgtCount[0].C} rows`)

  // Batch insert
  let inserted = 0
  const start = Date.now()

  for (let i = 0; i < finalRows.length; i += BATCH_SIZE) {
    const batch = finalRows.slice(i, i + BATCH_SIZE)
    const values = batch.map(rowToValues).join(', ')
    const sql = `INSERT INTO ${TARGET} (ID, USER_ID, EMAIL, TIMEZONE, TIME_OF_DAY, FREQUENCY, NEXT_SYNC_AT, EMAILS_PROCESSED_SINCE_LAST_SYNC, LAST_SYNCED_AT, LAST_SYNC_REQUESTED_AT, SYNC_STATUS, SKIP_INBOX, DAILY_DIGEST, CREATED_AT, UPDATED_AT) VALUES ${values}`

    await executeSql(jwt, sql, biscuit)
    inserted += batch.length
    const elapsed = ((Date.now() - start) / 1000).toFixed(0)
    console.log(`[migrate-uss] ${inserted}/${finalRows.length} (${elapsed}s)`)
  }

  const finalCount = await executeSql(jwt, `SELECT COUNT(*) AS C FROM ${TARGET}`, biscuit)
  console.log(`[migrate-uss] Done. Target now has ${finalCount[0].C} rows`)
}

main().catch((err) => {
  console.error(`[migrate-uss] FAILED:`, err.message)
  process.exit(1)
})
