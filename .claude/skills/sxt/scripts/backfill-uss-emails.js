/**
 * Backfill missing emails in dealsync_prod_v2.user_sync_settings
 * from Firestore users collection (production).
 *
 * Approach: delete all, re-migrate with all email sources.
 */

import { authenticate, executeSql } from './sxt-client.js'
import { SpaceAndTime } from 'sxt-nodejs-sdk'
import { execSync } from 'child_process'

const FIRESTORE_PROJECT = 'profilepagev1'
const FIRESTORE_BASE = `https://firestore.googleapis.com/v1/projects/${FIRESTORE_PROJECT}/databases/(default)/documents`
const TARGET = 'DEALSYNC_PROD_V2.USER_SYNC_SETTINGS'
const BATCH_SIZE = 100

function generateBiscuit(pk) {
  const sxt = new SpaceAndTime()
  const auth = sxt.Authorization()
  return auth.CreateBiscuitToken(
    [
      { operation: 'dql_select', resource: 'dealsync_v1.user_sync_settings' },
      { operation: 'dql_select', resource: 'dealsync_v1.email_ingestion_flags' },
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

function rowToValues(row) {
  return `(${escapeStr(row.ID)}, ${escapeStr(row.USER_ID)}, ${escapeStr(row.EMAIL)}, ${escapeStr(row.TIMEZONE)}, ${escapeStr(row.TIME_OF_DAY)}, ${escapeStr(row.FREQUENCY)}, ${escapeStr(row.NEXT_SYNC_AT)}, ${row.EMAILS_PROCESSED_SINCE_LAST_SYNC ?? 0}, ${escapeStr(row.LAST_SYNCED_AT)}, ${escapeStr(row.LAST_SYNC_REQUESTED_AT)}, ${escapeStr(row.SYNC_STATUS)}, ${row.SKIP_INBOX}, ${row.DAILY_DIGEST}, ${escapeStr(row.CREATED_AT)}, ${escapeStr(row.UPDATED_AT)})`
}

async function fetchEmailFromFirestore(token, userId) {
  try {
    const resp = await fetch(
      `${FIRESTORE_BASE}/users/${encodeURIComponent(userId)}`,
      { headers: { Authorization: `Bearer ${token}` } },
    )
    if (!resp.ok) return ''
    const doc = await resp.json()
    return doc?.fields?.email?.stringValue || ''
  } catch {
    return ''
  }
}

async function main() {
  const pk = process.env.SXT_PRIVATE_KEY
  const { jwt } = await authenticate()
  const biscuit = generateBiscuit(pk)

  // 1. Delete all
  console.log('[backfill] Deleting existing rows...')
  const del = await executeSql(jwt, `DELETE FROM ${TARGET} WHERE 1=1`, biscuit)
  console.log('[backfill] Deleted:', JSON.stringify(del))

  // 2. Fetch source with flags JOIN
  console.log('[backfill] Fetching source...')
  const allRows = []
  let offset = 0
  while (true) {
    const rows = await executeSql(
      jwt,
      `SELECT uss.ID, uss.USER_ID, eif.EMAIL_ADDRESS_OF_INBOX, uss.TIMEZONE, uss.TIME_OF_DAY, uss.FREQUENCY, uss.NEXT_SYNC_AT, uss.EMAILS_PROCESSED_SINCE_LAST_SYNC, uss.LAST_SYNCED_AT, uss.LAST_SYNC_REQUESTED_AT, uss.SYNC_STATUS, uss.SKIP_INBOX, uss.DAILY_DIGEST, uss.CREATED_AT, uss.UPDATED_AT FROM DEALSYNC_V1.USER_SYNC_SETTINGS uss LEFT JOIN DEALSYNC_V1.EMAIL_INGESTION_FLAGS eif ON eif.USER_ID = uss.USER_ID LIMIT 1000 OFFSET ${offset}`,
      biscuit,
    )
    if (!rows || rows.length === 0) break
    allRows.push(...rows)
    offset += 1000
  }
  console.log(`[backfill] Fetched ${allRows.length} rows`)

  // 3. Resolve missing emails from Firestore (dealsync-accounts then users)
  const missing = allRows.filter(
    (r) => !r.EMAIL_ADDRESS_OF_INBOX || r.EMAIL_ADDRESS_OF_INBOX.trim() === '',
  )
  console.log(`[backfill] ${allRows.length - missing.length} have email from flags, ${missing.length} need Firestore`)

  if (missing.length > 0) {
    const token = execSync(`gcloud auth print-access-token --project=${FIRESTORE_PROJECT}`, {
      encoding: 'utf-8',
    }).trim()

    let found = 0
    for (let i = 0; i < missing.length; i++) {
      const row = missing[i]

      // Try dealsync-accounts first
      try {
        const resp = await fetch(
          `${FIRESTORE_BASE}/dealsync-accounts/${encodeURIComponent(row.USER_ID)}`,
          { headers: { Authorization: `Bearer ${token}` } },
        )
        if (resp.ok) {
          const doc = await resp.json()
          const email = doc?.fields?.emailAddressOfInbox?.stringValue || ''
          if (email.trim()) {
            row.EMAIL_ADDRESS_OF_INBOX = email
            found++
            continue
          }
        }
      } catch {}

      // Fall back to users collection
      const email = await fetchEmailFromFirestore(token, row.USER_ID)
      if (email.trim()) {
        row.EMAIL_ADDRESS_OF_INBOX = email
        found++
      }

      if ((i + 1) % 50 === 0) {
        console.log(`[backfill] Firestore: ${i + 1}/${missing.length}, found ${found}`)
      }
    }
    console.log(`[backfill] Firestore resolved ${found}/${missing.length}`)
  }

  // 4. Build final rows
  const finalRows = allRows.map((row) => ({
    ...row,
    EMAIL: (row.EMAIL_ADDRESS_OF_INBOX || '').trim().toLowerCase(),
  }))

  const withEmail = finalRows.filter((r) => r.EMAIL !== '').length
  console.log(`[backfill] Final: ${withEmail} with email, ${finalRows.length - withEmail} without`)

  // 5. Batch insert
  let inserted = 0
  const start = Date.now()
  for (let i = 0; i < finalRows.length; i += BATCH_SIZE) {
    const batch = finalRows.slice(i, i + BATCH_SIZE)
    const values = batch.map(rowToValues).join(', ')
    await executeSql(
      jwt,
      `INSERT INTO ${TARGET} (ID, USER_ID, EMAIL, TIMEZONE, TIME_OF_DAY, FREQUENCY, NEXT_SYNC_AT, EMAILS_PROCESSED_SINCE_LAST_SYNC, LAST_SYNCED_AT, LAST_SYNC_REQUESTED_AT, SYNC_STATUS, SKIP_INBOX, DAILY_DIGEST, CREATED_AT, UPDATED_AT) VALUES ${values}`,
      biscuit,
    )
    inserted += batch.length
    console.log(`[backfill] ${inserted}/${finalRows.length} (${((Date.now() - start) / 1000).toFixed(0)}s)`)
  }

  const finalCount = await executeSql(jwt, `SELECT COUNT(*) AS C FROM ${TARGET}`, biscuit)
  const stillEmpty = await executeSql(jwt, `SELECT COUNT(*) AS C FROM ${TARGET} WHERE EMAIL = ''`, biscuit)
  console.log(`[backfill] Done. ${finalCount[0].C} rows, ${stillEmpty[0].C} still without email`)
}

main().catch((err) => {
  console.error('[backfill] FAILED:', err.message)
  process.exit(1)
})
