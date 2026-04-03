import { randomUUID } from 'crypto'
import { authenticate, executeSql } from './sxt-client.js'
import { SpaceAndTime } from 'sxt-nodejs-sdk'

const pk = process.env.SXT_PRIVATE_KEY
const { jwt } = await authenticate()
const sxt = new SpaceAndTime()
const auth = sxt.Authorization()
const biscuit = auth.CreateBiscuitToken(
  [
    { operation: 'dql_select', resource: 'dealsync_v1.email_thread_evaluations' },
    { operation: 'dql_select', resource: 'dealsync_prod_v2.email_thread_evaluations' },
    { operation: 'dml_insert', resource: 'dealsync_prod_v2.email_thread_evaluations' },
  ],
  pk,
).data[0]

const exec = (sql) => executeSql(jwt, sql, biscuit)

function escapeStr(val) {
  if (val === null || val === undefined) return 'NULL'
  return "'" + String(val).replace(/'/g, "''") + "'"
}

// Fetch all from v1
console.log('Fetching from dealsync_v1...')
const allRows = []
let offset = 0
while (true) {
  const rows = await exec(
    `SELECT ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT FROM DEALSYNC_V1.EMAIL_THREAD_EVALUATIONS LIMIT 5000 OFFSET ${offset}`,
  )
  if (!rows || rows.length === 0) break
  allRows.push(...rows)
  offset += 5000
  if (offset % 50000 === 0) console.log(`Fetched ${allRows.length}...`)
}
console.log(`Fetched ${allRows.length} rows`)

// Dedup by THREAD_ID — keep latest
const byThread = new Map()
for (const row of allRows) {
  const existing = byThread.get(row.THREAD_ID)
  if (!existing || (row.UPDATED_AT || '') > (existing.UPDATED_AT || '')) {
    byThread.set(row.THREAD_ID, row)
  }
}
const deduped = [...byThread.values()]
console.log(`Deduped: ${deduped.length} unique (dropped ${allRows.length - deduped.length})`)

// Insert in batches
let inserted = 0
const BATCH = 500
const start = Date.now()
for (let i = 0; i < deduped.length; i += BATCH) {
  const batch = deduped.slice(i, i + BATCH)
  const values = batch
    .map(
      (r) =>
        `(gen_random_uuid(), ${escapeStr(r.THREAD_ID)}, ${escapeStr(r.AI_EVALUATION_AUDIT_ID)}, ${escapeStr(r.AI_INSIGHT)}, ${escapeStr(r.AI_SUMMARY)}, ${r.IS_DEAL}, ${r.LIKELY_SCAM === null ? 'NULL' : r.LIKELY_SCAM}, ${r.AI_SCORE === null ? 'NULL' : r.AI_SCORE}, ${escapeStr(r.CREATED_AT)}, ${escapeStr(r.UPDATED_AT)})`,
    )
    .join(', ')
  await exec(
    `INSERT INTO DEALSYNC_PROD_V2.EMAIL_THREAD_EVALUATIONS (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT) VALUES ${values} ON CONFLICT (THREAD_ID) DO UPDATE SET AI_INSIGHT = EXCLUDED.AI_INSIGHT, AI_SUMMARY = EXCLUDED.AI_SUMMARY, IS_DEAL = EXCLUDED.IS_DEAL, UPDATED_AT = EXCLUDED.UPDATED_AT`,
  )
  inserted += batch.length
  const elapsed = ((Date.now() - start) / 1000).toFixed(0)
  console.log(`${inserted}/${deduped.length} (${elapsed}s)`)
}

const final = await exec(
  'SELECT COUNT(*) AS C FROM DEALSYNC_PROD_V2.EMAIL_THREAD_EVALUATIONS',
)
console.log(`Done. ${final[0].C} rows`)
