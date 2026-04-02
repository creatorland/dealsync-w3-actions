/**
 * Check global sync progress for dealsync_prod_v2.
 *
 * Usage:
 *   cd .claude/skills/sxt/scripts
 *   set -a && source ../../../../.env && set +a
 *   SXT_PRIVATE_KEY="f0899ba8..." node --experimental-wasm-modules check-sync-progress.js
 */

import { authenticate, executeSql } from './sxt-client.js'
import { SpaceAndTime } from 'sxt-nodejs-sdk'

async function main() {
  const pk = process.env.SXT_PRIVATE_KEY
  const { jwt } = await authenticate()
  const sxt = new SpaceAndTime()
  const auth = sxt.Authorization()
  const biscuit = auth.CreateBiscuitToken(
    [{ operation: 'dql_select', resource: 'dealsync_prod_v2.deal_states' }],
    pk,
  ).data[0]

  const rows = await executeSql(
    jwt,
    'SELECT STATUS, COUNT(*) AS C FROM DEALSYNC_PROD_V2.DEAL_STATES GROUP BY STATUS ORDER BY C DESC',
    biscuit,
  )

  const counts = {}
  let total = 0
  for (const row of rows) {
    counts[row.STATUS] = Number(row.C)
    total += Number(row.C)
  }

  const pending = counts.pending || 0
  const filtering = counts.filtering || 0
  const classifying = counts.classifying || 0
  const inFlight = filtering + classifying
  const processed = total - pending - inFlight
  const progress = ((processed / total) * 100).toFixed(2)

  console.log('═'.repeat(50))
  console.log(' DEALSYNC PROD V2 — SYNC PROGRESS')
  console.log('═'.repeat(50))
  for (const row of rows) {
    const pct = ((Number(row.C) / total) * 100).toFixed(1)
    console.log(`  ${row.STATUS.padEnd(25)} ${String(row.C).padStart(10)}  (${pct}%)`)
  }
  console.log('─'.repeat(50))
  console.log(`  ${'total'.padEnd(25)} ${String(total).padStart(10)}`)
  console.log('─'.repeat(50))
  console.log(`  ${'in-flight (filtering+classifying)'.padEnd(35)} ${String(inFlight).padStart(5)}`)
  console.log(`  ${'processed (terminal states)'.padEnd(35)} ${String(processed).padStart(5)}`)
  console.log(`  ${'progress'.padEnd(35)} ${progress}%`)
  console.log('═'.repeat(50))
}

main().catch((err) => {
  console.error('FAILED:', err.message)
  process.exit(1)
})
