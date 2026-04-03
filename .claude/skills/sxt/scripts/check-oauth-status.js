/**
 * Check OAuth status for all users with pending deal_states.
 * Queries Firestore production for valid OAuth tokens.
 *
 * Usage:
 *   cd .claude/skills/sxt/scripts
 *   set -a && source ../../../../.env && set +a
 *   SXT_PRIVATE_KEY="..." node --experimental-wasm-modules check-oauth-status.js
 */

import { authenticate, executeSql } from './sxt-client.js'
import { SpaceAndTime } from 'sxt-nodejs-sdk'
import { execSync } from 'child_process'

const FIRESTORE_PROJECT = 'profilepagev1'
const FIRESTORE_BASE = `https://firestore.googleapis.com/v1/projects/${FIRESTORE_PROJECT}/databases/(default)/documents`

async function main() {
  const pk = process.env.SXT_PRIVATE_KEY
  const { jwt } = await authenticate()
  const sxt = new SpaceAndTime()
  const auth = sxt.Authorization()
  const biscuit = auth.CreateBiscuitToken(
    [{ operation: 'dql_select', resource: 'dealsync_prod_v2.deal_states' }],
    pk,
  ).data[0]

  // Get all users with pending deal_states
  const users = await executeSql(
    jwt,
    "SELECT USER_ID, COUNT(*) AS C FROM DEALSYNC_PROD_V2.DEAL_STATES WHERE STATUS = 'pending' GROUP BY USER_ID ORDER BY C DESC LIMIT 1000",
    biscuit,
  )
  console.log(`[oauth-check] ${users.length} users with pending deal_states`)

  const token = execSync(`gcloud auth print-access-token --project=${FIRESTORE_PROJECT}`, {
    encoding: 'utf-8',
  }).trim()

  const active = []
  const revoked = []
  let checked = 0

  for (const user of users) {
    const userId = user.USER_ID
    const pending = Number(user.C)

    try {
      const resp = await fetch(
        `${FIRESTORE_BASE}/users-sensitive-data/${encodeURIComponent(userId)}/oauth-token/youtube`,
        { headers: { Authorization: `Bearer ${token}` } },
      )

      if (resp.ok) {
        const doc = await resp.json()
        const refreshToken = doc?.fields?.refresh_token?.stringValue
        if (refreshToken) {
          active.push({ userId, pending })
        } else {
          revoked.push({ userId, pending, reason: 'no refresh_token' })
        }
      } else if (resp.status === 404) {
        revoked.push({ userId, pending, reason: 'no oauth doc' })
      } else {
        revoked.push({ userId, pending, reason: `http ${resp.status}` })
      }
    } catch (e) {
      revoked.push({ userId, pending, reason: e.message })
    }

    checked++
    if (checked % 100 === 0) {
      console.log(`[oauth-check] ${checked}/${users.length} — active: ${active.length}, revoked: ${revoked.length}`)
    }
  }

  const activePending = active.reduce((s, u) => s + u.pending, 0)
  const revokedPending = revoked.reduce((s, u) => s + u.pending, 0)

  console.log('\n══════════════════════════════════════════')
  console.log(` OAUTH STATUS`)
  console.log('══════════════════════════════════════════')
  console.log(`  Active:  ${active.length} users (${activePending} pending emails)`)
  console.log(`  Revoked: ${revoked.length} users (${revokedPending} pending emails)`)
  console.log(`  Total:   ${users.length} users`)
  console.log('══════════════════════════════════════════')

  console.log('\nRevoked users (top 20 by pending count):')
  revoked.sort((a, b) => b.pending - a.pending)
  for (const u of revoked.slice(0, 20)) {
    console.log(`  ${u.userId.padEnd(25)} ${String(u.pending).padStart(8)} pending  (${u.reason})`)
  }

  // Output revoked user IDs for downstream use
  console.log('\n--- REVOKED USER IDS ---')
  for (const u of revoked) {
    console.log(u.userId)
  }
}

main().catch((err) => {
  console.error('FAILED:', err.message)
  process.exit(1)
})
