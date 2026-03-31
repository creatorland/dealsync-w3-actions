import * as core from '@actions/core'
import { sanitizeSchema, sanitizeId } from '../lib/constants.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { isRejected } from '../lib/filter-rules.js'
import { fetchEmails } from '../lib/email-client.js'
import { dealStates as dealStatesSql } from '../lib/sql/index.js'

/**
 * Combined fetch-headers + filter command.
 * Fetches email headers (metadata only, no body) from content fetcher,
 * applies filter rules, and returns filtered/rejected ID lists.
 */
export async function runFetchAndFilter() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchId = sanitizeId(core.getInput('batch-id'))
  const chunkSize = parseInt(core.getInput('chunk-size') || '50', 10)
  const fetchTimeoutMs = parseInt(core.getInput('fetch-timeout-ms') || '30000', 10)

  if (!batchId) throw new Error('batch-id is required')

  console.log(
    `[fetch-and-filter] starting for batch ${batchId} (chunk=${chunkSize}, timeout=${fetchTimeoutMs}ms)`,
  )

  // 1. Authenticate + fetch metadata from SxT
  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)
  const metadataRows = await exec(dealStatesSql.selectEmailsByBatch(schema, batchId))

  if (!metadataRows || metadataRows.length === 0) {
    console.log('[fetch-and-filter] no rows found for batch')
    return { filtered_ids: '', rejected_ids: '', total: 0 }
  }

  console.log(`[fetch-and-filter] found ${metadataRows.length} deal_states`)

  // 2. Fetch headers only from content fetcher (format=metadata, no body)
  const contentFetcherUrl = core.getInput('content-fetcher-url')
  const messageIds = metadataRows.map((r) => r.MESSAGE_ID)
  const metaByMessageId = new Map(metadataRows.map((r) => [r.MESSAGE_ID, r]))

  const allEmails = await fetchEmails(messageIds, metaByMessageId, {
    contentFetcherUrl,
    userId: metadataRows[0].USER_ID,
    syncStateId: metadataRows[0].SYNC_STATE_ID,
    chunkSize,
    fetchTimeoutMs,
    format: 'metadata',
  })

  console.log(`[fetch-and-filter] fetched ${allEmails.length} emails, applying filter rules`)

  // 3. Apply filter rules
  const filteredIds = []
  const rejectedIds = []

  for (const email of allEmails) {
    if (isRejected(email)) {
      rejectedIds.push(email.id)
    } else {
      filteredIds.push(email.id)
    }
  }

  console.log(
    `[fetch-and-filter] result: ${filteredIds.length} passed, ${rejectedIds.length} rejected`,
  )

  return {
    filtered_ids: filteredIds.map((id) => `'${sanitizeId(id)}'`).join(','),
    rejected_ids: rejectedIds.map((id) => `'${sanitizeId(id)}'`).join(','),
    total: allEmails.length,
  }
}
