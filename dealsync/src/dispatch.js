import * as crypto from 'crypto'
import * as core from '@actions/core'
import { dispatch, STATUS, sanitizeSchema } from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

function parseNonNegativeInt(value, name) {
  const n = parseInt(value, 10)
  if (isNaN(n) || n < 0) throw new Error(`${name} must be non-negative integer, got: ${value}`)
  return n
}

function generateBatchId() {
  // UUIDv7: timestamp-based, sortable
  const now = Date.now()
  const bytes = new Uint8Array(16)
  // Timestamp (48 bits)
  bytes[0] = (now / 2 ** 40) & 0xff
  bytes[1] = (now / 2 ** 32) & 0xff
  bytes[2] = (now / 2 ** 24) & 0xff
  bytes[3] = (now / 2 ** 16) & 0xff
  bytes[4] = (now / 2 ** 8) & 0xff
  bytes[5] = now & 0xff
  // Version 7 + random
  const rand = crypto.randomBytes(10)
  bytes[6] = 0x70 | (rand[0] & 0x0f)
  bytes[7] = rand[1]
  bytes[8] = 0x80 | (rand[2] & 0x3f)
  for (let i = 9; i < 16; i++) bytes[i] = rand[i - 6]
  // Format as UUID
  const hex = Buffer.from(bytes).toString('hex')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}

export async function runDispatch() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const w3RpcUrl = core.getInput('w3-rpc-url')
  const processorName = core.getInput('processor-name') || 'Dealsync Processor'

  const activeFilter = parseNonNegativeInt(core.getInput('active-filter'), 'active-filter')
  const activeDetect = parseNonNegativeInt(core.getInput('active-detect'), 'active-detect')
  const pendingFilter = parseNonNegativeInt(core.getInput('pending-filter'), 'pending-filter')
  const pendingDetect = parseNonNegativeInt(core.getInput('pending-detect'), 'pending-detect')
  const maxFilter = parseNonNegativeInt(core.getInput('max-filter') || '600', 'max-filter')
  const maxDetect = parseNonNegativeInt(core.getInput('max-detect') || '300', 'max-detect')
  const filterBatchSize = parseNonNegativeInt(
    core.getInput('filter-batch-size') || '200',
    'filter-batch-size',
  )
  const detectBatchSize = parseNonNegativeInt(
    core.getInput('detect-batch-size') || '50',
    'detect-batch-size',
  )

  if (pendingFilter === 0 && pendingDetect === 0) {
    core.info('No pending emails to dispatch')
    return { dispatched_filter_count: 0, dispatched_detect_count: 0 }
  }

  core.info('Authenticating...')
  const jwt = await authenticate(authUrl, authSecret)

  let filterSlots = maxFilter - activeFilter
  let detectSlots = maxDetect - activeDetect
  let dispatchedFilter = 0
  let dispatchedDetect = 0

  // Dispatch filter batches: claim first, then trigger
  while (filterSlots > 0 && pendingFilter > 0) {
    const batchId = generateBatchId()

    // 1. Claim batch with generated batch ID
    await executeSql(
      apiUrl,
      jwt,
      biscuit,
      dispatch.claimFilterBatch(schema, batchId, filterBatchSize),
    )

    const rows = await executeSql(apiUrl, jwt, biscuit, dispatch.countClaimed(schema, batchId))
    const claimed = rows[0]?.CNT ?? 0

    if (claimed === 0) break

    // 2. Trigger processor with batch ID so it knows which rows to process
    try {
      const triggerUrl = `${w3RpcUrl}/workflow/${encodeURIComponent(processorName)}/trigger`
      const resp = await fetch(triggerUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          inputs: { batch_type: 'filter', batch_id: batchId },
        }),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      dispatchedFilter++
      core.info(`Filter batch: batchId=${batchId}, claimed=${claimed}`)
    } catch (err) {
      core.error(`Filter trigger failed for ${batchId}: ${err.message}`)
      await executeSql(
        apiUrl,
        jwt,
        biscuit,
        dispatch.resetClaimed(schema, batchId, STATUS.PENDING),
      )
      break
    }

    filterSlots -= claimed
    await sleep(100)
  }

  // Dispatch detection batches: claim first, then trigger
  while (detectSlots > 0 && pendingDetect > 0) {
    const batchId = generateBatchId()

    // 1. Claim batch
    await executeSql(
      apiUrl,
      jwt,
      biscuit,
      dispatch.claimDetectBatch(schema, batchId, detectBatchSize),
    )

    const rows = await executeSql(apiUrl, jwt, biscuit, dispatch.countClaimed(schema, batchId))
    const claimed = rows[0]?.CNT ?? 0

    if (claimed === 0) break

    // 2. Trigger processor
    try {
      const triggerUrl = `${w3RpcUrl}/workflow/${encodeURIComponent(processorName)}/trigger`
      const resp = await fetch(triggerUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          inputs: { batch_type: 'detection', batch_id: batchId },
        }),
      })
      if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`)
      dispatchedDetect++
      core.info(`Detect batch: batchId=${batchId}, claimed=${claimed}`)
    } catch (err) {
      core.error(`Detect trigger failed for ${batchId}: ${err.message}`)
      await executeSql(
        apiUrl,
        jwt,
        biscuit,
        dispatch.resetClaimed(schema, batchId, STATUS.PENDING_CLASSIFICATION),
      )
      break
    }

    detectSlots -= claimed
    await sleep(100)
  }

  return { dispatched_filter_count: dispatchedFilter, dispatched_detect_count: dispatchedDetect }
}
