import * as core from '@actions/core'
import { workflowTriggers, sanitizeSchema, sanitizeString } from '../../shared/queries.js'
import { authenticate, executeSql } from './sxt-client.js'

/**
 * Append or update workflow_triggers trail on deal_states claimed by a batch ID.
 *
 * Actions:
 *   - "start": Append a new entry with success=false (called at processor start)
 *   - "complete": Update the last entry matching this batch_id to success=true
 */
export async function runWorkflowTriggers() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))

  const action = core.getInput('trigger-action') // 'start' or 'complete'
  const batchId = core.getInput('batch-id')
  const triggerType = core.getInput('trigger-type') || 'filter' // 'filter' or 'detection'

  if (!batchId) throw new Error('batch-id is required')
  if (!['start', 'complete'].includes(action)) {
    throw new Error(`trigger-action must be "start" or "complete", got: ${action}`)
  }

  const jwt = await authenticate(authUrl, authSecret)

  // Fetch current workflow_triggers for claimed rows
  const rows = await executeSql(
    apiUrl,
    jwt,
    biscuit,
    workflowTriggers.fetchByBatchId(schema, batchId),
  )

  let updated = 0
  for (const row of rows) {
    const emailMetadataId = row.EMAIL_METADATA_ID
    let triggers = []
    try {
      triggers = row.WORKFLOW_TRIGGERS ? JSON.parse(row.WORKFLOW_TRIGGERS) : []
    } catch {
      triggers = []
    }

    if (action === 'start') {
      triggers.push({
        type: triggerType,
        batch_id: batchId,
        timestamp: new Date().toISOString(),
        success: false,
      })
    } else {
      // complete: find last entry with this batch_id and set success=true
      for (let i = triggers.length - 1; i >= 0; i--) {
        if (triggers[i].batch_id === batchId) {
          triggers[i].success = true
          break
        }
      }
    }

    const serialized = sanitizeString(JSON.stringify(triggers))
    await executeSql(
      apiUrl,
      jwt,
      biscuit,
      workflowTriggers.update(schema, emailMetadataId, serialized),
    )
    updated++
  }

  core.info(`workflow-triggers ${action}: updated ${updated} deal_states`)
  return { updated }
}
