import { v7 as uuidv7 } from 'uuid'
import * as core from '@actions/core'
import { buildPrompt } from '../lib/prompt.js'
import { callModel, parseAndValidate } from '../lib/ai-client.js'
import { saveResults, sanitizeString, sanitizeSchema, sanitizeId } from '../lib/constants.js'
import { authenticate, executeSql } from '../lib/sxt-client.js'
import { fetchEmails } from '../lib/email-client.js'
import { dealStates as dealStatesSql } from '../lib/sql/index.js'

/**
 * Step 1: Fetch content + call AI + save audit checkpoint.
 *
 * Resilience pipeline:
 *   Layer 0: Primary model call (with HTTP retries + exponential backoff)
 *   Layer 1: Local JSON repair (strip fences, extract array, coerce schema)
 *   Layer 2: Corrective retry (send broken output back to same model with error)
 *   Layer 3: Fallback model (same prompt, different model)
 */
export async function runFetchAndClassify() {
  const authUrl = core.getInput('auth-url')
  const authSecret = core.getInput('auth-secret')
  const apiUrl = core.getInput('api-url')
  const biscuit = core.getInput('biscuit')
  const schema = sanitizeSchema(core.getInput('schema'))
  const batchId = sanitizeId(core.getInput('batch-id'))
  const contentFetcherUrl = core.getInput('content-fetcher-url')
  const hyperbolicKey = core.getInput('hyperbolic-key')
  const primaryModel = core.getInput('primary-model') || 'Qwen/Qwen3-235B-A22B-Instruct-2507'
  const fallbackModel = core.getInput('fallback-model') || 'deepseek-ai/DeepSeek-V3'
  const aiApiUrl = core.getInput('ai-api-url') || 'https://api.hyperbolic.xyz/v1/chat/completions'
  const chunkSize = parseInt(core.getInput('chunk-size') || '10', 10)
  const fetchTimeoutMs = parseInt(core.getInput('fetch-timeout-ms') || '120000', 10)

  if (!batchId) throw new Error('batch-id is required')

  console.log(
    `[classify] starting batch ${batchId} (chunk=${chunkSize}, timeout=${fetchTimeoutMs}ms)`,
  )

  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  // Check metadata exists
  const metadataRows = await exec(dealStatesSql.selectEmailsByBatch(schema, batchId))

  if (!metadataRows || metadataRows.length === 0) {
    console.log('[classify] no rows for batch (already completed?)')
    return { skipped: true, thread_count: 0 }
  }

  console.log(`[classify] ${metadataRows.length} deal_states`)

  // Check for existing audit (checkpoint)
  const existingAudit = await exec(saveResults.getAuditByBatchId(schema, batchId))

  if (existingAudit.length > 0 && existingAudit[0].AI_EVALUATION) {
    console.log('[classify] audit exists — skipping AI call')
    try {
      const parsed = JSON.parse(existingAudit[0].AI_EVALUATION)
      return { skipped: true, thread_count: (parsed.threads || parsed || []).length }
    } catch {
      console.log('[classify] existing audit has invalid JSON, re-running AI')
    }
  }

  // Fetch content
  const messageIds = metadataRows.map((r) => r.MESSAGE_ID)
  const metaByMessageId = new Map(metadataRows.map((r) => [r.MESSAGE_ID, r]))

  const allEmails = await fetchEmails(messageIds, metaByMessageId, {
    contentFetcherUrl,
    userId: metadataRows[0].USER_ID,
    syncStateId: metadataRows[0].SYNC_STATE_ID,
    chunkSize,
    fetchTimeoutMs,
  })

  // Build prompt
  const { systemPrompt, userPrompt } = buildPrompt(allEmails)

  // =========================================================================
  //  AI RESILIENCE PIPELINE (callModel + parseAndValidate from ai-client.js)
  // =========================================================================

  const aiOpts = { apiUrl: aiApiUrl, apiKey: hyperbolicKey }

  const classifyMessages = [
    { role: 'system', content: systemPrompt },
    { role: 'user', content: userPrompt },
  ]

  let threads = null
  let modelUsed = primaryModel

  // --- Layer 0: Primary model call ---
  let primaryRaw
  try {
    const result = await callModel(primaryModel, classifyMessages, { temperature: 0, ...aiOpts })
    primaryRaw = result.content
  } catch (primaryApiError) {
    console.log(`[classify] Primary model API failed: ${primaryApiError.message}`)
    primaryRaw = null
  }

  if (primaryRaw) {
    // --- Layer 1: Local JSON repair ---
    try {
      threads = parseAndValidate(primaryRaw)
      console.log(`[classify] Primary model succeeded: ${threads.length} threads`)
    } catch (parseError) {
      console.log(`[classify] Primary JSON parse failed: ${parseError.message}`)

      // --- Layer 2: Corrective retry (same model, send broken output back) ---
      try {
        console.log(`[classify] Attempting corrective retry with ${primaryModel}`)
        const correctiveMessages = [
          ...classifyMessages,
          { role: 'assistant', content: primaryRaw },
          {
            role: 'user',
            content: `Your previous classification response could not be parsed as valid JSON.\n\nParse error:\n${parseError.message}\n\nPlease return the corrected classification as a valid JSON array. Fix only the JSON formatting issue. Do not change any classification decisions. Return ONLY the JSON array with no other text.`,
          },
        ]
        const corrected = await callModel(primaryModel, correctiveMessages, {
          temperature: 0,
          ...aiOpts,
        })
        const correctedRaw = corrected.content
        threads = parseAndValidate(correctedRaw)
        modelUsed = `${primaryModel}(corrective-retry)`
        console.log(`[classify] Corrective retry succeeded: ${threads.length} threads`)
      } catch (correctiveError) {
        console.log(`[classify] Corrective retry failed: ${correctiveError.message}`)
      }
    }
  }

  // --- Layer 3: Fallback model ---
  if (!threads) {
    console.log(`[classify] Falling back to ${fallbackModel}`)
    modelUsed = fallbackModel
    try {
      const fallbackResult = await callModel(fallbackModel, classifyMessages, {
        temperature: 0.6,
        ...aiOpts,
      })
      const fallbackRaw = fallbackResult.content
      threads = parseAndValidate(fallbackRaw)
      console.log(`[classify] Fallback model succeeded: ${threads.length} threads`)
    } catch (fallbackError) {
      console.error(`[classify] All layers exhausted. Primary and fallback both failed.`)
      throw new Error(
        `Classification failed: primary and fallback models both returned no valid JSON. Last error: ${fallbackError.message}`,
      )
    }
  }

  // Wrap in { threads: [...] } for downstream compatibility
  const aiOutput = { threads }

  // Save audit checkpoint
  const auditId = uuidv7()
  const evaluation = sanitizeString(JSON.stringify(aiOutput).substring(0, 6400))
  try {
    await exec(saveResults.insertAudit(schema, {
      id: auditId,
      batchId,
      threadCount: threads.length,
      emailCount: metadataRows.length,
      cost: 0,
      inputTokens: 0,
      outputTokens: 0,
      model: modelUsed,
      evaluation,
    }))
    console.log(`[classify] audit saved: ${auditId} (model: ${modelUsed})`)
  } catch (err) {
    if (
      err.message.includes('integrity constraint') ||
      err.message.includes('unique') ||
      err.message.includes('duplicate')
    ) {
      console.log(`[classify] audit already exists for batch (concurrent run), continuing`)
    } else {
      throw err
    }
  }

  console.log(`[classify] ${threads.length} threads ready for processing`)
  return { skipped: false, thread_count: threads.length }
}
