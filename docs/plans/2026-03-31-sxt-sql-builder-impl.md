# SxT SQL Builder Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Centralize all 40+ inline SQL statements into `src/lib/sql.js` — pure functions that return SQL strings — so business logic never constructs SQL directly.

**Architecture:** A single module (`src/lib/sql.js`) exports namespaced builder objects (e.g., `dealStates.claimBatch(...)`, `deals.upsert(...)`). Each function accepts typed params and returns a complete SQL string. The existing sanitization utilities (`sanitizeId`, `sanitizeString`, `sanitizeSchema`, `toSqlIdList`) stay in `queries.js` and are imported by `sql.js`. All callers replace inline SQL template literals with builder calls. TDD: tests first for the builder, then mechanical swap in each consumer.

**Tech Stack:** Node 24 ESM, Jest (--experimental-vm-modules), no new dependencies

---

## Task 1: Create `src/lib/sql.js` — DEAL_STATES builders

**Files:**

- Create: `src/lib/sql.js`
- Test: `__tests__/sql.test.js`

This task covers the DEAL_STATES table, which has the most query variants (14 distinct patterns across the codebase).

**Step 1: Write failing tests for DEAL_STATES builders**

```js
// __tests__/sql.test.js
import { dealStates } from '../src/lib/sql.js'

describe('dealStates', () => {
  const S = 'TEST_SCHEMA'

  describe('claimFilterBatch', () => {
    it('produces UPDATE with subquery LIMIT', () => {
      const sql = dealStates.claimFilterBatch(S, 'batch-123', 200)
      expect(sql).toContain(`UPDATE ${S}.DEAL_STATES`)
      expect(sql).toContain("SET STATUS = 'filtering'")
      expect(sql).toContain("BATCH_ID = 'batch-123'")
      expect(sql).toContain("STATUS = 'pending'")
      expect(sql).toContain('LIMIT 200')
    })
  })

  describe('claimClassifyBatch', () => {
    it('produces thread-aware UPDATE with NOT EXISTS', () => {
      const sql = dealStates.claimClassifyBatch(S, 'batch-456', 5)
      expect(sql).toContain("SET STATUS = 'classifying'")
      expect(sql).toContain("BATCH_ID = 'batch-456'")
      expect(sql).toContain("STATUS = 'pending_classification'")
      expect(sql).toContain('NOT EXISTS')
      expect(sql).toContain('LIMIT 5')
    })
  })

  describe('selectByBatch', () => {
    it('selects rows by batch ID', () => {
      const sql = dealStates.selectByBatch(S, 'batch-123')
      expect(sql).toContain(`SELECT EMAIL_METADATA_ID`)
      expect(sql).toContain(`FROM ${S}.DEAL_STATES`)
      expect(sql).toContain("BATCH_ID = 'batch-123'")
    })
  })

  describe('selectByBatchWithJoins', () => {
    it('includes LEFT JOINs for evaluations and user sync settings', () => {
      const sql = dealStates.selectByBatchWithJoins(S, 'batch-789')
      expect(sql).toContain('LEFT JOIN')
      expect(sql).toContain('EMAIL_THREAD_EVALUATIONS')
      expect(sql).toContain('USER_SYNC_SETTINGS')
      expect(sql).toContain("BATCH_ID = 'batch-789'")
    })
  })

  describe('selectMetadataByBatch', () => {
    it('selects EMAIL_METADATA_ID and THREAD_ID', () => {
      const sql = dealStates.selectMetadataByBatch(S, 'batch-123')
      expect(sql).toContain('SELECT EMAIL_METADATA_ID, THREAD_ID')
      expect(sql).toContain("BATCH_ID = 'batch-123'")
    })

    it('selects DISTINCT THREAD_ID and USER_ID when distinct=true', () => {
      const sql = dealStates.selectDistinctThreadUsers(S, 'batch-123')
      expect(sql).toContain('SELECT DISTINCT THREAD_ID, USER_ID')
      expect(sql).toContain("BATCH_ID = 'batch-123'")
    })
  })

  describe('updateStatusByIds', () => {
    it('updates status for a list of IDs', () => {
      const sql = dealStates.updateStatusByIds(S, ["'id-1'", "'id-2'"], 'deal')
      expect(sql).toContain("SET STATUS = 'deal'")
      expect(sql).toContain("'id-1'")
      expect(sql).toContain("'id-2'")
    })
  })

  describe('updateStatusByBatch', () => {
    it('updates status filtered by batch and current status', () => {
      const sql = dealStates.updateStatusByBatch(S, 'batch-1', 'filtering', 'failed')
      expect(sql).toContain("SET STATUS = 'failed'")
      expect(sql).toContain("BATCH_ID = 'batch-1'")
      expect(sql).toContain("STATUS = 'filtering'")
    })
  })

  describe('touchBatch', () => {
    it('updates UPDATED_AT for a batch', () => {
      const sql = dealStates.touchBatch(S, 'batch-1')
      expect(sql).toContain('SET UPDATED_AT = CURRENT_TIMESTAMP')
      expect(sql).toContain("BATCH_ID = 'batch-1'")
    })
  })

  describe('findStuckBatches', () => {
    it('finds batches with attempts < maxRetries', () => {
      const sql = dealStates.findStuckBatches(S, 'classifying', 5, 6)
      expect(sql).toContain("STATUS = 'classifying'")
      expect(sql).toContain("INTERVAL '5' MINUTE")
      expect(sql).toContain('HAVING COUNT(DISTINCT be.TRIGGER_HASH) < 6')
      expect(sql).toContain('LIMIT 1')
    })
  })

  describe('findDeadBatches', () => {
    it('finds batches with attempts >= maxRetries', () => {
      const sql = dealStates.findDeadBatches(S, 'filtering', 5, 6)
      expect(sql).toContain("STATUS = 'filtering'")
      expect(sql).toContain('HAVING COUNT(DISTINCT be.TRIGGER_HASH) >= 6')
    })
  })

  describe('countByBatchAndStatus', () => {
    it('counts rows by batch and status', () => {
      const sql = dealStates.countByBatchAndStatus(S, 'batch-1', 'filtering')
      expect(sql).toContain('SELECT COUNT(*) AS C')
      expect(sql).toContain("BATCH_ID = 'batch-1'")
      expect(sql).toContain("STATUS = 'filtering'")
    })
  })

  describe('countOrphaned', () => {
    it('counts stale unbatched rows', () => {
      const sql = dealStates.countOrphaned(S, ['pending_classification'], 30)
      expect(sql).toContain("STATUS IN ('pending_classification')")
      expect(sql).toContain('BATCH_ID IS NULL')
      expect(sql).toContain("INTERVAL '30' MINUTE")
    })
  })

  describe('failOrphaned', () => {
    it('marks stale unbatched rows as failed', () => {
      const sql = dealStates.failOrphaned(S, ['pending_classification'], 30)
      expect(sql).toContain("SET STATUS = 'failed'")
      expect(sql).toContain('BATCH_ID IS NULL')
      expect(sql).toContain("INTERVAL '30' MINUTE")
    })
  })

  describe('syncFromEmailMetadata', () => {
    it('inserts missing rows from EMAIL_METADATA', () => {
      const sql = dealStates.syncFromEmailMetadata(S, 'EMAIL_CORE_STAGING')
      expect(sql).toContain(`INSERT INTO ${S}.DEAL_STATES`)
      expect(sql).toContain('FROM EMAIL_CORE_STAGING.EMAIL_METADATA')
      expect(sql).toContain('NOT EXISTS')
      expect(sql).toContain('ON CONFLICT (EMAIL_METADATA_ID)')
    })
  })

  describe('SQL injection prevention', () => {
    it('sanitizeId rejects malicious batch IDs', () => {
      // Builder functions use sanitizeId internally
      expect(() => dealStates.claimFilterBatch(S, "'; DROP TABLE --", 10)).toThrow('Invalid ID')
    })

    it('sanitizeSchema rejects malicious schema names', () => {
      expect(() => dealStates.claimFilterBatch('BAD SCHEMA; --', 'b1', 10)).toThrow(
        'Invalid schema',
      )
    })
  })
})
```

**Step 2: Run tests to verify they fail**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql.test.js`
Expected: FAIL — `Cannot find module '../src/lib/sql.js'`

**Step 3: Write `src/lib/sql.js` — DEAL_STATES section**

```js
// src/lib/sql.js
//
// SxT-aware SQL builder. Pure functions: params in, SQL string out.
// No database connection, no side effects, no dependencies beyond queries.js.
//
// SxT constraints:
//   - No CTEs (WITH ... AS)
//   - No division operator
//   - INTERVAL syntax: INTERVAL 'N' MINUTE
//   - ON CONFLICT (...) DO UPDATE supported
//   - LEFT JOIN on single column only
//   - No outer JOINs

import { sanitizeId, sanitizeString, sanitizeSchema, toSqlIdList } from './queries.js'

// ─── DEAL_STATES ────────────────────────────────────────────────────────────

export const dealStates = {
  /**
   * Claim pending rows for filtering.
   * Atomic UPDATE with subquery (no CTE).
   */
  claimFilterBatch: (schema, batchId, batchSize) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = 'filtering', BATCH_ID = '${bid}', UPDATED_AT = CURRENT_TIMESTAMP WHERE EMAIL_METADATA_ID IN (SELECT EMAIL_METADATA_ID FROM ${s}.DEAL_STATES WHERE STATUS = 'pending' LIMIT ${Number(batchSize)})`
  },

  /**
   * Claim pending_classification rows for classify.
   * Thread-aware: excludes threads with rows still in pending/filtering.
   */
  claimClassifyBatch: (schema, batchId, batchSize) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = 'classifying', BATCH_ID = '${bid}', UPDATED_AT = CURRENT_TIMESTAMP WHERE THREAD_ID IN (SELECT DISTINCT ds.THREAD_ID FROM ${s}.DEAL_STATES ds WHERE ds.STATUS = 'pending_classification' AND NOT EXISTS (SELECT 1 FROM ${s}.DEAL_STATES ds2 WHERE ds2.THREAD_ID = ds.THREAD_ID AND ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID AND ds2.STATUS IN ('pending', 'filtering')) LIMIT ${Number(batchSize)}) AND STATUS = 'pending_classification'`
  },

  /** Select basic columns by batch ID. */
  selectByBatch: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID FROM ${s}.DEAL_STATES WHERE BATCH_ID = '${bid}'`
  },

  /** Select by batch with LEFT JOINs for previous eval + user email (classify pipeline). */
  selectByBatchWithJoins: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT ds.EMAIL_METADATA_ID, ds.MESSAGE_ID, ds.USER_ID, ds.THREAD_ID, ds.SYNC_STATE_ID, ete.AI_SUMMARY AS PREVIOUS_AI_SUMMARY, ete.IS_DEAL AS PREVIOUS_IS_DEAL, uss.EMAIL AS CREATOR_EMAIL FROM ${s}.DEAL_STATES ds LEFT JOIN ${s}.EMAIL_THREAD_EVALUATIONS ete ON ete.THREAD_ID = ds.THREAD_ID LEFT JOIN ${s}.USER_SYNC_SETTINGS uss ON uss.USER_ID = ds.USER_ID WHERE ds.BATCH_ID = '${bid}'`
  },

  /** Select EMAIL_METADATA_ID, THREAD_ID by batch (for update-deal-states). */
  selectMetadataByBatch: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT EMAIL_METADATA_ID, THREAD_ID FROM ${s}.DEAL_STATES WHERE BATCH_ID = '${bid}'`
  },

  /** Select DISTINCT THREAD_ID, USER_ID by batch (for save-deals). */
  selectDistinctThreadUsers: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT DISTINCT THREAD_ID, USER_ID FROM ${s}.DEAL_STATES WHERE BATCH_ID = '${bid}'`
  },

  /** Update status for a pre-quoted list of EMAIL_METADATA_IDs. */
  updateStatusByIds: (schema, quotedIds, status) => {
    const s = sanitizeSchema(schema)
    const st = sanitizeString(status)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = '${st}' WHERE EMAIL_METADATA_ID IN (${quotedIds.join(',')})`
  },

  /** Update status for a pre-quoted list of EMAIL_METADATA_IDs, with UPDATED_AT. */
  updateStatusByIdsWithTimestamp: (schema, quotedIds, status) => {
    const s = sanitizeSchema(schema)
    const st = sanitizeString(status)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = '${st}', UPDATED_AT = CURRENT_TIMESTAMP WHERE EMAIL_METADATA_ID IN (${quotedIds.join(',')})`
  },

  /** Update status filtered by batch ID and current status. */
  updateStatusByBatch: (schema, batchId, fromStatus, toStatus) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    const from = sanitizeString(fromStatus)
    const to = sanitizeString(toStatus)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = '${to}', UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${bid}' AND STATUS = '${from}'`
  },

  /** Touch UPDATED_AT to extend lease. */
  touchBatch: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `UPDATE ${s}.DEAL_STATES SET UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${bid}'`
  },

  /** Find stuck batches with attempts < maxRetries (eligible for retry). */
  findStuckBatches: (schema, status, intervalMinutes, maxRetries) => {
    const s = sanitizeSchema(schema)
    const st = sanitizeString(status)
    return `SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS ATTEMPTS FROM ${s}.DEAL_STATES ds LEFT JOIN ${s}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID WHERE ds.STATUS = '${st}' AND ds.BATCH_ID IS NOT NULL AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(intervalMinutes)}' MINUTE GROUP BY ds.BATCH_ID HAVING COUNT(DISTINCT be.TRIGGER_HASH) < ${Number(maxRetries)} LIMIT 1`
  },

  /** Find dead batches with attempts >= maxRetries (dead-letter candidates). */
  findDeadBatches: (schema, status, intervalMinutes, maxRetries) => {
    const s = sanitizeSchema(schema)
    const st = sanitizeString(status)
    return `SELECT ds.BATCH_ID FROM ${s}.DEAL_STATES ds LEFT JOIN ${s}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID WHERE ds.STATUS = '${st}' AND ds.BATCH_ID IS NOT NULL AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(intervalMinutes)}' MINUTE GROUP BY ds.BATCH_ID HAVING COUNT(DISTINCT be.TRIGGER_HASH) >= ${Number(maxRetries)}`
  },

  /** Count rows by batch and status. */
  countByBatchAndStatus: (schema, batchId, status) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    const st = sanitizeString(status)
    return `SELECT COUNT(*) AS C FROM ${s}.DEAL_STATES WHERE BATCH_ID = '${bid}' AND STATUS = '${st}'`
  },

  /** Count stale unbatched rows (orphan detection). */
  countOrphaned: (schema, statuses, staleMinutes) => {
    const s = sanitizeSchema(schema)
    const literals = statuses.map((st) => `'${sanitizeString(st)}'`).join(',')
    return `SELECT COUNT(*) AS C FROM ${s}.DEAL_STATES WHERE STATUS IN (${literals}) AND BATCH_ID IS NULL AND UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(staleMinutes)}' MINUTE`
  },

  /** Mark stale unbatched rows as failed. */
  failOrphaned: (schema, statuses, staleMinutes) => {
    const s = sanitizeSchema(schema)
    const literals = statuses.map((st) => `'${sanitizeString(st)}'`).join(',')
    return `UPDATE ${s}.DEAL_STATES SET STATUS = 'failed', UPDATED_AT = CURRENT_TIMESTAMP WHERE STATUS IN (${literals}) AND BATCH_ID IS NULL AND UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(staleMinutes)}' MINUTE`
  },

  /** Sync new EMAIL_METADATA rows into DEAL_STATES as 'pending'. */
  syncFromEmailMetadata: (schema, emailCoreSchema) => {
    const s = sanitizeSchema(schema)
    const ecs = sanitizeSchema(emailCoreSchema)
    return `INSERT INTO ${s}.DEAL_STATES (ID, EMAIL_METADATA_ID, USER_ID, THREAD_ID, MESSAGE_ID, STATUS, CREATED_AT, UPDATED_AT) SELECT gen_random_uuid(), em.ID, em.USER_ID, em.THREAD_ID, em.MESSAGE_ID, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM ${ecs}.EMAIL_METADATA em WHERE NOT EXISTS (SELECT 1 FROM ${s}.DEAL_STATES ds WHERE ds.EMAIL_METADATA_ID = em.ID) ON CONFLICT (EMAIL_METADATA_ID) DO UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP`
  },
}
```

**Step 4: Run tests to verify they pass**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql.test.js`
Expected: PASS — all dealStates tests green

**Step 5: Commit**

```bash
git add src/lib/sql.js __tests__/sql.test.js
git commit -m "feat: add sql.js builder — DEAL_STATES queries"
```

---

## Task 2: Add remaining table builders to `src/lib/sql.js`

**Files:**

- Modify: `src/lib/sql.js`
- Modify: `__tests__/sql.test.js`

Covers: BATCH_EVENTS, AI_EVALUATION_AUDITS, EMAIL_THREAD_EVALUATIONS, DEALS, DEAL_CONTACTS, CONTACTS

**Step 1: Write failing tests for remaining builders**

Add to `__tests__/sql.test.js`:

```js
import {
  batchEvents,
  aiEvaluationAudits,
  emailThreadEvaluations,
  deals,
  dealContacts,
  contacts,
} from '../src/lib/sql.js'

describe('batchEvents', () => {
  const S = 'TEST_SCHEMA'

  it('produces INSERT with ON CONFLICT for single event', () => {
    const sql = batchEvents.upsert(S, 'trigger-1', 'batch-1', 'classify', 'new')
    expect(sql).toContain(`INSERT INTO ${S}.BATCH_EVENTS`)
    expect(sql).toContain("'trigger-1'")
    expect(sql).toContain("'batch-1'")
    expect(sql).toContain('ON CONFLICT (TRIGGER_HASH)')
  })

  it('produces VALUES for pre-built batch event tuples', () => {
    const sql = batchEvents.upsertValues(S, [
      "('hash1', 'batch1', 'classify', 'complete', CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain(`INSERT INTO ${S}.BATCH_EVENTS`)
    expect(sql).toContain('hash1')
    expect(sql).toContain('ON CONFLICT (TRIGGER_HASH)')
  })
})

describe('aiEvaluationAudits', () => {
  const S = 'TEST_SCHEMA'

  it('selects audit by batch ID', () => {
    const sql = aiEvaluationAudits.selectByBatch(S, 'batch-1')
    expect(sql).toContain('SELECT AI_EVALUATION')
    expect(sql).toContain(`FROM ${S}.AI_EVALUATION_AUDITS`)
    expect(sql).toContain("BATCH_ID = 'batch-1'")
  })

  it('inserts audit row', () => {
    const sql = aiEvaluationAudits.insert(S, {
      id: 'aud-1',
      batchId: 'batch-1',
      threadCount: 5,
      emailCount: 12,
      cost: 0,
      inputTokens: 100,
      outputTokens: 200,
      model: 'test-model',
      evaluation: '{threads:[]}',
    })
    expect(sql).toContain(`INSERT INTO ${S}.AI_EVALUATION_AUDITS`)
    expect(sql).toContain("'aud-1'")
    expect(sql).toContain("'batch-1'")
    expect(sql).toContain("'test-model'")
  })
})

describe('emailThreadEvaluations', () => {
  const S = 'TEST_SCHEMA'

  it('upserts with pre-built VALUE tuples', () => {
    const sql = emailThreadEvaluations.upsert(S, [
      "('id1', 'th-1', '', 'cat', 'summary', true, false, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain(`INSERT INTO ${S}.EMAIL_THREAD_EVALUATIONS`)
    expect(sql).toContain('ON CONFLICT (THREAD_ID) DO UPDATE SET')
    expect(sql).toContain('th-1')
  })

  it('selects existing evaluations by thread IDs', () => {
    const sql = emailThreadEvaluations.selectByThreadIds(S, ["'th-1'", "'th-2'"])
    expect(sql).toContain('SELECT THREAD_ID, IS_DEAL')
    expect(sql).toContain(`FROM ${S}.EMAIL_THREAD_EVALUATIONS`)
    expect(sql).toContain("'th-1'")
  })
})

describe('deals', () => {
  const S = 'TEST_SCHEMA'

  it('deletes by thread IDs', () => {
    const sql = deals.deleteByThreadIds(S, ["'th-1'", "'th-2'"])
    expect(sql).toContain(`DELETE FROM ${S}.DEALS`)
    expect(sql).toContain("'th-1'")
  })

  it('upserts with pre-built VALUE tuples', () => {
    const sql = deals.upsert(S, [
      "('d1', 'u1', 'th-1', '', 'Deal', 'brand', 'cat', 100, 'USD', 'Co', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain(`INSERT INTO ${S}.DEALS`)
    expect(sql).toContain('ON CONFLICT (THREAD_ID) DO UPDATE SET')
  })

  it('selects deals by thread IDs', () => {
    const sql = deals.selectByThreadIds(S, ["'th-1'", "'th-2'"])
    expect(sql).toContain('SELECT ID, THREAD_ID, USER_ID')
    expect(sql).toContain(`FROM ${S}.DEALS`)
    expect(sql).toContain("'th-1'")
  })
})

describe('dealContacts', () => {
  const S = 'TEST_SCHEMA'

  it('deletes by deal IDs', () => {
    const sql = dealContacts.deleteByDealIds(S, ["'d-1'", "'d-2'"])
    expect(sql).toContain(`DELETE FROM ${S}.DEAL_CONTACTS`)
    expect(sql).toContain("'d-1'")
  })

  it('upserts with pre-built VALUE tuples', () => {
    const sql = dealContacts.upsert(S, [
      "('d1', 'u1', 'test@co.com', 'primary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain(`INSERT INTO ${S}.DEAL_CONTACTS`)
    expect(sql).toContain('ON CONFLICT (DEAL_ID, USER_ID, EMAIL)')
  })
})

describe('contacts', () => {
  it('upserts with pre-built VALUE tuples', () => {
    const sql = contacts.upsert('CORE_SCHEMA', [
      "('u1', 'alice@co.com', 'Alice', NULL, 'CEO', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain('INSERT INTO CORE_SCHEMA.CONTACTS')
    expect(sql).toContain('ON CONFLICT (USER_ID, EMAIL)')
  })
})
```

**Step 2: Run tests to verify they fail**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql.test.js`
Expected: FAIL — missing exports

**Step 3: Add remaining builders to `src/lib/sql.js`**

Append to `src/lib/sql.js`:

```js
// ─── BATCH_EVENTS ────────────────────────────────────────────────────────────

export const batchEvents = {
  /** Insert a single batch event (idempotent by TRIGGER_HASH). */
  upsert: (schema, triggerHash, batchId, batchType, eventType) => {
    const s = sanitizeSchema(schema)
    const th = sanitizeId(triggerHash)
    const bid = sanitizeId(batchId)
    const bt = sanitizeString(batchType)
    const et = sanitizeString(eventType)
    return `INSERT INTO ${s}.BATCH_EVENTS (TRIGGER_HASH, BATCH_ID, BATCH_TYPE, EVENT_TYPE, CREATED_AT) VALUES ('${th}', '${bid}', '${bt}', '${et}', CURRENT_TIMESTAMP) ON CONFLICT (TRIGGER_HASH) DO UPDATE SET EVENT_TYPE = EXCLUDED.EVENT_TYPE, CREATED_AT = CURRENT_TIMESTAMP`
  },

  /** Insert pre-built VALUE tuples (used by WriteBatcher). */
  upsertValues: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.BATCH_EVENTS (TRIGGER_HASH, BATCH_ID, BATCH_TYPE, EVENT_TYPE, CREATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (TRIGGER_HASH) DO UPDATE SET EVENT_TYPE = EXCLUDED.EVENT_TYPE, CREATED_AT = CURRENT_TIMESTAMP`
  },
}

// ─── AI_EVALUATION_AUDITS ────────────────────────────────────────────────────

export const aiEvaluationAudits = {
  /** Select audit by batch ID (checkpoint check). */
  selectByBatch: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT AI_EVALUATION FROM ${s}.AI_EVALUATION_AUDITS WHERE BATCH_ID = '${bid}'`
  },

  /** Insert audit row. */
  insert: (
    schema,
    { id, batchId, threadCount, emailCount, cost, inputTokens, outputTokens, model, evaluation },
  ) => {
    const s = sanitizeSchema(schema)
    const safeId = sanitizeId(id)
    const safeBid = sanitizeId(batchId)
    const safeModel = sanitizeString(model)
    const safeEval = sanitizeString(evaluation)
    return `INSERT INTO ${s}.AI_EVALUATION_AUDITS (ID, BATCH_ID, THREAD_COUNT, EMAIL_COUNT, INFERENCE_COST, INPUT_TOKENS, OUTPUT_TOKENS, MODEL_USED, AI_EVALUATION, CREATED_AT) VALUES ('${safeId}', '${safeBid}', ${Number(threadCount)}, ${Number(emailCount)}, ${Number(cost)}, ${Number(inputTokens)}, ${Number(outputTokens)}, '${safeModel}', '${safeEval}', CURRENT_TIMESTAMP)`
  },
}

// ─── EMAIL_THREAD_EVALUATIONS ────────────────────────────────────────────────

export const emailThreadEvaluations = {
  /** Upsert with pre-built VALUE tuples. */
  upsert: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.EMAIL_THREAD_EVALUATIONS (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (THREAD_ID) DO UPDATE SET AI_EVALUATION_AUDIT_ID = EXCLUDED.AI_EVALUATION_AUDIT_ID, AI_INSIGHT = EXCLUDED.AI_INSIGHT, AI_SUMMARY = EXCLUDED.AI_SUMMARY, IS_DEAL = EXCLUDED.IS_DEAL, LIKELY_SCAM = EXCLUDED.LIKELY_SCAM, AI_SCORE = EXCLUDED.AI_SCORE, UPDATED_AT = CURRENT_TIMESTAMP`
  },

  /** Select existing evaluations by thread IDs (for unfetchable thread handling). */
  selectByThreadIds: (schema, quotedThreadIds) => {
    const s = sanitizeSchema(schema)
    return `SELECT THREAD_ID, IS_DEAL FROM ${s}.EMAIL_THREAD_EVALUATIONS WHERE THREAD_ID IN (${quotedThreadIds.join(',')})`
  },
}

// ─── DEALS ───────────────────────────────────────────────────────────────────

export const deals = {
  /** Delete deals by pre-quoted thread IDs. */
  deleteByThreadIds: (schema, quotedThreadIds) => {
    const s = sanitizeSchema(schema)
    return `DELETE FROM ${s}.DEALS WHERE THREAD_ID IN (${quotedThreadIds.join(',')})`
  },

  /** Upsert deals with pre-built VALUE tuples. */
  upsert: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.DEALS (ID, USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (THREAD_ID) DO UPDATE SET EMAIL_THREAD_EVALUATION_ID = EXCLUDED.EMAIL_THREAD_EVALUATION_ID, DEAL_NAME = EXCLUDED.DEAL_NAME, DEAL_TYPE = EXCLUDED.DEAL_TYPE, CATEGORY = EXCLUDED.CATEGORY, VALUE = EXCLUDED.VALUE, CURRENCY = EXCLUDED.CURRENCY, BRAND = EXCLUDED.BRAND, UPDATED_AT = CURRENT_TIMESTAMP`
  },

  /** Select deals by pre-quoted thread IDs (for save-deal-contacts). */
  selectByThreadIds: (schema, quotedThreadIds) => {
    const s = sanitizeSchema(schema)
    return `SELECT ID, THREAD_ID, USER_ID FROM ${s}.DEALS WHERE THREAD_ID IN (${quotedThreadIds.join(',')})`
  },
}

// ─── DEAL_CONTACTS ───────────────────────────────────────────────────────────

export const dealContacts = {
  /** Delete deal contacts by pre-quoted deal IDs. */
  deleteByDealIds: (schema, quotedDealIds) => {
    const s = sanitizeSchema(schema)
    return `DELETE FROM ${s}.DEAL_CONTACTS WHERE DEAL_ID IN (${quotedDealIds.join(',')})`
  },

  /** Upsert deal contacts with pre-built VALUE tuples. */
  upsert: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.DEAL_CONTACTS (DEAL_ID, USER_ID, EMAIL, CONTACT_TYPE, CREATED_AT, UPDATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (DEAL_ID, USER_ID, EMAIL) DO UPDATE SET CONTACT_TYPE = EXCLUDED.CONTACT_TYPE, UPDATED_AT = CURRENT_TIMESTAMP`
  },
}

// ─── CONTACTS ────────────────────────────────────────────────────────────────

export const contacts = {
  /** Upsert core contacts with pre-built VALUE tuples. */
  upsert: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.CONTACTS (USER_ID, EMAIL, NAME, COMPANY_NAME, TITLE, PHONE_NUMBER, CREATED_AT, UPDATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (USER_ID, EMAIL) DO UPDATE SET NAME = EXCLUDED.NAME, COMPANY_NAME = EXCLUDED.COMPANY_NAME, TITLE = EXCLUDED.TITLE, PHONE_NUMBER = EXCLUDED.PHONE_NUMBER, UPDATED_AT = CURRENT_TIMESTAMP`
  },
}
```

**Step 4: Run tests to verify they pass**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql.test.js`
Expected: PASS — all tests green

**Step 5: Commit**

```bash
git add src/lib/sql.js __tests__/sql.test.js
git commit -m "feat: add remaining table builders to sql.js"
```

---

## Task 3: Replace SQL in `src/lib/pipeline.js`

**Files:**

- Modify: `src/lib/pipeline.js`
- Test: `__tests__/pipeline.test.js` (existing tests must still pass)

Replace all inline SQL in `sweepStuckRows`, `sweepOrphanedRows`, and `insertBatchEvent`.

**Step 1: Replace `sweepStuckRows` SQL (lines 97-121)**

Replace inline SQL with builder calls:

```js
// At top of file, add:
import { dealStates as dealStatesSql, batchEvents as batchEventsSql } from './sql.js'

// In sweepStuckRows:
// Line 97-98: replace findDeadBatches SQL
const exhausted = await exec(
  dealStatesSql.findDeadBatches(safeSchema, statusSql, STUCK_INTERVAL_MINUTES, maxRetries),
)

// Line 109-110: replace countByBatchAndStatus SQL
const countRows = await exec(dealStatesSql.countByBatchAndStatus(safeSchema, safeBid, statusSql))

// Line 119-120: replace updateStatusByBatch SQL
await exec(dealStatesSql.updateStatusByBatch(safeSchema, safeBid, statusSql, STATUS.FAILED))
```

**Step 2: Replace `sweepOrphanedRows` SQL (lines 162-169)**

```js
// Line 162-163: replace countOrphaned SQL
const countRows = await exec(dealStatesSql.countOrphaned(safeSchema, statuses, safeStaleMinutes))

// Line 168-169: replace failOrphaned SQL
await exec(dealStatesSql.failOrphaned(safeSchema, statuses, safeStaleMinutes))
```

Note: Remove the local `literals` variable since the builder handles it internally.

**Step 3: Replace `insertBatchEvent` SQL (line 196)**

The builder now handles sanitization internally. Simplify `insertBatchEvent` to pass raw values:

```js
export async function insertBatchEvent(
  executeSqlFn,
  schema,
  { triggerHash, batchId, batchType, eventType },
) {
  const sql = batchEventsSql.upsert(schema, triggerHash, batchId, batchType, eventType)
  await executeSqlFn(sql)
}
```

**Step 4: Run existing pipeline tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/pipeline.test.js`
Expected: PASS — all existing tests still green

**Step 5: Commit**

```bash
git add src/lib/pipeline.js
git commit -m "refactor: replace inline SQL in pipeline.js with sql.js builders"
```

---

## Task 4: Replace SQL in `src/lib/queries.js`

**Files:**

- Modify: `src/lib/queries.js`
- Test: existing tests via dependent command tests

The `saveResults` namespace in `queries.js` contains SQL that should delegate to `sql.js`. The `detection` namespace stays as-is (thin wrappers accepting pre-joined ID strings).

**Step 1: Replace `saveResults` with `sql.js` delegates**

```js
import { aiEvaluationAudits as auditSql } from './sql.js'

export const saveResults = {
  getAuditByBatchId: (schema, batchId) => auditSql.selectByBatch(schema, batchId),
  insertAudit: (schema, params) => auditSql.insert(schema, params),
}
```

This is the key security win: `saveResults.getAuditByBatchId` previously didn't sanitize `batchId` — now `auditSql.selectByBatch` calls `sanitizeId(batchId)` internally.

**Step 2: Run dependent tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/`
Expected: PASS — all tests green (the `saveResults` signature is unchanged)

**Step 3: Commit**

```bash
git add src/lib/queries.js
git commit -m "refactor: delegate saveResults queries to sql.js builders"
```

---

## Task 5: Replace SQL in `src/lib/write-batcher.js`

**Files:**

- Modify: `src/lib/write-batcher.js`
- Test: `__tests__/write-batcher.test.js` (existing tests must still pass)

Replace all SQL in `_executeQueue` with `sql.js` builder calls.

**Step 1: Add import and replace `_executeQueue` cases**

```js
import {
  emailThreadEvaluations as evalSql,
  deals as dealsSql,
  dealContacts as dealContactsSql,
  contacts as contactsSql,
  batchEvents as batchEventsSql,
  dealStates as dealStatesSql,
} from './sql.js'
```

Replace each case in `_executeQueue`:

```js
case 'evals':
  await this._executeSqlFn(evalSql.upsert(s, items))
  break

case 'dealDeletes':
  await this._executeSqlFn(dealsSql.deleteByThreadIds(s, items))
  break

case 'deals':
  await this._executeSqlFn(dealsSql.upsert(s, items))
  break

case 'contactDeletes':
  await this._executeSqlFn(dealContactsSql.deleteByDealIds(s, items))
  break

case 'contacts':
  await this._executeSqlFn(dealContactsSql.upsert(s, items))
  break

case 'coreContacts': {
  // Keep dedup logic here (batcher responsibility), use builder for SQL
  const dedupMap = new Map()
  for (const item of items) {
    const m = item.match(/^\('([^']*(?:''[^']*)*)',\s*'([^']*(?:''[^']*)*)'/)
    const key = m ? `${m[1]}|${m[2]}` : item
    dedupMap.set(key, item)
  }
  const uniqueItems = [...dedupMap.values()]
  if (uniqueItems.length < items.length) {
    console.log(`[write-batcher] coreContacts deduped: ${items.length} → ${uniqueItems.length}`)
  }
  const cs = this._coreSchema
  await this._executeSqlFn(contactsSql.upsert(cs, uniqueItems))
  break
}

case 'stateUpdates': {
  const allDealIds = []
  const allNotDealIds = []
  for (const item of items) {
    allDealIds.push(...item.dealEmailIds)
    allNotDealIds.push(...item.notDealEmailIds)
  }
  if (allDealIds.length > 0) {
    await this._executeSqlFn(dealStatesSql.updateStatusByIds(s, allDealIds, 'deal'))
  }
  if (allNotDealIds.length > 0) {
    await this._executeSqlFn(dealStatesSql.updateStatusByIds(s, allNotDealIds, 'not_deal'))
  }
  break
}

case 'batchEvents':
  await this._executeSqlFn(batchEventsSql.upsertValues(s, items))
  break
```

**Step 2: Run existing write-batcher tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/write-batcher.test.js`
Expected: PASS — all existing tests still green (SQL output is identical)

**Step 3: Commit**

```bash
git add src/lib/sql.js src/lib/write-batcher.js
git commit -m "refactor: replace inline SQL in write-batcher.js with sql.js builders"
```

---

## Task 6: Replace SQL in standalone command files (batch 1)

**Files:**

- Modify: `src/commands/sync-deal-states.js`
- Modify: `src/commands/claim-filter-batch.js`
- Modify: `src/commands/claim-classify-batch.js`
- Modify: `src/commands/fetch-and-filter.js`
- Tests: `__tests__/sync-deal-states.test.js`, `__tests__/claim-filter-batch.test.js`, `__tests__/claim-classify-batch.test.js`

These are the simpler standalone commands with 1-5 SQL statements each.

**Step 1: Replace SQL in `sync-deal-states.js` (line 22)**

```js
import { dealStates as dealStatesSql } from '../lib/sql.js'

// Line 22: replace inline INSERT...SELECT
const sql = dealStatesSql.syncFromEmailMetadata(schema, emailCoreSchema)
```

**Step 2: Replace SQL in `claim-filter-batch.js` (lines 42, 47, 69, 96, 101)**

```js
import { dealStates as dealStatesSql } from '../lib/sql.js'

// Line 42: claim
await exec(dealStatesSql.claimFilterBatch(schema, batchId, batchSize))

// Line 47: select
const rows = await exec(dealStatesSql.selectByBatch(schema, batchId))

// Line 69: stuck batches
const stuckBatches = await exec(
  dealStatesSql.findStuckBatches(schema, STATUS.FILTERING, 5, maxRetries),
)

// Line 96: select stuck rows
const stuckRows = await exec(dealStatesSql.selectByBatch(schema, stuckBatchId))

// Line 101: touch batch
await exec(dealStatesSql.touchBatch(schema, stuckBatchId))
```

**Step 3: Replace SQL in `claim-classify-batch.js` (lines 38, 43, 60, 69, 73)**

```js
import { dealStates as dealStatesSql } from '../lib/sql.js'

// Line 38: claim
await exec(dealStatesSql.claimClassifyBatch(schema, batchId, batchSize))

// Line 43: select
const rows = await exec(dealStatesSql.selectByBatch(schema, batchId))

// Line 60: stuck batches
const stuckBatches = await exec(
  dealStatesSql.findStuckBatches(schema, STATUS.CLASSIFYING, 5, maxRetries),
)

// Line 69: select stuck rows
const stuckRows = await exec(dealStatesSql.selectByBatch(schema, stuckBatchId))

// Line 73: touch batch
await exec(dealStatesSql.touchBatch(schema, stuckBatchId))
```

**Step 4: Replace SQL in `fetch-and-filter.js` (line 33)**

```js
import { dealStates as dealStatesSql } from '../lib/sql.js'

// Line 33: select by batch
const metadataRows = await executeSql(
  apiUrl,
  jwt,
  biscuit,
  dealStatesSql.selectByBatch(schema, batchId),
)
```

Note: `fetch-and-filter.js` selects the same 5 columns as `selectByBatch` — verified match.

**Step 5: Run all affected tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sync-deal-states.test.js __tests__/claim-filter-batch.test.js __tests__/claim-classify-batch.test.js`
Expected: PASS

**Step 6: Commit**

```bash
git add src/commands/sync-deal-states.js src/commands/claim-filter-batch.js src/commands/claim-classify-batch.js src/commands/fetch-and-filter.js
git commit -m "refactor: replace inline SQL in standalone commands with sql.js builders"
```

---

## Task 7: Replace SQL in standalone command files (batch 2)

**Files:**

- Modify: `src/commands/fetch-and-classify.js`
- Modify: `src/commands/save-evals.js`
- Modify: `src/commands/save-deals.js`
- Modify: `src/commands/save-deal-contacts.js`
- Modify: `src/commands/update-deal-states.js`

These commands use `saveResults.*` (already delegating to `sql.js` after Task 4) plus some inline SQL.

**Step 1: Replace SQL in `fetch-and-classify.js` (lines 50-51)**

```js
import { dealStates as dealStatesSql } from '../lib/sql.js'

// Lines 50-51: select by batch
const metadataRows = await executeSql(
  apiUrl,
  jwt,
  biscuit,
  dealStatesSql.selectByBatch(schema, batchId),
)
```

Note: The `saveResults.*` calls at lines 66 and 226 are already handled by Task 4.

**Step 2: Replace SQL in `save-evals.js` (lines 57-67)**

```js
import { emailThreadEvaluations as evalSql } from '../lib/sql.js'

// Change values from joined string to array of tuples (remove .join(', ')):
const valueTuples = threads.map((thread) => {
  const threadId = sanitizeId(thread.thread_id)
  const evalId = uuidv7()
  const category = sanitizeString(thread.category || '')
  const aiSummary = sanitizeString(thread.ai_summary || '')
  const isDeal = thread.is_deal ? 'true' : 'false'
  const isLikelyScam = (thread.category || '').toLowerCase() === 'likely_scam' ? 'true' : 'false'
  const aiScore = typeof thread.ai_score === 'number' ? thread.ai_score : 0
  return `('${evalId}', '${threadId}', '', '${category}', '${aiSummary}', ${isDeal}, ${isLikelyScam}, ${aiScore}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
})

const sql = evalSql.upsert(schema, valueTuples)
```

**Step 3: Replace SQL in `save-deals.js` (lines 42, 68, 99-111)**

```js
import { dealStates as dealStatesSql, deals as dealsSql } from '../lib/sql.js'

// Line 42: select thread/user by batch
const metadataRows = await executeSql(
  apiUrl,
  jwt,
  biscuit,
  dealStatesSql.selectDistinctThreadUsers(schema, batchId),
)

// Line 68: delete non-deal threads — change from joined string to array
const quotedIds = notDealThreadIds.map((id) => `'${id}'`)
await executeSql(apiUrl, jwt, biscuit, dealsSql.deleteByThreadIds(schema, quotedIds))

// Lines 99-111: upsert deals — change dealValues from joined string to array
const dealTuples = dealThreads.map((thread) => {
  /* same mapping logic as before, no .join(', ') */
})
await executeSql(apiUrl, jwt, biscuit, dealsSql.upsert(schema, dealTuples))
```

**Step 4: Replace SQL in `save-deal-contacts.js` (lines 59, 109, 122)**

```js
import {
  deals as dealsSql,
  contacts as contactsSql,
  dealContacts as dealContactsSql,
} from '../lib/sql.js'

// Line 59: select deals by thread IDs — change from joined string to array
const quotedIds = dealThreadIds.map((id) => `'${id}'`)
const dealsResult = await executeSql(
  apiUrl,
  jwt,
  biscuit,
  dealsSql.selectByThreadIds(schema, quotedIds),
)

// Line 109: upsert core contacts
await executeSql(apiUrl, jwt, biscuit, contactsSql.upsert(coreSchema, coreContactValues))

// Line 122: upsert deal contacts
await executeSql(apiUrl, jwt, biscuit, dealContactsSql.upsert(schema, dealContactValues))
```

**Step 5: Replace SQL in `update-deal-states.js` (line 48)**

```js
import { dealStates as dealStatesSql } from '../lib/sql.js'

// Line 48: select metadata by batch
const metadataRows = await executeSql(
  apiUrl,
  jwt,
  biscuit,
  dealStatesSql.selectMetadataByBatch(schema, batchId),
)
```

Note: `detection.updateDeals` / `detection.updateNotDeal` calls remain unchanged (they accept pre-joined ID strings from `toSqlIdList()`).

**Step 6: Run all tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/`
Expected: PASS

**Step 7: Commit**

```bash
git add src/commands/fetch-and-classify.js src/commands/save-evals.js src/commands/save-deals.js src/commands/save-deal-contacts.js src/commands/update-deal-states.js
git commit -m "refactor: replace inline SQL in save/update commands with sql.js builders"
```

---

## Task 8: Replace SQL in pipeline orchestrators

**Files:**

- Modify: `src/commands/run-filter-pipeline.js`
- Modify: `src/commands/run-classify-pipeline.js`
- Tests: `__tests__/run-filter-pipeline.test.js`, `__tests__/run-classify-pipeline.test.js`

These are the largest files with the most SQL (8+ statements each). They duplicate patterns from the standalone commands.

**Step 1: Replace SQL in `run-filter-pipeline.js`**

```js
import { dealStates as dealStatesSql } from '../lib/sql.js'

// Line 50: claimBatch — claim pending rows
await exec(dealStatesSql.claimFilterBatch(schema, batchId, batchSize))

// Line 55: select claimed rows
const rows = await exec(dealStatesSql.selectByBatch(schema, batchId))

// Line 76: find stuck batches
const stuckBatches = await exec(
  dealStatesSql.findStuckBatches(schema, STATUS.FILTERING, 5, maxRetries),
)

// Line 95: select stuck rows
const stuckRows = await exec(dealStatesSql.selectByBatch(schema, stuckBatchId))

// Line 100: touch batch
await exec(dealStatesSql.touchBatch(schema, stuckBatchId))

// Line 162: update passed IDs to pending_classification
const quotedIds = filteredIds.map((id) => `'${sanitizeId(id)}'`)
await exec(
  dealStatesSql.updateStatusByIdsWithTimestamp(schema, quotedIds, STATUS.PENDING_CLASSIFICATION),
)

// Line 170: update rejected IDs to filter_rejected
const quotedRejected = rejectedIds.map((id) => `'${sanitizeId(id)}'`)
await exec(
  dealStatesSql.updateStatusByIdsWithTimestamp(schema, quotedRejected, STATUS.FILTER_REJECTED),
)

// Line 193: onDeadLetter — fail batch
await execNoRL(dealStatesSql.updateStatusByBatch(schema, safeBid, STATUS.FILTERING, STATUS.FAILED))
```

**Step 2: Replace SQL in `run-classify-pipeline.js`**

```js
import { dealStates as dealStatesSql, emailThreadEvaluations as evalSql } from '../lib/sql.js'

// Line 66: claimBatch — claim pending_classification threads
await exec(dealStatesSql.claimClassifyBatch(schema, batchId, classifyBatchSize))

// Line 72: select claimed rows with JOINs
const rows = await exec(dealStatesSql.selectByBatchWithJoins(schema, batchId))

// Line 94: find stuck batches
const stuckBatches = await exec(
  dealStatesSql.findStuckBatches(schema, STATUS.CLASSIFYING, 5, maxRetries),
)

// Line 112: select stuck rows with JOINs
const stuckRows = await exec(dealStatesSql.selectByBatchWithJoins(schema, stuckBatchId))

// Line 117: touch stuck batch
await exec(dealStatesSql.touchBatch(schema, stuckBatchId))

// Line 207: select existing evals for unfetchable threads
const quotedUnfetchable = unfetchableThreadIds.map((id) => `'${sanitizeId(id)}'`)
const existingEvals = await execNoRL(evalSql.selectByThreadIds(schema, quotedUnfetchable))

// Lines 230-231: update unfetchable deal rows
const quotedDealIds = unfetchableDealIds.map((id) => `'${sanitizeId(id)}'`)
await execNoRL(dealStatesSql.updateStatusByIds(schema, quotedDealIds, 'deal'))

// Lines 239-240: update unfetchable not-deal rows
const quotedNotDealIds = unfetchableNotDealIds.map((id) => `'${sanitizeId(id)}'`)
await execNoRL(dealStatesSql.updateStatusByIds(schema, quotedNotDealIds, 'not_deal'))

// Lines 250-251: batch event completion (unfetchable path)
await batcher.pushBatchEvents([
  `('${sanitizeId(batchId)}', '${sanitizeId(batchId)}', 'classify', 'complete', CURRENT_TIMESTAMP)`,
])

// Lines 549-550: update deal state rows
const quotedIds = dealEmailIds.map((id) => `'${sanitizeId(id)}'`)
await execNoRL(dealStatesSql.updateStatusByIds(schema, quotedIds, 'deal'))

// Lines 555-556: update not-deal state rows
const quotedNDIds = notDealEmailIds.map((id) => `'${sanitizeId(id)}'`)
await execNoRL(dealStatesSql.updateStatusByIds(schema, quotedNDIds, 'not_deal'))

// Lines 568-569: batch event completion
await batcher.pushBatchEvents([
  `('${sanitizeId(batchId)}', '${sanitizeId(batchId)}', 'classify', 'complete', CURRENT_TIMESTAMP)`,
])

// Line 584: onDeadLetter — fail batch
await execNoRL(
  dealStatesSql.updateStatusByBatch(schema, safeBid, STATUS.CLASSIFYING, STATUS.FAILED),
)
```

**Step 3: Run existing pipeline orchestrator tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-filter-pipeline.test.js __tests__/run-classify-pipeline.test.js`
Expected: PASS

**Step 4: Commit**

```bash
git add src/commands/run-filter-pipeline.js src/commands/run-classify-pipeline.js
git commit -m "refactor: replace inline SQL in pipeline orchestrators with sql.js builders"
```

---

## Task 9: Clean up and verify no inline SQL remains

**Files:**

- Modify: `src/lib/queries.js` (if needed)

**Step 1: Search for remaining inline SQL in commands and lib**

Run: `grep -rn "INSERT INTO\|UPDATE.*SET\|DELETE FROM\|SELECT.*FROM" src/commands/ src/lib/pipeline.js src/lib/write-batcher.js --include="*.js" | grep -v node_modules | grep -v sql.js`

Expected: Only `queries.js` detection wrappers remain. All template literal SQL should be gone from command files.

**Step 2: If any stragglers found, replace them**

**Step 3: Run full test suite**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/`
Expected: PASS

**Step 4: Commit**

```bash
git add src/lib/queries.js
git commit -m "chore: clean up queries.js after sql.js migration"
```

---

## Task 10: Run `npm run all` and verify packaging

**Step 1: Run the full build pipeline**

Run: `npm run all`
Expected: format + test + package all succeed

**Step 2: Verify `dist/index.js` was regenerated**

Run: `ls -la dist/index.js`
Expected: file exists with recent timestamp

**Step 3: Commit dist if changed**

```bash
git add dist/index.js
git commit -m "chore: rebuild dist after sql.js refactor"
```

---

## Summary of Changes

| File                                    | Change                                                            |
| --------------------------------------- | ----------------------------------------------------------------- |
| `src/lib/sql.js`                        | **NEW** — all SQL builder functions                               |
| `__tests__/sql.test.js`                 | **NEW** — unit tests for all builders                             |
| `src/lib/queries.js`                    | `saveResults` delegates to `sql.js`; sanitization utils unchanged |
| `src/lib/pipeline.js`                   | All 6 SQL statements replaced with builder calls                  |
| `src/lib/write-batcher.js`              | All 9 SQL statements replaced with builder calls                  |
| `src/commands/sync-deal-states.js`      | 1 SQL statement replaced                                          |
| `src/commands/claim-filter-batch.js`    | 5 SQL statements replaced                                         |
| `src/commands/claim-classify-batch.js`  | 5 SQL statements replaced                                         |
| `src/commands/fetch-and-filter.js`      | 1 SQL statement replaced                                          |
| `src/commands/fetch-and-classify.js`    | 1 SQL statement replaced                                          |
| `src/commands/save-evals.js`            | 1 SQL statement replaced                                          |
| `src/commands/save-deals.js`            | 3 SQL statements replaced                                         |
| `src/commands/save-deal-contacts.js`    | 3 SQL statements replaced                                         |
| `src/commands/update-deal-states.js`    | 1 SQL statement replaced                                          |
| `src/commands/run-filter-pipeline.js`   | 8 SQL statements replaced                                         |
| `src/commands/run-classify-pipeline.js` | 12+ SQL statements replaced                                       |

**Security improvement:** All `batchId` interpolations now go through `sanitizeId()` inside the builder (previously ~15 call sites had unsanitized UUIDs).

**No behavior changes.** All SQL output is identical. This is a pure structural refactor.
