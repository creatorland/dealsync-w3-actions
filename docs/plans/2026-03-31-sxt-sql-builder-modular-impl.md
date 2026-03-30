# SxT SQL Builder — Modular Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extract all 54 inline SQL statements into `src/lib/sql/` — per-table modules with explicit naming — so business logic never constructs SQL directly.

**Architecture:** A `src/lib/sql/` directory with 5 table-specific modules + barrel index. Each module exports a namespace object (e.g., `dealStates.claimFilterBatch(...)`). Each function accepts typed params, sanitizes internally, and returns a complete SQL string. Existing sanitization utilities stay in `queries.js` and are imported by sql modules. All callers replace inline SQL template literals with builder calls.

**Tech Stack:** Node 24 ESM, Jest (--experimental-vm-modules), no new dependencies

---

## Task 1: Create `src/lib/sql/deal-states.js` — DEAL_STATES builders

**Files:**

- Create: `src/lib/sql/deal-states.js`
- Create: `__tests__/sql/deal-states.test.js`

This task covers the DEAL_STATES table (13 distinct query patterns).

**Step 1: Write failing tests**

```js
// __tests__/sql/deal-states.test.js
import { dealStates } from '../../src/lib/sql/deal-states.js'

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

  describe('selectEmailsByBatch', () => {
    it('selects 5 columns by batch ID', () => {
      const sql = dealStates.selectEmailsByBatch(S, 'batch-123')
      expect(sql).toContain('SELECT EMAIL_METADATA_ID')
      expect(sql).toContain(`FROM ${S}.DEAL_STATES`)
      expect(sql).toContain("BATCH_ID = 'batch-123'")
    })
  })

  describe('selectEmailsWithEvalAndCreator', () => {
    it('includes LEFT JOINs for evaluations and user sync settings', () => {
      const sql = dealStates.selectEmailsWithEvalAndCreator(S, 'batch-789')
      expect(sql).toContain('LEFT JOIN')
      expect(sql).toContain('EMAIL_THREAD_EVALUATIONS')
      expect(sql).toContain('USER_SYNC_SETTINGS')
      expect(sql).toContain("BATCH_ID = 'batch-789'")
    })
  })

  describe('selectEmailAndThreadIdsByBatch', () => {
    it('selects EMAIL_METADATA_ID and THREAD_ID', () => {
      const sql = dealStates.selectEmailAndThreadIdsByBatch(S, 'batch-123')
      expect(sql).toContain('SELECT EMAIL_METADATA_ID, THREAD_ID')
      expect(sql).toContain("BATCH_ID = 'batch-123'")
    })
  })

  describe('selectDistinctThreadUsers', () => {
    it('selects DISTINCT THREAD_ID and USER_ID', () => {
      const sql = dealStates.selectDistinctThreadUsers(S, 'batch-123')
      expect(sql).toContain('SELECT DISTINCT THREAD_ID, USER_ID')
      expect(sql).toContain("BATCH_ID = 'batch-123'")
    })
  })

  describe('updateStatusByIds', () => {
    it('updates status with UPDATED_AT for a list of IDs', () => {
      const sql = dealStates.updateStatusByIds(S, ["'id-1'", "'id-2'"], 'deal')
      expect(sql).toContain("SET STATUS = 'deal'")
      expect(sql).toContain('UPDATED_AT = CURRENT_TIMESTAMP')
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

  describe('refreshBatchTimestamp', () => {
    it('updates UPDATED_AT for a batch', () => {
      const sql = dealStates.refreshBatchTimestamp(S, 'batch-1')
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

  describe('markOrphanedAsFailed', () => {
    it('marks stale unbatched rows as failed', () => {
      const sql = dealStates.markOrphanedAsFailed(S, ['pending_classification'], 30)
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

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql/deal-states.test.js`
Expected: FAIL — `Cannot find module '../../src/lib/sql/deal-states.js'`

**Step 3: Write `src/lib/sql/deal-states.js`**

```js
// src/lib/sql/deal-states.js
//
// DEAL_STATES table SQL builders.
// Pure functions: params in, SQL string out. No DB connection, no side effects.
//
// SxT constraints:
//   - No CTEs (WITH ... AS)
//   - No division operator
//   - INTERVAL syntax: INTERVAL 'N' MINUTE
//   - ON CONFLICT (...) DO UPDATE supported
//   - LEFT JOIN on single column only

import { sanitizeId, sanitizeString, sanitizeSchema } from '../queries.js'

export const dealStates = {
  claimFilterBatch: (schema, batchId, batchSize) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = 'filtering', BATCH_ID = '${bid}', UPDATED_AT = CURRENT_TIMESTAMP WHERE EMAIL_METADATA_ID IN (SELECT EMAIL_METADATA_ID FROM ${s}.DEAL_STATES WHERE STATUS = 'pending' LIMIT ${Number(batchSize)})`
  },

  claimClassifyBatch: (schema, batchId, batchSize) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = 'classifying', BATCH_ID = '${bid}', UPDATED_AT = CURRENT_TIMESTAMP WHERE THREAD_ID IN (SELECT DISTINCT ds.THREAD_ID FROM ${s}.DEAL_STATES ds WHERE ds.STATUS = 'pending_classification' AND NOT EXISTS (SELECT 1 FROM ${s}.DEAL_STATES ds2 WHERE ds2.THREAD_ID = ds.THREAD_ID AND ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID AND ds2.STATUS IN ('pending', 'filtering')) LIMIT ${Number(batchSize)}) AND STATUS = 'pending_classification'`
  },

  selectEmailsByBatch: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID FROM ${s}.DEAL_STATES WHERE BATCH_ID = '${bid}'`
  },

  selectEmailsWithEvalAndCreator: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT ds.EMAIL_METADATA_ID, ds.MESSAGE_ID, ds.USER_ID, ds.THREAD_ID, ds.SYNC_STATE_ID, ete.AI_SUMMARY AS PREVIOUS_AI_SUMMARY, ete.IS_DEAL AS PREVIOUS_IS_DEAL, uss.EMAIL AS CREATOR_EMAIL FROM ${s}.DEAL_STATES ds LEFT JOIN ${s}.EMAIL_THREAD_EVALUATIONS ete ON ete.THREAD_ID = ds.THREAD_ID LEFT JOIN ${s}.USER_SYNC_SETTINGS uss ON uss.USER_ID = ds.USER_ID WHERE ds.BATCH_ID = '${bid}'`
  },

  selectEmailAndThreadIdsByBatch: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT EMAIL_METADATA_ID, THREAD_ID FROM ${s}.DEAL_STATES WHERE BATCH_ID = '${bid}'`
  },

  selectDistinctThreadUsers: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT DISTINCT THREAD_ID, USER_ID FROM ${s}.DEAL_STATES WHERE BATCH_ID = '${bid}'`
  },

  updateStatusByIds: (schema, quotedIds, status) => {
    const s = sanitizeSchema(schema)
    const st = sanitizeString(status)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = '${st}', UPDATED_AT = CURRENT_TIMESTAMP WHERE EMAIL_METADATA_ID IN (${quotedIds.join(',')})`
  },

  updateStatusByBatch: (schema, batchId, fromStatus, toStatus) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    const from = sanitizeString(fromStatus)
    const to = sanitizeString(toStatus)
    return `UPDATE ${s}.DEAL_STATES SET STATUS = '${to}', UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${bid}' AND STATUS = '${from}'`
  },

  refreshBatchTimestamp: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `UPDATE ${s}.DEAL_STATES SET UPDATED_AT = CURRENT_TIMESTAMP WHERE BATCH_ID = '${bid}'`
  },

  findStuckBatches: (schema, status, intervalMinutes, maxRetries) => {
    const s = sanitizeSchema(schema)
    const st = sanitizeString(status)
    return `SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS ATTEMPTS FROM ${s}.DEAL_STATES ds LEFT JOIN ${s}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID WHERE ds.STATUS = '${st}' AND ds.BATCH_ID IS NOT NULL AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(intervalMinutes)}' MINUTE GROUP BY ds.BATCH_ID HAVING COUNT(DISTINCT be.TRIGGER_HASH) < ${Number(maxRetries)} LIMIT 1`
  },

  findDeadBatches: (schema, status, intervalMinutes, maxRetries) => {
    const s = sanitizeSchema(schema)
    const st = sanitizeString(status)
    return `SELECT ds.BATCH_ID FROM ${s}.DEAL_STATES ds LEFT JOIN ${s}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID WHERE ds.STATUS = '${st}' AND ds.BATCH_ID IS NOT NULL AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(intervalMinutes)}' MINUTE GROUP BY ds.BATCH_ID HAVING COUNT(DISTINCT be.TRIGGER_HASH) >= ${Number(maxRetries)}`
  },

  countByBatchAndStatus: (schema, batchId, status) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    const st = sanitizeString(status)
    return `SELECT COUNT(*) AS C FROM ${s}.DEAL_STATES WHERE BATCH_ID = '${bid}' AND STATUS = '${st}'`
  },

  countOrphaned: (schema, statuses, staleMinutes) => {
    const s = sanitizeSchema(schema)
    const literals = statuses.map((st) => `'${sanitizeString(st)}'`).join(',')
    return `SELECT COUNT(*) AS C FROM ${s}.DEAL_STATES WHERE STATUS IN (${literals}) AND BATCH_ID IS NULL AND UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(staleMinutes)}' MINUTE`
  },

  markOrphanedAsFailed: (schema, statuses, staleMinutes) => {
    const s = sanitizeSchema(schema)
    const literals = statuses.map((st) => `'${sanitizeString(st)}'`).join(',')
    return `UPDATE ${s}.DEAL_STATES SET STATUS = 'failed', UPDATED_AT = CURRENT_TIMESTAMP WHERE STATUS IN (${literals}) AND BATCH_ID IS NULL AND UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(staleMinutes)}' MINUTE`
  },

  syncFromEmailMetadata: (schema, emailCoreSchema) => {
    const s = sanitizeSchema(schema)
    const ecs = sanitizeSchema(emailCoreSchema)
    return `INSERT INTO ${s}.DEAL_STATES (ID, EMAIL_METADATA_ID, USER_ID, THREAD_ID, MESSAGE_ID, STATUS, CREATED_AT, UPDATED_AT) SELECT gen_random_uuid(), em.ID, em.USER_ID, em.THREAD_ID, em.MESSAGE_ID, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM ${ecs}.EMAIL_METADATA em WHERE NOT EXISTS (SELECT 1 FROM ${s}.DEAL_STATES ds WHERE ds.EMAIL_METADATA_ID = em.ID) ON CONFLICT (EMAIL_METADATA_ID) DO UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP`
  },
}
```

**Step 4: Run tests to verify they pass**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql/deal-states.test.js`
Expected: PASS

**Step 5: Commit**

```bash
git add src/lib/sql/deal-states.js __tests__/sql/deal-states.test.js
git commit -m "feat: add sql/deal-states.js builder — 15 DEAL_STATES queries"
```

---

## Task 2: Create remaining sql modules + barrel index

**Files:**

- Create: `src/lib/sql/batch-events.js`
- Create: `src/lib/sql/audits.js`
- Create: `src/lib/sql/evaluations.js`
- Create: `src/lib/sql/deals.js`
- Create: `src/lib/sql/index.js`
- Create: `__tests__/sql/batch-events.test.js`
- Create: `__tests__/sql/audits.test.js`
- Create: `__tests__/sql/evaluations.test.js`
- Create: `__tests__/sql/deals.test.js`

**Step 1: Write failing tests for all remaining modules**

```js
// __tests__/sql/batch-events.test.js
import { batchEvents } from '../../src/lib/sql/batch-events.js'

describe('batchEvents', () => {
  const S = 'TEST_SCHEMA'

  it('upsert produces INSERT with ON CONFLICT for single event', () => {
    const sql = batchEvents.upsert(S, 'trigger-1', 'batch-1', 'classify', 'new')
    expect(sql).toContain(`INSERT INTO ${S}.BATCH_EVENTS`)
    expect(sql).toContain("'trigger-1'")
    expect(sql).toContain("'batch-1'")
    expect(sql).toContain('ON CONFLICT (TRIGGER_HASH)')
  })

  it('upsertBulk produces VALUES for pre-built tuples', () => {
    const sql = batchEvents.upsertBulk(S, [
      "('hash1', 'batch1', 'classify', 'complete', CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain(`INSERT INTO ${S}.BATCH_EVENTS`)
    expect(sql).toContain('hash1')
    expect(sql).toContain('ON CONFLICT (TRIGGER_HASH)')
  })
})
```

```js
// __tests__/sql/audits.test.js
import { audits } from '../../src/lib/sql/audits.js'

describe('audits', () => {
  const S = 'TEST_SCHEMA'

  it('selectByBatch selects audit by batch ID', () => {
    const sql = audits.selectByBatch(S, 'batch-1')
    expect(sql).toContain('SELECT AI_EVALUATION')
    expect(sql).toContain(`FROM ${S}.AI_EVALUATION_AUDITS`)
    expect(sql).toContain("BATCH_ID = 'batch-1'")
  })

  it('insert creates audit row', () => {
    const sql = audits.insert(S, {
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
```

```js
// __tests__/sql/evaluations.test.js
import { evaluations } from '../../src/lib/sql/evaluations.js'

describe('evaluations', () => {
  const S = 'TEST_SCHEMA'

  it('upsert with pre-built VALUE tuples', () => {
    const sql = evaluations.upsert(S, [
      "('id1', 'th-1', '', 'cat', 'summary', true, false, 8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain(`INSERT INTO ${S}.EMAIL_THREAD_EVALUATIONS`)
    expect(sql).toContain('ON CONFLICT (THREAD_ID) DO UPDATE SET')
    expect(sql).toContain('th-1')
  })

  it('selectByThreadIds selects existing evaluations', () => {
    const sql = evaluations.selectByThreadIds(S, ["'th-1'", "'th-2'"])
    expect(sql).toContain('SELECT THREAD_ID, IS_DEAL')
    expect(sql).toContain(`FROM ${S}.EMAIL_THREAD_EVALUATIONS`)
    expect(sql).toContain("'th-1'")
  })
})
```

```js
// __tests__/sql/deals.test.js
import { deals, dealContacts, contacts } from '../../src/lib/sql/deals.js'

describe('deals', () => {
  const S = 'TEST_SCHEMA'

  it('deleteByThreadIds deletes by thread IDs', () => {
    const sql = deals.deleteByThreadIds(S, ["'th-1'", "'th-2'"])
    expect(sql).toContain(`DELETE FROM ${S}.DEALS`)
    expect(sql).toContain("'th-1'")
  })

  it('upsert with pre-built VALUE tuples', () => {
    const sql = deals.upsert(S, [
      "('d1', 'u1', 'th-1', '', 'Deal', 'brand', 'cat', 100, 'USD', 'Co', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain(`INSERT INTO ${S}.DEALS`)
    expect(sql).toContain('ON CONFLICT (THREAD_ID) DO UPDATE SET')
  })

  it('selectByThreadIds selects deals', () => {
    const sql = deals.selectByThreadIds(S, ["'th-1'", "'th-2'"])
    expect(sql).toContain('SELECT ID, THREAD_ID, USER_ID')
    expect(sql).toContain(`FROM ${S}.DEALS`)
    expect(sql).toContain("'th-1'")
  })
})

describe('dealContacts', () => {
  const S = 'TEST_SCHEMA'

  it('deleteByDealIds deletes by deal IDs', () => {
    const sql = dealContacts.deleteByDealIds(S, ["'d-1'", "'d-2'"])
    expect(sql).toContain(`DELETE FROM ${S}.DEAL_CONTACTS`)
    expect(sql).toContain("'d-1'")
  })

  it('upsert with pre-built VALUE tuples', () => {
    const sql = dealContacts.upsert(S, [
      "('d1', 'u1', 'test@co.com', 'primary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain(`INSERT INTO ${S}.DEAL_CONTACTS`)
    expect(sql).toContain('ON CONFLICT (DEAL_ID, USER_ID, EMAIL)')
  })
})

describe('contacts', () => {
  it('upsert with pre-built VALUE tuples', () => {
    const sql = contacts.upsert('CORE_SCHEMA', [
      "('u1', 'alice@co.com', 'Alice', NULL, 'CEO', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])
    expect(sql).toContain('INSERT INTO CORE_SCHEMA.CONTACTS')
    expect(sql).toContain('ON CONFLICT (USER_ID, EMAIL)')
  })
})
```

**Step 2: Run tests to verify they fail**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql/`
Expected: FAIL — missing modules

**Step 3: Write `src/lib/sql/batch-events.js`**

```js
import { sanitizeId, sanitizeString, sanitizeSchema } from '../queries.js'

export const batchEvents = {
  upsert: (schema, triggerHash, batchId, batchType, eventType) => {
    const s = sanitizeSchema(schema)
    const th = sanitizeId(triggerHash)
    const bid = sanitizeId(batchId)
    const bt = sanitizeString(batchType)
    const et = sanitizeString(eventType)
    return `INSERT INTO ${s}.BATCH_EVENTS (TRIGGER_HASH, BATCH_ID, BATCH_TYPE, EVENT_TYPE, CREATED_AT) VALUES ('${th}', '${bid}', '${bt}', '${et}', CURRENT_TIMESTAMP) ON CONFLICT (TRIGGER_HASH) DO UPDATE SET EVENT_TYPE = EXCLUDED.EVENT_TYPE, CREATED_AT = CURRENT_TIMESTAMP`
  },

  upsertBulk: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.BATCH_EVENTS (TRIGGER_HASH, BATCH_ID, BATCH_TYPE, EVENT_TYPE, CREATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (TRIGGER_HASH) DO UPDATE SET EVENT_TYPE = EXCLUDED.EVENT_TYPE, CREATED_AT = CURRENT_TIMESTAMP`
  },
}
```

**Step 4: Write `src/lib/sql/audits.js`**

```js
import { sanitizeId, sanitizeString, sanitizeSchema } from '../queries.js'

export const audits = {
  selectByBatch: (schema, batchId) => {
    const s = sanitizeSchema(schema)
    const bid = sanitizeId(batchId)
    return `SELECT AI_EVALUATION FROM ${s}.AI_EVALUATION_AUDITS WHERE BATCH_ID = '${bid}'`
  },

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
```

**Step 5: Write `src/lib/sql/evaluations.js`**

```js
import { sanitizeSchema } from '../queries.js'

export const evaluations = {
  upsert: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.EMAIL_THREAD_EVALUATIONS (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (THREAD_ID) DO UPDATE SET AI_EVALUATION_AUDIT_ID = EXCLUDED.AI_EVALUATION_AUDIT_ID, AI_INSIGHT = EXCLUDED.AI_INSIGHT, AI_SUMMARY = EXCLUDED.AI_SUMMARY, IS_DEAL = EXCLUDED.IS_DEAL, LIKELY_SCAM = EXCLUDED.LIKELY_SCAM, AI_SCORE = EXCLUDED.AI_SCORE, UPDATED_AT = CURRENT_TIMESTAMP`
  },

  selectByThreadIds: (schema, quotedThreadIds) => {
    const s = sanitizeSchema(schema)
    return `SELECT THREAD_ID, IS_DEAL FROM ${s}.EMAIL_THREAD_EVALUATIONS WHERE THREAD_ID IN (${quotedThreadIds.join(',')})`
  },
}
```

**Step 6: Write `src/lib/sql/deals.js`**

```js
import { sanitizeSchema } from '../queries.js'

export const deals = {
  deleteByThreadIds: (schema, quotedThreadIds) => {
    const s = sanitizeSchema(schema)
    return `DELETE FROM ${s}.DEALS WHERE THREAD_ID IN (${quotedThreadIds.join(',')})`
  },

  upsert: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.DEALS (ID, USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (THREAD_ID) DO UPDATE SET EMAIL_THREAD_EVALUATION_ID = EXCLUDED.EMAIL_THREAD_EVALUATION_ID, DEAL_NAME = EXCLUDED.DEAL_NAME, DEAL_TYPE = EXCLUDED.DEAL_TYPE, CATEGORY = EXCLUDED.CATEGORY, VALUE = EXCLUDED.VALUE, CURRENCY = EXCLUDED.CURRENCY, BRAND = EXCLUDED.BRAND, UPDATED_AT = CURRENT_TIMESTAMP`
  },

  selectByThreadIds: (schema, quotedThreadIds) => {
    const s = sanitizeSchema(schema)
    return `SELECT ID, THREAD_ID, USER_ID FROM ${s}.DEALS WHERE THREAD_ID IN (${quotedThreadIds.join(',')})`
  },
}

export const dealContacts = {
  deleteByDealIds: (schema, quotedDealIds) => {
    const s = sanitizeSchema(schema)
    return `DELETE FROM ${s}.DEAL_CONTACTS WHERE DEAL_ID IN (${quotedDealIds.join(',')})`
  },

  upsert: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.DEAL_CONTACTS (DEAL_ID, USER_ID, EMAIL, CONTACT_TYPE, CREATED_AT, UPDATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (DEAL_ID, USER_ID, EMAIL) DO UPDATE SET CONTACT_TYPE = EXCLUDED.CONTACT_TYPE, UPDATED_AT = CURRENT_TIMESTAMP`
  },
}

export const contacts = {
  upsert: (schema, valueTuples) => {
    const s = sanitizeSchema(schema)
    return `INSERT INTO ${s}.CONTACTS (USER_ID, EMAIL, NAME, COMPANY_NAME, TITLE, PHONE_NUMBER, CREATED_AT, UPDATED_AT) VALUES ${valueTuples.join(', ')} ON CONFLICT (USER_ID, EMAIL) DO UPDATE SET NAME = EXCLUDED.NAME, COMPANY_NAME = EXCLUDED.COMPANY_NAME, TITLE = EXCLUDED.TITLE, PHONE_NUMBER = EXCLUDED.PHONE_NUMBER, UPDATED_AT = CURRENT_TIMESTAMP`
  },
}
```

**Step 7: Write `src/lib/sql/index.js`**

```js
// Barrel re-export for all SQL builder modules.
export { dealStates } from './deal-states.js'
export { batchEvents } from './batch-events.js'
export { audits } from './audits.js'
export { evaluations } from './evaluations.js'
export { deals, dealContacts, contacts } from './deals.js'
```

**Step 8: Run all sql tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sql/`
Expected: PASS

**Step 9: Commit**

```bash
git add src/lib/sql/ __tests__/sql/
git commit -m "feat: add sql builder modules — batch-events, audits, evaluations, deals + barrel index"
```

---

## Task 3: Replace SQL in `src/lib/pipeline.js`

**Files:**

- Modify: `src/lib/pipeline.js`
- Test: `__tests__/pipeline.test.js` (existing tests must still pass)

Replace all 6 inline SQL statements in `sweepStuckRows`, `sweepOrphanedRows`, and `insertBatchEvent`.

**Step 1: Add import and replace inline SQL**

At top of `pipeline.js`, add:

```js
import { dealStates as dealStatesSql, batchEvents as batchEventsSql } from './sql/index.js'
```

Replace in `sweepStuckRows`:

- Line ~98 (findDeadBatches SQL) → `dealStatesSql.findDeadBatches(safeSchema, statusSql, STUCK_INTERVAL_MINUTES, maxRetries)`
- Line ~110 (countByBatchAndStatus SQL) → `dealStatesSql.countByBatchAndStatus(safeSchema, safeBid, statusSql)`
- Line ~120 (updateStatusByBatch SQL) → `dealStatesSql.updateStatusByBatch(safeSchema, safeBid, statusSql, STATUS.FAILED)`

Replace in `sweepOrphanedRows`:

- Line ~163 (countOrphaned SQL) → `dealStatesSql.countOrphaned(safeSchema, statuses, safeStaleMinutes)`
- Line ~169 (markOrphanedAsFailed SQL) → `dealStatesSql.markOrphanedAsFailed(safeSchema, statuses, safeStaleMinutes)`

Replace in `insertBatchEvent`:

- Line ~196 (upsert SQL) → `batchEventsSql.upsert(schema, triggerHash, batchId, batchType, eventType)`

Remove local sanitization that's now handled by builders (but keep the function signatures unchanged).

**Step 2: Run existing pipeline tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/pipeline.test.js`
Expected: PASS

**Step 3: Commit**

```bash
git add src/lib/pipeline.js
git commit -m "refactor: replace inline SQL in pipeline.js with sql builders"
```

---

## Task 4: Replace SQL in `src/lib/queries.js`

**Files:**

- Modify: `src/lib/queries.js`

Delegate `saveResults` to `audits` builder. Keep `detection` as-is (thin wrappers accepting pre-joined ID strings — these get replaced when their callers are updated in Tasks 6-7).

**Step 1: Replace `saveResults` with delegates**

```js
import { audits } from './sql/index.js'

export const saveResults = {
  getAuditByBatchId: (schema, batchId) => audits.selectByBatch(schema, batchId),
  insertAudit: (schema, params) => audits.insert(schema, params),
}
```

The `detection` namespace stays unchanged — it will be replaced when `update-deal-states.js` and `run-classify-pipeline.js` are updated in later tasks.

**Step 2: Run all tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/`
Expected: PASS

**Step 3: Commit**

```bash
git add src/lib/queries.js
git commit -m "refactor: delegate saveResults queries to sql/audits.js builders"
```

---

## Task 5: Replace SQL in `src/lib/write-batcher.js`

**Files:**

- Modify: `src/lib/write-batcher.js`
- Test: `__tests__/write-batcher.test.js` (existing tests must still pass)

Replace all 9 inline SQL statements in `_executeQueue`.

**Step 1: Add import and replace each case**

```js
import {
  evaluations as evalSql,
  deals as dealsSql,
  dealContacts as dealContactsSql,
  contacts as contactsSql,
  batchEvents as batchEventsSql,
  dealStates as dealStatesSql,
} from './sql/index.js'
```

Replace each case in `_executeQueue`:

- `case 'evals':` → `evalSql.upsert(s, items)`
- `case 'dealDeletes':` → `dealsSql.deleteByThreadIds(s, items)`
- `case 'deals':` → `dealsSql.upsert(s, items)`
- `case 'contactDeletes':` → `dealContactsSql.deleteByDealIds(s, items)`
- `case 'contacts':` → `dealContactsSql.upsert(s, items)`
- `case 'coreContacts':` → `contactsSql.upsert(cs, uniqueItems)` (keep dedup logic in batcher)
- `case 'stateUpdates':` deal IDs → `dealStatesSql.updateStatusByIds(s, allDealIds, 'deal')`, not-deal IDs → `dealStatesSql.updateStatusByIds(s, allNotDealIds, 'not_deal')`
- `case 'batchEvents':` → `batchEventsSql.upsertBulk(s, items)`

**Step 2: Run existing write-batcher tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/write-batcher.test.js`
Expected: PASS

**Step 3: Commit**

```bash
git add src/lib/write-batcher.js
git commit -m "refactor: replace inline SQL in write-batcher.js with sql builders"
```

---

## Task 6: Replace SQL in standalone commands (batch 1)

**Files:**

- Modify: `src/commands/sync-deal-states.js`
- Modify: `src/commands/claim-filter-batch.js`
- Modify: `src/commands/claim-classify-batch.js`
- Modify: `src/commands/fetch-and-filter.js`
- Modify: `src/commands/fetch-and-classify.js`

**Step 1: Replace SQL in each file**

`sync-deal-states.js` (1 statement):

```js
import { dealStates as dealStatesSql } from '../lib/sql/index.js'
// Line 22 → dealStatesSql.syncFromEmailMetadata(schema, emailCoreSchema)
```

`claim-filter-batch.js` (5 statements):

```js
import { dealStates as dealStatesSql } from '../lib/sql/index.js'
// Line 42 → dealStatesSql.claimFilterBatch(schema, batchId, batchSize)
// Line 47 → dealStatesSql.selectEmailsByBatch(schema, batchId)
// Line 69 → dealStatesSql.findStuckBatches(schema, STATUS.FILTERING, 5, maxRetries)
// Line 96 → dealStatesSql.selectEmailsByBatch(schema, stuckBatchId)
// Line 101 → dealStatesSql.refreshBatchTimestamp(schema, stuckBatchId)
```

`claim-classify-batch.js` (5 statements):

```js
import { dealStates as dealStatesSql } from '../lib/sql/index.js'
// Line 38 → dealStatesSql.claimClassifyBatch(schema, batchId, batchSize)
// Line 43 → dealStatesSql.selectEmailsByBatch(schema, batchId)
// Line 60 → dealStatesSql.findStuckBatches(schema, STATUS.CLASSIFYING, 5, maxRetries)
// Line 69 → dealStatesSql.selectEmailsByBatch(schema, stuckBatchId)
// Line 73 → dealStatesSql.refreshBatchTimestamp(schema, stuckBatchId)
```

`fetch-and-filter.js` (1 statement):

```js
import { dealStates as dealStatesSql } from '../lib/sql/index.js'
// Line 33 → dealStatesSql.selectEmailsByBatch(schema, batchId)
```

`fetch-and-classify.js` (1 statement):

```js
import { dealStates as dealStatesSql } from '../lib/sql/index.js'
// Lines 50-51 → dealStatesSql.selectEmailsByBatch(schema, batchId)
```

**Step 2: Run affected tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sync-deal-states.test.js __tests__/claim-filter-batch.test.js __tests__/claim-classify-batch.test.js`
Expected: PASS

**Step 3: Commit**

```bash
git add src/commands/sync-deal-states.js src/commands/claim-filter-batch.js src/commands/claim-classify-batch.js src/commands/fetch-and-filter.js src/commands/fetch-and-classify.js
git commit -m "refactor: replace inline SQL in standalone commands (batch 1) with sql builders"
```

---

## Task 7: Replace SQL in standalone commands (batch 2)

**Files:**

- Modify: `src/commands/save-evals.js`
- Modify: `src/commands/save-deals.js`
- Modify: `src/commands/save-deal-contacts.js`
- Modify: `src/commands/update-deal-states.js`

**Step 1: Replace SQL in each file**

`save-evals.js` (1 inline statement):

```js
import { evaluations as evalSql } from '../lib/sql/index.js'
// Lines 57-67: change joined string → array of tuples, then evalSql.upsert(schema, valueTuples)
```

`save-deals.js` (3 statements):

```js
import { dealStates as dealStatesSql, deals as dealsSql } from '../lib/sql/index.js'
// Line 42 → dealStatesSql.selectDistinctThreadUsers(schema, batchId)
// Line 68 → dealsSql.deleteByThreadIds(schema, quotedIds)
// Lines 99-110 → dealsSql.upsert(schema, dealTuples)
```

`save-deal-contacts.js` (3 statements):

```js
import {
  deals as dealsSql,
  contacts as contactsSql,
  dealContacts as dealContactsSql,
} from '../lib/sql/index.js'
// Line 59 → dealsSql.selectByThreadIds(schema, quotedIds)
// Line 109 → contactsSql.upsert(coreSchema, coreContactValues)
// Line 122 → dealContactsSql.upsert(schema, dealContactValues)
```

`update-deal-states.js` (1 inline statement):

```js
import { dealStates as dealStatesSql } from '../lib/sql/index.js'
// Line 48 → dealStatesSql.selectEmailAndThreadIdsByBatch(schema, batchId)
```

Note: `detection.updateDeals` / `detection.updateNotDeal` calls in `update-deal-states.js` remain unchanged — they accept pre-joined ID strings from `toSqlIdList()`.

**Step 2: Run all tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/`
Expected: PASS

**Step 3: Commit**

```bash
git add src/commands/save-evals.js src/commands/save-deals.js src/commands/save-deal-contacts.js src/commands/update-deal-states.js
git commit -m "refactor: replace inline SQL in save/update commands with sql builders"
```

---

## Task 8: Replace SQL in pipeline orchestrators

**Files:**

- Modify: `src/commands/run-filter-pipeline.js`
- Modify: `src/commands/run-classify-pipeline.js`

These are the largest files (8+ SQL statements each).

**Step 1: Replace SQL in `run-filter-pipeline.js` (8 statements)**

```js
import { dealStates as dealStatesSql } from '../lib/sql/index.js'

// Line 50: claim → dealStatesSql.claimFilterBatch(schema, batchId, batchSize)
// Line 55: select → dealStatesSql.selectEmailsByBatch(schema, batchId)
// Line 77: stuck → dealStatesSql.findStuckBatches(schema, STATUS.FILTERING, 5, maxRetries)
// Line 95: select stuck → dealStatesSql.selectEmailsByBatch(schema, stuckBatchId)
// Line 100: touch → dealStatesSql.refreshBatchTimestamp(schema, stuckBatchId)
// Line 162: update passed → dealStatesSql.updateStatusByIds(schema, quotedIds, STATUS.PENDING_CLASSIFICATION)
// Line 170: update rejected → dealStatesSql.updateStatusByIds(schema, quotedRejected, STATUS.FILTER_REJECTED)
// Line 193: dead-letter → dealStatesSql.updateStatusByBatch(schema, safeBid, STATUS.FILTERING, STATUS.FAILED)
```

**Step 2: Replace SQL in `run-classify-pipeline.js` (12+ statements)**

```js
import { dealStates as dealStatesSql, evaluations as evalSql } from '../lib/sql/index.js'

// Line 66: claim → dealStatesSql.claimClassifyBatch(schema, batchId, classifyBatchSize)
// Line 72: select with joins → dealStatesSql.selectEmailsWithEvalAndCreator(schema, batchId)
// Line 94: stuck → dealStatesSql.findStuckBatches(schema, STATUS.CLASSIFYING, 5, maxRetries)
// Line 112: select stuck with joins → dealStatesSql.selectEmailsWithEvalAndCreator(schema, stuckBatchId)
// Line 117: touch → dealStatesSql.refreshBatchTimestamp(schema, stuckBatchId)
// Line 207: select evals → evalSql.selectByThreadIds(schema, quotedUnfetchable)
// Lines 230-231: update deal → dealStatesSql.updateStatusByIds(schema, quotedDealIds, 'deal')
// Lines 239-240: update not-deal → dealStatesSql.updateStatusByIds(schema, quotedNotDealIds, 'not_deal')
// Lines 549-550: update deal → dealStatesSql.updateStatusByIds(schema, quotedIds, 'deal')
// Lines 555-556: update not-deal → dealStatesSql.updateStatusByIds(schema, quotedNDIds, 'not_deal')
// Line 584: dead-letter → dealStatesSql.updateStatusByBatch(schema, safeBid, STATUS.CLASSIFYING, STATUS.FAILED)
```

**Step 3: Run existing pipeline orchestrator tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-filter-pipeline.test.js __tests__/run-classify-pipeline.test.js`
Expected: PASS

**Step 4: Commit**

```bash
git add src/commands/run-filter-pipeline.js src/commands/run-classify-pipeline.js
git commit -m "refactor: replace inline SQL in pipeline orchestrators with sql builders"
```

---

## Task 9: Migrate `detection` namespace from queries.js to sql builders

**Files:**

- Modify: `src/lib/queries.js`
- Modify: `src/commands/update-deal-states.js`

The `detection.updateDeals` and `detection.updateNotDeal` wrappers in `queries.js` can now delegate to `dealStatesSql.updateStatusByIds`. However, they accept a pre-joined string (`sqlQuotedIds`) while the builder expects an array. Update the callers to pass arrays instead.

**Step 1: Update `update-deal-states.js` to use builder directly**

Replace `detection.updateDeals(schema, sqlQuotedIds)` and `detection.updateNotDeal(schema, sqlQuotedIds)` with `dealStatesSql.updateStatusByIds(schema, quotedIdArray, 'deal')` and `dealStatesSql.updateStatusByIds(schema, quotedIdArray, 'not_deal')`. Change `toSqlIdList()` call to produce an array instead of a joined string.

**Step 2: Remove `detection` namespace from queries.js**

Delete the `detection` export entirely. The only consumer was `update-deal-states.js`.

**Step 3: Run all tests**

Run: `node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/`
Expected: PASS

**Step 4: Commit**

```bash
git add src/lib/queries.js src/commands/update-deal-states.js
git commit -m "refactor: remove detection namespace, use dealStates.updateStatusByIds directly"
```

---

## Task 10: Verify no inline SQL remains + full build

**Step 1: Search for remaining inline SQL**

Run: `grep -rn "INSERT INTO\|UPDATE.*SET\|DELETE FROM\|SELECT.*FROM" src/commands/ src/lib/pipeline.js src/lib/write-batcher.js --include="*.js" | grep -v sql/`
Expected: No results (all SQL is now in `src/lib/sql/`)

Separately verify `queries.js` only has `saveResults` delegates + sanitization utils:
Run: `grep -n "INSERT INTO\|UPDATE.*SET\|DELETE FROM\|SELECT.*FROM" src/lib/queries.js`
Expected: No results

**Step 2: Run full test suite**

Run: `npm test`
Expected: PASS

**Step 3: Run full build**

Run: `npm run all`
Expected: format + test + package all succeed

**Step 4: Commit dist**

```bash
git add dist/index.js
git commit -m "chore: rebuild dist after sql builder refactor"
```

---

## Summary

| File                          | Change                                                                               |
| ----------------------------- | ------------------------------------------------------------------------------------ |
| `src/lib/sql/index.js`        | **NEW** — barrel re-export                                                           |
| `src/lib/sql/deal-states.js`  | **NEW** — 15 DEAL_STATES builders                                                    |
| `src/lib/sql/batch-events.js` | **NEW** — 2 BATCH_EVENTS builders                                                    |
| `src/lib/sql/audits.js`       | **NEW** — 2 AI_EVALUATION_AUDITS builders                                            |
| `src/lib/sql/evaluations.js`  | **NEW** — 2 EMAIL_THREAD_EVALUATIONS builders                                        |
| `src/lib/sql/deals.js`        | **NEW** — 6 builders (deals + dealContacts + contacts)                               |
| `__tests__/sql/*.test.js`     | **NEW** — unit tests for all builders                                                |
| `src/lib/queries.js`          | `saveResults` delegates to audits; `detection` removed; sanitization utils unchanged |
| `src/lib/pipeline.js`         | 6 SQL statements → builder calls                                                     |
| `src/lib/write-batcher.js`    | 9 SQL statements → builder calls                                                     |
| `src/commands/*.js`           | All remaining SQL statements → builder calls                                         |
