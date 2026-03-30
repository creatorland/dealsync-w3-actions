# Plan: SxT-Aware SQL Builder (`src/lib/sql.js`)

**Date:** 2026-03-31  
**Status:** Proposed  
**Context:** This plan replaces the earlier suggestion of using Knex.js as a query builder. After evaluating Space and Time (SxT)'s SQL dialect constraints and the REST-based access model of `sxt-client.js`, Knex.js was ruled out. See rationale below.

---

## Why Not Knex.js

Knex.js is a SQL query builder that targets relational databases over persistent TCP connections (PostgreSQL, MySQL, SQLite, etc.). SxT is accessed exclusively via **REST API** — Knex's connection pooling, transaction management, and migration helpers are all inapplicable.

More critically, Knex generates dialect-specific SQL that does not match SxT's supported subset:

| SQL Feature Used in Codebase                       | Knex Support                                 | SxT Reality                                      |
| -------------------------------------------------- | -------------------------------------------- | ------------------------------------------------ |
| `ON CONFLICT (...) DO UPDATE SET`                  | ⚠️ Postgres-only via `.onConflict().merge()` | SxT supports this, but Knex can't connect to SxT |
| `INTERVAL '5' MINUTE`                              | ❌ Generates dialect-specific syntax         | SxT has its own interval syntax                  |
| `INSERT INTO ... SELECT ...` (bulk sync)           | Only via `knex.raw()`                        | Required for `sync-deal-states.js`               |
| `gen_random_uuid()`                                | ❌ Postgres-only                             | SxT support is unverified                        |
| Subquery in `UPDATE ... WHERE ... IN (SELECT ...)` | ⚠️ Limited                                   | SxT JOIN/subquery support is restricted          |
| `HAVING COUNT(DISTINCT ...)`                       | ⚠️ Only for supported dialects               | SxT aggregation is a subset                      |

Using Knex would force `knex.raw()` for the majority of queries, defeating its purpose. A custom SxT dialect is theoretically possible but would be more engineering work than simply centralizing SQL into a dedicated module.

---

## Goal

Centralize all raw SQL string construction into a single module (`src/lib/sql.js`) to:

1. **Decouple query structure from business logic** — schema changes (e.g., adding a column to `DEALS`) require editing one file, not hunting through multiple pipeline files.
2. **Eliminate string interpolation bugs** — all unsafe values are funneled through a consistent escaping helper.
3. **Enable unit testing of query shapes** — pure functions that return strings are trivially testable without a live database.
4. **Zero added dependencies** — no new npm packages needed.

---

## Proposed Implementation

### New File: `src/lib/sql.js`

This module exposes named query-builder functions organized by table. Each function accepts typed parameters and returns a complete SQL string ready to send to `sxt-client.js`.

#### Structure

```js
// src/lib/sql.js

/**
 * Escape a string value for safe embedding in SxT SQL literals.
 * SxT does not support parameterized queries over REST; values must be
 * embedded directly. This helper prevents quote injection.
 */
function esc(value) {
  if (value === null || value === undefined) return 'NULL'
  return String(value).replace(/'/g, "''")
}

// ─── DEAL_STATES ────────────────────────────────────────────────────────────

export const dealStates = {
  /**
   * Claim a batch of pending rows by stamping them with a batchId and status.
   * Uses a subquery-based UPDATE (no CTE — SxT does not support CTEs).
   */
  claimBatch: (schema, batchId, status, targetStatus, batchSize) =>
    `UPDATE ${schema}.DEAL_STATES
     SET STATUS = '${esc(status)}', BATCH_ID = '${esc(batchId)}', UPDATED_AT = CURRENT_TIMESTAMP
     WHERE EMAIL_METADATA_ID IN (
       SELECT EMAIL_METADATA_ID FROM ${schema}.DEAL_STATES
       WHERE STATUS = '${esc(targetStatus)}' LIMIT ${Number(batchSize)}
     )`,

  /** Select all rows belonging to a batch. */
  selectByBatch: (schema, batchId) =>
    `SELECT EMAIL_METADATA_ID, MESSAGE_ID, USER_ID, THREAD_ID, SYNC_STATE_ID
     FROM ${schema}.DEAL_STATES
     WHERE BATCH_ID = '${esc(batchId)}'`,

  /** Update status for a list of email_metadata_ids. */
  updateStatus: (schema, ids, status) => {
    const quoted = ids.map((id) => `'${esc(id)}'`).join(', ')
    return `UPDATE ${schema}.DEAL_STATES
            SET STATUS = '${esc(status)}', UPDATED_AT = CURRENT_TIMESTAMP
            WHERE EMAIL_METADATA_ID IN (${quoted})`
  },

  /** Update status for all rows under a batchId where status matches. */
  updateStatusByBatch: (schema, batchId, fromStatus, toStatus) =>
    `UPDATE ${schema}.DEAL_STATES
     SET STATUS = '${esc(toStatus)}', UPDATED_AT = CURRENT_TIMESTAMP
     WHERE BATCH_ID = '${esc(batchId)}' AND STATUS = '${esc(fromStatus)}'`,

  /** Touch UPDATED_AT to extend lease on a stuck batch. */
  touchBatch: (schema, batchId) =>
    `UPDATE ${schema}.DEAL_STATES
     SET UPDATED_AT = CURRENT_TIMESTAMP
     WHERE BATCH_ID = '${esc(batchId)}'`,

  /** Find stuck batches eligible for retry (have not exceeded maxRetries). */
  findStuckBatch: (schema, status, intervalMinutes, maxRetries) =>
    `SELECT ds.BATCH_ID, COUNT(DISTINCT be.TRIGGER_HASH) AS ATTEMPTS
     FROM ${schema}.DEAL_STATES ds
     LEFT JOIN ${schema}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID
     WHERE ds.STATUS = '${esc(status)}'
       AND ds.BATCH_ID IS NOT NULL
       AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(intervalMinutes)}' MINUTE
     GROUP BY ds.BATCH_ID
     HAVING COUNT(DISTINCT be.TRIGGER_HASH) < ${Number(maxRetries)}
     LIMIT 1`,

  /** Find batches that have hit maxRetries (dead-letter candidates). */
  findDeadBatches: (schema, status, intervalMinutes, maxRetries) =>
    `SELECT ds.BATCH_ID
     FROM ${schema}.DEAL_STATES ds
     LEFT JOIN ${schema}.BATCH_EVENTS be ON be.BATCH_ID = ds.BATCH_ID
     WHERE ds.STATUS = '${esc(status)}'
       AND ds.BATCH_ID IS NOT NULL
       AND ds.UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(intervalMinutes)}' MINUTE
     GROUP BY ds.BATCH_ID
     HAVING COUNT(DISTINCT be.TRIGGER_HASH) >= ${Number(maxRetries)}`,

  /** Count rows by status. */
  countByStatus: (schema, batchId, status) =>
    `SELECT COUNT(*) AS C FROM ${schema}.DEAL_STATES
     WHERE BATCH_ID = '${esc(batchId)}' AND STATUS = '${esc(status)}'`,

  /** Count stale unbatched rows. */
  countStale: (schema, statuses, staleMinutes) => {
    const literals = statuses.map((s) => `'${esc(s)}'`).join(', ')
    return `SELECT COUNT(*) AS C FROM ${schema}.DEAL_STATES
            WHERE STATUS IN (${literals})
              AND BATCH_ID IS NULL
              AND UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(staleMinutes)}' MINUTE`
  },

  /** Mark stale unbatched rows as failed. */
  failStale: (schema, statuses, staleMinutes) => {
    const literals = statuses.map((s) => `'${esc(s)}'`).join(', ')
    return `UPDATE ${schema}.DEAL_STATES
            SET STATUS = 'failed', UPDATED_AT = CURRENT_TIMESTAMP
            WHERE STATUS IN (${literals})
              AND BATCH_ID IS NULL
              AND UPDATED_AT < CURRENT_TIMESTAMP - INTERVAL '${Number(staleMinutes)}' MINUTE`
  },

  /** Sync new EMAIL_METADATA rows into DEAL_STATES as 'pending'. */
  syncFromEmailMetadata: (schema, emailCoreSchema) =>
    `INSERT INTO ${schema}.DEAL_STATES
       (ID, EMAIL_METADATA_ID, USER_ID, THREAD_ID, MESSAGE_ID, STATUS, CREATED_AT, UPDATED_AT)
     SELECT gen_random_uuid(), em.ID, em.USER_ID, em.THREAD_ID, em.MESSAGE_ID,
            'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
     FROM ${emailCoreSchema}.EMAIL_METADATA em
     WHERE NOT EXISTS (
       SELECT 1 FROM ${schema}.DEAL_STATES ds WHERE ds.EMAIL_METADATA_ID = em.ID
     )
     ON CONFLICT (EMAIL_METADATA_ID) DO UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP`,

  /**
   * Claim pending_classification rows for a thread-level classify batch.
   * Excludes threads that still have rows in earlier stages (pending/filtering).
   */
  claimClassifyBatch: (schema, batchId, batchSize) =>
    `UPDATE ${schema}.DEAL_STATES
     SET STATUS = 'classifying', BATCH_ID = '${esc(batchId)}', UPDATED_AT = CURRENT_TIMESTAMP
     WHERE THREAD_ID IN (
       SELECT DISTINCT ds.THREAD_ID FROM ${schema}.DEAL_STATES ds
       WHERE ds.STATUS = 'pending_classification'
         AND NOT EXISTS (
           SELECT 1 FROM ${schema}.DEAL_STATES ds2
           WHERE ds2.THREAD_ID = ds.THREAD_ID
             AND ds2.SYNC_STATE_ID = ds.SYNC_STATE_ID
             AND ds2.STATUS IN ('pending', 'filtering')
         )
       LIMIT ${Number(batchSize)}
     )
     AND STATUS = 'pending_classification'`,
}

// ─── BATCH_EVENTS ────────────────────────────────────────────────────────────

export const batchEvents = {
  /** Upsert a batch event (idempotent by TRIGGER_HASH). */
  upsert: (schema, rows) => {
    const values = rows
      .map(
        (r) =>
          `('${esc(r.triggerHash)}', '${esc(r.batchId)}', '${esc(r.batchType)}', '${esc(r.eventType)}', CURRENT_TIMESTAMP)`,
      )
      .join(', ')
    return `INSERT INTO ${schema}.BATCH_EVENTS
              (TRIGGER_HASH, BATCH_ID, BATCH_TYPE, EVENT_TYPE, CREATED_AT)
            VALUES ${values}
            ON CONFLICT (TRIGGER_HASH) DO UPDATE SET
              EVENT_TYPE = EXCLUDED.EVENT_TYPE, CREATED_AT = CURRENT_TIMESTAMP`
  },
}

// ─── AI_EVALUATION_AUDITS ────────────────────────────────────────────────────

export const aiEvaluationAudits = {
  selectByBatch: (schema, batchId) =>
    `SELECT AI_EVALUATION FROM ${schema}.AI_EVALUATION_AUDITS
     WHERE BATCH_ID = '${esc(batchId)}'`,

  insert: (schema, row) =>
    `INSERT INTO ${schema}.AI_EVALUATION_AUDITS
       (ID, BATCH_ID, AI_EVALUATION, CREATED_AT)
     VALUES (
       '${esc(row.id)}', '${esc(row.batchId)}',
       '${esc(JSON.stringify(row.aiEvaluation))}',
       CURRENT_TIMESTAMP
     )`,
}

// ─── EMAIL_THREAD_EVALUATIONS ────────────────────────────────────────────────

export const emailThreadEvaluations = {
  /** Batch upsert thread evaluation rows. `rows` is array of value-tuple strings. */
  upsert: (schema, valueTuples) =>
    `INSERT INTO ${schema}.EMAIL_THREAD_EVALUATIONS
       (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT)
     VALUES ${valueTuples.join(', ')}
     ON CONFLICT (THREAD_ID) DO UPDATE SET
       AI_EVALUATION_AUDIT_ID = EXCLUDED.AI_EVALUATION_AUDIT_ID,
       AI_INSIGHT = EXCLUDED.AI_INSIGHT,
       AI_SUMMARY = EXCLUDED.AI_SUMMARY,
       IS_DEAL = EXCLUDED.IS_DEAL,
       LIKELY_SCAM = EXCLUDED.LIKELY_SCAM,
       AI_SCORE = EXCLUDED.AI_SCORE,
       UPDATED_AT = CURRENT_TIMESTAMP`,
}

// ─── DEALS ───────────────────────────────────────────────────────────────────

export const deals = {
  /** Delete deals by thread IDs (used before re-inserting updated records). */
  deleteByThreadIds: (schema, threadIds) => {
    const quoted = threadIds.map((id) => `'${esc(id)}'`).join(', ')
    return `DELETE FROM ${schema}.DEALS WHERE THREAD_ID IN (${quoted})`
  },

  /** Batch upsert deals. `valueTuples` is array of value-tuple strings. */
  upsert: (schema, valueTuples) =>
    `INSERT INTO ${schema}.DEALS
       (ID, USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT)
     VALUES ${valueTuples.join(', ')}
     ON CONFLICT (THREAD_ID) DO UPDATE SET
       EMAIL_THREAD_EVALUATION_ID = EXCLUDED.EMAIL_THREAD_EVALUATION_ID,
       DEAL_NAME = EXCLUDED.DEAL_NAME,
       DEAL_TYPE = EXCLUDED.DEAL_TYPE,
       CATEGORY = EXCLUDED.CATEGORY,
       VALUE = EXCLUDED.VALUE,
       CURRENCY = EXCLUDED.CURRENCY,
       BRAND = EXCLUDED.BRAND,
       UPDATED_AT = CURRENT_TIMESTAMP`,
}

// ─── DEAL_CONTACTS ───────────────────────────────────────────────────────────

export const dealContacts = {
  deleteByDealIds: (schema, dealIds) => {
    const quoted = dealIds.map((id) => `'${esc(id)}'`).join(', ')
    return `DELETE FROM ${schema}.DEAL_CONTACTS WHERE DEAL_ID IN (${quoted})`
  },

  upsert: (schema, valueTuples) =>
    `INSERT INTO ${schema}.DEAL_CONTACTS
       (DEAL_ID, USER_ID, EMAIL, CONTACT_TYPE, CREATED_AT, UPDATED_AT)
     VALUES ${valueTuples.join(', ')}
     ON CONFLICT (DEAL_ID, USER_ID, EMAIL) DO UPDATE SET
       CONTACT_TYPE = EXCLUDED.CONTACT_TYPE,
       UPDATED_AT = CURRENT_TIMESTAMP`,
}

// ─── CONTACTS ────────────────────────────────────────────────────────────────

export const contacts = {
  upsert: (schema, valueTuples) =>
    `INSERT INTO ${schema}.CONTACTS
       (USER_ID, EMAIL, NAME, COMPANY_NAME, TITLE, PHONE_NUMBER, CREATED_AT, UPDATED_AT)
     VALUES ${valueTuples.join(', ')}
     ON CONFLICT (USER_ID, EMAIL) DO UPDATE SET
       NAME = EXCLUDED.NAME,
       COMPANY_NAME = EXCLUDED.COMPANY_NAME,
       TITLE = EXCLUDED.TITLE,
       PHONE_NUMBER = EXCLUDED.PHONE_NUMBER,
       UPDATED_AT = CURRENT_TIMESTAMP`,
}
```

---

## Migration Steps

The refactor is purely mechanical — no behavior changes. Each step can be done incrementally as a PR.

### Step 1 — Add `src/lib/sql.js`

Create the file as specified above. No other files are changed.

### Step 2 — Replace SQL in `src/lib/write-batcher.js`

This file has the densest SQL. Import from `sql.js` and swap inline template strings for builder calls.

```js
// Before
const sql = `INSERT INTO ${s}.DEALS (...) VALUES ${items.join(', ')} ON CONFLICT ...`

// After
import { deals } from './sql.js'
const sql = deals.upsert(s, items)
```

### Step 3 — Replace SQL in pipeline and command files

Files to update:

- `src/lib/pipeline.js` — stuck batch detection, dead-letter sweeps
- `src/lib/queries.js` — AI audit reads/writes, status updates
- `src/commands/sync-deal-states.js` — email metadata sync
- `src/commands/run-filter-pipeline.js` — claim/select/update batch
- `src/commands/claim-filter-batch.js` — same pattern
- `src/commands/claim-classify-batch.js` — classify batch claim
- `src/commands/fetch-and-classify.js` — select pending classification

### Step 4 — Add unit tests for `sql.js`

Because the functions are pure (string in → string out), they can be tested without a live SxT connection:

```js
// __tests__/sql.test.js
import { dealStates } from '../src/lib/sql.js'

test('claimBatch produces correct SQL', () => {
  const sql = dealStates.claimBatch('MYSCHEMA', 'batch-123', 'filtering', 'pending', 50)
  expect(sql).toContain("SET STATUS = 'filtering'")
  expect(sql).toContain("BATCH_ID = 'batch-123'")
  expect(sql).toContain('LIMIT 50')
})

test('esc prevents quote injection', () => {
  const sql = dealStates.touchBatch('S', "'; DROP TABLE DEAL_STATES; --")
  expect(sql).not.toContain('DROP TABLE')
  expect(sql).toContain("''")
})
```

---

## SxT SQL Constraints to Keep in Mind

When adding future queries, stay within SxT's supported subset:

| ✅ Supported                                             | ❌ Not Supported                        |
| -------------------------------------------------------- | --------------------------------------- |
| `SELECT`, `INSERT`, `UPDATE`, `DELETE`                   | CTEs (`WITH ... AS`)                    |
| `WHERE`, `GROUP BY`, `HAVING`, `LIMIT`, `OFFSET`         | Division operator `/`                   |
| `UNION ALL`                                              | `ORDER BY` on all column types (verify) |
| Inner JOINs on a single column                           | Multi-column or outer JOINs             |
| `SUM`, `COUNT` aggregates                                | `AVG`, `MAX`, `MIN` (verify)            |
| `ON CONFLICT (...) DO UPDATE`                            | `MERGE` statement                       |
| `CURRENT_TIMESTAMP`                                      | `NOW()` (verify)                        |
| `INTERVAL 'N' MINUTE`                                    | String/binary inequality (`>`, `<`)     |
| `BOOLEAN`, `VARCHAR`, `BIGINT`, `DECIMAL75`, `TIMESTAMP` | Division in expressions                 |

Always verify new functions against [docs.spaceandtime.io](https://docs.spaceandtime.io/) before writing a query that uses them.

---

## Files Affected

| File                                   | Change                                     |
| -------------------------------------- | ------------------------------------------ |
| `src/lib/sql.js`                       | **NEW** — the builder module               |
| `src/lib/write-batcher.js`             | Replace inline SQL with builder calls      |
| `src/lib/pipeline.js`                  | Replace inline SQL with builder calls      |
| `src/lib/queries.js`                   | Replace inline SQL with builder calls      |
| `src/commands/sync-deal-states.js`     | Replace inline SQL                         |
| `src/commands/run-filter-pipeline.js`  | Replace inline SQL                         |
| `src/commands/claim-filter-batch.js`   | Replace inline SQL                         |
| `src/commands/claim-classify-batch.js` | Replace inline SQL                         |
| `src/commands/fetch-and-classify.js`   | Replace inline SQL                         |
| `__tests__/sql.test.js`                | **NEW** — unit tests for builder functions |

No changes to `sxt-client.js`, `action.yml`, or any config files.
