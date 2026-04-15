# sync-deal-values Backfill Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a manually-triggered W3 workflow that backfills `DEALS.VALUE` and `DEALS.CURRENCY` for rows written with bad values during the 2026-03-31 → fix-deploy bug window, by re-parsing authoritative AI output from `AI_EVALUATION_AUDITS`.

**Architecture:** New `sync-deal-values` command in `src/commands/`, registered in `src/main.js`. Claims paginated batches of affected deals, joins through `EMAIL_THREAD_EVALUATIONS` → `AI_EVALUATION_AUDITS` to get stored AI JSON, re-parses through existing `parseAndValidate` (same zod schema as live pipeline), and issues per-row UPDATEs with an idempotency guard in the WHERE clause. New W3 workflow YAML `ds-sync-deal-values-<commit7>` with `workflow_dispatch` only — no cron.

**Tech Stack:** Node 24 ESM, Jest (`--experimental-vm-modules`), `@actions/core`, existing SxT client in `src/lib/db.js`, existing `parseAndValidate` in `src/lib/ai.js`.

**Design doc:** [docs/plans/2026-04-15-sync-deal-values-backfill-design.md](./2026-04-15-sync-deal-values-backfill-design.md)

---

## Context for the engineer

**Codebase layout you need to know:**
- Command dispatch: `src/main.js` routes the `command` input to a handler. Add new commands to the `COMMANDS` map.
- SQL builders: `src/lib/sql/*.js` expose per-table object literals whose methods return SQL strings. Schema/id/string sanitation comes from `src/lib/sql/sanitize.js`.
- Database client: `src/lib/db.js` exports `authenticate(authUrl, authSecret)` → `{ jwt }` and `executeSql(apiUrl, jwt, biscuit, sql)` → parsed rows. Re-auth on 401 is handled internally.
- AI parser: `src/lib/ai.js` exports `parseAndValidate(rawJsonString, threadOrder?)`. Throws on bad JSON or schema violation. Returns array of thread objects with `thread_id`, `deal_value` (number|null), `deal_currency` (string|null), etc.
- Bundling: `npm run package` regenerates `dist/index.js` — always commit alongside source.
- W3 workflows: YAML lives in `.github/workflows/`. `workflow_dispatch` runs manually via `mcp__w3__trigger-workflow` or the W3 UI. Secrets referenced as `${{ secrets.NAME }}`.

**Authoritative data path:** A deal's thread lives in `DEAL_STATES.THREAD_ID`. `EMAIL_THREAD_EVALUATIONS` has `THREAD_ID` (PK) and `AI_EVALUATION_AUDIT_ID` (FK). `AI_EVALUATION_AUDITS.AI_EVALUATION` is the raw AI JSON array for the batch — each element has a `thread_id` matching the deal's thread.

**Idempotency:** The UPDATE's `WHERE` includes `(VALUE = 0 OR VALUE IS NULL)` so re-running after a partial run doesn't overwrite rows we already fixed.

**Running tests:**
```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/<file>.test.js
```

---

## Task 1: SQL builders for affected deals + UPDATE

**Files:**
- Modify: `src/lib/sql/deals.js` (extend the `deals` export)
- Test: none yet — builders are trivial templates; covered by the command test in Task 3.

**Step 1: Add two methods**

Edit `src/lib/sql/deals.js`. Append to the `deals` object:

```js
  findAffectedForBackfill: (schema, { startDate, cursorId, limit }) => {
    const s = sanitizeSchema(schema)
    // startDate expected in ISO form YYYY-MM-DD; cursorId is the last seen ID or '' for start
    return `SELECT ID, THREAD_ID, USER_ID FROM ${s}.DEALS WHERE (VALUE = 0 OR VALUE IS NULL) AND CREATED_AT >= '${startDate}' AND ID > '${cursorId}' ORDER BY ID LIMIT ${Number(limit)}`
  },

  backfillValue: (schema, { dealId, value, currency }) => {
    const s = sanitizeSchema(schema)
    const safeId = sanitizeId(dealId)
    const safeCurrency = sanitizeString(currency || 'USD')
    const numValue = Number.isFinite(value) ? value : 0
    return `UPDATE ${s}.DEALS SET VALUE = ${numValue}, CURRENCY = '${safeCurrency}', UPDATED_AT = CURRENT_TIMESTAMP WHERE ID = '${safeId}' AND (VALUE = 0 OR VALUE IS NULL)`
  },
```

Import `sanitizeId` and `sanitizeString` if not already imported at top:

```js
import { sanitizeSchema, sanitizeId, sanitizeString } from './sanitize.js'
```

Verify which helpers are already imported; only add missing ones.

**Step 2: Commit**

```bash
git add src/lib/sql/deals.js
git commit -m "feat: add SQL builders for deal-value backfill"
```

---

## Task 2: SQL builder to fetch audit JSON by thread

**Files:**
- Modify: `src/lib/sql/audits.js`

**Step 1: Add a new method**

Open `src/lib/sql/audits.js`. Read the existing exports to see the pattern. Append to the `audits` object (or whatever it's exported as):

```js
  findByThread: (schema, threadId) => {
    const s = sanitizeSchema(schema)
    const safeTid = sanitizeId(threadId)
    return `SELECT A.AI_EVALUATION FROM ${s}.AI_EVALUATION_AUDITS A JOIN ${s}.EMAIL_THREAD_EVALUATIONS E ON E.AI_EVALUATION_AUDIT_ID = A.ID WHERE E.THREAD_ID = '${safeTid}' LIMIT 1`
  },
```

Ensure `sanitizeId` is imported (check top of file).

**Step 2: Commit**

```bash
git add src/lib/sql/audits.js
git commit -m "feat: add SQL builder to fetch audit JSON by thread"
```

---

## Task 3: sync-deal-values command — happy path + tests

**Files:**
- Create: `src/commands/sync-deal-values.js`
- Test: `__tests__/sync-deal-values.test.js`

**Step 1: Write the failing test (happy path + skips)**

Create `__tests__/sync-deal-values.test.js`:

```js
import { jest } from '@jest/globals'

jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn(),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

const executeSql = jest.fn()
const authenticate = jest.fn().mockResolvedValue('jwt-stub')

jest.unstable_mockModule('../src/lib/db.js', () => ({
  authenticate,
  executeSql: (apiUrl, jwt, biscuit, sql) => executeSql(sql),
  acquireRateLimitToken: jest.fn().mockResolvedValue(true),
}))

// Same three prompt mocks as __tests__/ai.test.js
jest.unstable_mockModule('../prompts/system.md', () => ({ default: 's' }))
jest.unstable_mockModule('../prompts/user.md', () => ({ default: 'u {{THREAD_DATA}}' }))
jest.unstable_mockModule('../prompts/system-llama.md', () => ({ default: 'sl' }))

const core = await import('@actions/core')
const { runSyncDealValues } = await import('../src/commands/sync-deal-values.js')

function setInputs(inputs) {
  core.getInput.mockImplementation((name) => inputs[name] ?? '')
}

const auditJson = JSON.stringify([
  {
    thread_id: 'thread-1',
    is_deal: true,
    category: 'in_progress',
    deal_type: 'brand_collaboration',
    deal_name: 'Acme',
    deal_value: 2500,
    deal_currency: 'EUR',
    ai_score: 8,
  },
  {
    thread_id: 'thread-2',
    is_deal: true,
    category: 'in_progress',
    deal_type: 'sponsorship',
    deal_name: 'Beta',
    deal_value: null,
    deal_currency: null,
    ai_score: 5,
  },
])

beforeEach(() => {
  executeSql.mockReset()
  setInputs({
    'sxt-auth-url': 'https://auth',
    'sxt-auth-secret': 'secret',
    'sxt-api-url': 'https://api',
    'sxt-biscuit': 'bisc',
    'sxt-schema': 'dealsync_stg_v1',
    'backfill-start-date': '2026-03-31',
    'backfill-batch-size': '500',
    'backfill-dry-run': 'false',
  })
})

test('backfills affected deal from audit JSON', async () => {
  executeSql
    // page 1: one affected deal
    .mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1', USER_ID: 'u1' }])
    // audit lookup
    .mockResolvedValueOnce([{ AI_EVALUATION: auditJson }])
    // UPDATE
    .mockResolvedValueOnce([])
    // page 2: empty, terminate
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(1)
  expect(result.skipped.auditMissing).toBe(0)
  expect(result.totalScanned).toBe(1)
  // assert UPDATE included the recovered value and currency
  const updateCall = executeSql.mock.calls.find(([sql]) => sql.startsWith('UPDATE'))
  expect(updateCall[0]).toContain('VALUE = 2500')
  expect(updateCall[0]).toContain("CURRENCY = 'EUR'")
  expect(updateCall[0]).toContain("WHERE ID = 'deal-1'")
  expect(updateCall[0]).toContain('VALUE = 0 OR VALUE IS NULL')
})

test('skips when audit is missing', async () => {
  executeSql
    .mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1', USER_ID: 'u1' }])
    .mockResolvedValueOnce([]) // audit lookup empty
    .mockResolvedValueOnce([]) // page 2 empty

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(0)
  expect(result.skipped.auditMissing).toBe(1)
  // no UPDATE issued
  expect(executeSql.mock.calls.find(([sql]) => sql.startsWith('UPDATE'))).toBeUndefined()
})

test('skips when thread not present in audit payload', async () => {
  executeSql
    .mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-missing', USER_ID: 'u1' }])
    .mockResolvedValueOnce([{ AI_EVALUATION: auditJson }]) // payload has thread-1 and thread-2, not thread-missing
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(0)
  expect(result.skipped.threadNotFound).toBe(1)
})

test('skips when audit deal_value is null', async () => {
  executeSql
    .mockResolvedValueOnce([{ ID: 'deal-2', THREAD_ID: 'thread-2', USER_ID: 'u1' }])
    .mockResolvedValueOnce([{ AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(0)
  expect(result.skipped.valueNull).toBe(1)
})

test('skips when audit JSON is unparsable', async () => {
  executeSql
    .mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1', USER_ID: 'u1' }])
    .mockResolvedValueOnce([{ AI_EVALUATION: 'not valid json' }])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(0)
  expect(result.skipped.parseError).toBe(1)
})

test('dry-run does not issue UPDATE', async () => {
  setInputs({
    'sxt-auth-url': 'https://auth',
    'sxt-auth-secret': 'secret',
    'sxt-api-url': 'https://api',
    'sxt-biscuit': 'bisc',
    'sxt-schema': 'dealsync_stg_v1',
    'backfill-start-date': '2026-03-31',
    'backfill-batch-size': '500',
    'backfill-dry-run': 'true',
  })
  executeSql
    .mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1', USER_ID: 'u1' }])
    .mockResolvedValueOnce([{ AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(1) // counts as would-recover
  expect(executeSql.mock.calls.find(([sql]) => sql.startsWith('UPDATE'))).toBeUndefined()
})

test('pagination advances via cursor and stops on empty page', async () => {
  executeSql
    // page 1: full batch of 2 (we'll set batch-size=2 below via input override)
    .mockResolvedValueOnce([
      { ID: 'deal-a', THREAD_ID: 'thread-1', USER_ID: 'u1' },
      { ID: 'deal-b', THREAD_ID: 'thread-1', USER_ID: 'u1' },
    ])
    // per-deal lookups + updates × 2
    .mockResolvedValueOnce([{ AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce([{ AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([])
    // page 2: empty, terminate
    .mockResolvedValueOnce([])

  setInputs({
    'sxt-auth-url': 'https://auth',
    'sxt-auth-secret': 'secret',
    'sxt-api-url': 'https://api',
    'sxt-biscuit': 'bisc',
    'sxt-schema': 'dealsync_stg_v1',
    'backfill-start-date': '2026-03-31',
    'backfill-batch-size': '2',
    'backfill-dry-run': 'false',
  })

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(2)
  expect(result.totalScanned).toBe(2)
  // second page SELECT used cursor = 'deal-b'
  const selectCalls = executeSql.mock.calls.filter(([sql]) => sql.startsWith('SELECT ID'))
  expect(selectCalls[1][0]).toContain("ID > 'deal-b'")
})
```

Run:

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sync-deal-values.test.js
```

Expected: FAIL (module not found).

**Step 2: Implement**

Create `src/commands/sync-deal-values.js`:

```js
import * as core from '@actions/core'
import { authenticate, executeSql } from '../lib/db.js'
import { parseAndValidate } from '../lib/ai.js'
import { sanitizeSchema } from '../lib/sql/sanitize.js'
import { deals as dealsSql } from '../lib/sql/deals.js'
import { audits as auditsSql } from '../lib/sql/audits.js'

export async function runSyncDealValues() {
  const authUrl = core.getInput('sxt-auth-url')
  const authSecret = core.getInput('sxt-auth-secret')
  const apiUrl = core.getInput('sxt-api-url')
  const biscuit = core.getInput('sxt-biscuit')
  const schema = sanitizeSchema(core.getInput('sxt-schema'))
  const startDate = core.getInput('backfill-start-date') || '2026-03-31'
  const batchSize = parseInt(core.getInput('backfill-batch-size') || '500', 10)
  const dryRun = core.getInput('backfill-dry-run') === 'true'

  console.log(
    `[sync-deal-values] starting startDate=${startDate} batchSize=${batchSize} dryRun=${dryRun}`,
  )

  const jwt = await authenticate(authUrl, authSecret)
  const exec = (sql) => executeSql(apiUrl, jwt, biscuit, sql)

  const summary = {
    recovered: 0,
    skipped: { auditMissing: 0, threadNotFound: 0, valueNull: 0, parseError: 0 },
    totalScanned: 0,
  }

  let cursorId = ''
  while (true) {
    const page = await exec(
      dealsSql.findAffectedForBackfill(schema, { startDate, cursorId, limit: batchSize }),
    )
    if (!page || page.length === 0) break

    for (const row of page) {
      summary.totalScanned++
      const dealId = row.ID
      const threadId = row.THREAD_ID

      const auditRows = await exec(auditsSql.findByThread(schema, threadId))
      if (!auditRows || auditRows.length === 0) {
        console.warn(`[sync-deal-values] skip deal_id=${dealId} thread_id=${threadId} reason=audit_missing`)
        summary.skipped.auditMissing++
        continue
      }

      let parsed
      try {
        parsed = parseAndValidate(auditRows[0].AI_EVALUATION)
      } catch (err) {
        console.warn(`[sync-deal-values] skip deal_id=${dealId} thread_id=${threadId} reason=parse_error err=${err.message}`)
        summary.skipped.parseError++
        continue
      }

      const entry = parsed.find((t) => t.thread_id === threadId)
      if (!entry) {
        console.warn(`[sync-deal-values] skip deal_id=${dealId} thread_id=${threadId} reason=thread_not_in_audit`)
        summary.skipped.threadNotFound++
        continue
      }
      if (entry.deal_value == null) {
        console.warn(`[sync-deal-values] skip deal_id=${dealId} thread_id=${threadId} reason=deal_value_null`)
        summary.skipped.valueNull++
        continue
      }

      const value = Number(entry.deal_value)
      const currency = entry.deal_currency || 'USD'
      if (!dryRun) {
        await exec(dealsSql.backfillValue(schema, { dealId, value, currency }))
      }
      console.log(
        `[sync-deal-values] ${dryRun ? 'would-recover' : 'recovered'} deal_id=${dealId} thread_id=${threadId} value=${value} currency=${currency}`,
      )
      summary.recovered++
    }

    cursorId = page[page.length - 1].ID
    if (page.length < batchSize) break
  }

  console.log(
    `[sync-deal-values] done recovered=${summary.recovered} skipped_audit_missing=${summary.skipped.auditMissing} skipped_thread_not_found=${summary.skipped.threadNotFound} skipped_value_null=${summary.skipped.valueNull} skipped_parse_error=${summary.skipped.parseError} scanned=${summary.totalScanned}`,
  )

  return summary
}
```

Run the test again:

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/sync-deal-values.test.js
```

Expected: all 7 tests PASS.

**Step 3: Full test suite**

```bash
npm test 2>&1 | tail -8
```

Expected: baseline pass count + 7 new tests pass, 3 pre-existing failures unchanged.

**Step 4: Commit**

```bash
git add src/commands/sync-deal-values.js __tests__/sync-deal-values.test.js
git commit -m "feat: add sync-deal-values command to backfill VALUE/CURRENCY"
```

---

## Task 4: Register the command

**Files:**
- Modify: `src/main.js`

**Step 1: Register**

In `src/main.js`:

1. Add import near the other command imports:

```js
import { runSyncDealValues } from './commands/sync-deal-values.js'
```

2. Add to the `COMMANDS` map:

```js
'sync-deal-values': runSyncDealValues,
```

**Step 2: Regenerate bundle**

```bash
npm run package
```

Expected: `dist/index.js` regenerated.

**Step 3: Full test suite**

```bash
npm test 2>&1 | tail -5
```

Expected: all previously-passing tests still pass.

**Step 4: Commit**

```bash
git add src/main.js dist/index.js
git commit -m "feat: register sync-deal-values in command dispatch"
```

---

## Task 5: W3 workflow YAML (testnet)

**Files:**
- Create: `.github/workflows/dealsync-sync-deal-values.testnet.yml`

**Step 1: Get the current HEAD commit**

```bash
git rev-parse HEAD
git rev-parse --short=7 HEAD
```

Record both. Call them `<full-sha>` and `<sha7>` below.

**Step 2: Create the workflow file**

Copy `.github/workflows/dealsync-classify.testnet.yml` as a reference for the environment hash and authority. Create `.github/workflows/dealsync-sync-deal-values.testnet.yml`:

```yaml
name: ds-sync-deal-values-<sha7>
authority: '<copy from classify.testnet.yml>'
on:
  workflow_dispatch:

concurrency:
  group: sync-deal-values-testnet
  cancel-in-progress: false

jobs:
  backfill:
    timeout-minutes: 60
    runs-on: ubuntu-latest
    environment: '<copy testnet env hash from classify.testnet.yml>'
    steps:
      - id: backfill
        name: Run deal-value backfill
        timeout-minutes: 60
        uses: creatorland/dealsync-action@<full-sha>
        with:
          command: sync-deal-values
          sxt-auth-url: ${{ secrets.SXT_AUTH_URL }}
          sxt-auth-secret: ${{ secrets.SXT_AUTH_SECRET }}
          sxt-api-url: ${{ secrets.SXT_API_URL }}
          sxt-biscuit: ${{ secrets.SXT_BISCUIT }}
          sxt-schema: ${{ secrets.SXT_SCHEMA }}
          backfill-start-date: '2026-03-31'
          backfill-batch-size: '500'
          backfill-dry-run: 'false'
          db-max-retries: ${{ secrets.DB_MAX_RETRIES }}
```

Replace `<sha7>`, `<full-sha>`, the authority, and the environment placeholders.

**Step 3: Commit + push**

```bash
git add .github/workflows/dealsync-sync-deal-values.testnet.yml
git commit -m "feat: add sync-deal-values testnet workflow"
git push
```

---

## Task 6: Deploy to W3 testnet

**Step 1: Deploy via MCP**

Using `mcp__w3__deploy-workflow` with `url: https://1.w3-testnet.io` and the full YAML from Task 5.

Expected: deploy succeeds. Trigger phase may return "Invalid params" — that's fine, the workflow is `workflow_dispatch` and will be triggered manually in Step 2.

**Step 2: Manual trigger (dry-run first)**

The YAML currently has `backfill-dry-run: 'false'`. For first run, consider editing to `'true'`, re-deploying, triggering to see counts without mutating, then flipping to `'false'` for the real run.

Use `mcp__w3__trigger-workflow` with the deployed workflow name.

**Step 3: Verify via SxT**

Check the run's logs for the final summary line:

```
[sync-deal-values] done recovered=... skipped_audit_missing=... ...
```

Sanity-check a few corrected rows:

```sql
SELECT ID, VALUE, CURRENCY FROM DEALSYNC_STG_V1.DEALS
WHERE ID IN ('<deal_id_from_logs>', ...)
```

Each should show a non-zero `VALUE` and, if applicable, the non-USD `CURRENCY` from the audit.

---

## Task 7: Deploy to betanet and prod (after testnet verification)

Repeat Task 5–6 with the betanet authority and environment hash. Create `.github/workflows/dealsync-sync-deal-values.betanet.yml`. Deploy to `https://1.w3-betanet.io`. Run a small `backfill-batch-size: '100'` canary, inspect the DEALS table, then re-trigger with full `'500'` until a run reports `totalScanned=0`.

Prod deploy follows the same pattern. Target the prod environment hash.

---

## Out of scope

- Prompt re-run from email content. Audit JSON is trusted.
- Failure table. Skip-and-warn logs are the sole diagnostic surface.
- Parallelism. Per-deal audit lookup + UPDATE is sequential inside a batch. If throughput is insufficient on 4,771 rows, introduce `runPool` in a follow-up.
