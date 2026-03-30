# Classify Pipeline Schema Rev 4 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Update classify pipeline contacts writes to use new composite-PK schemas, and fix deals.id to use thread_id.

**Architecture:** Four tasks: (1) fix broken test infrastructure + deals.id bug, (2) WriteBatcher gets configurable coreSchema + coreContacts queue, (3) pipeline builds two separate contact value sets, (4) bundle rebuild.

**Tech Stack:** Node 24, ESM, Jest (ESM), rollup for bundling.

---

### Task 1: Fix broken test infrastructure + deals.id bug

The classify pipeline test suite can't even load — the sxt-client mock is missing `acquireRateLimitToken`. The full-pipeline test also has a stale assertion (`pushStateUpdates` is never called by the pipeline — state updates are written directly). Fix both, plus the deals.id bug.

**Files:**

- Modify: `__tests__/run-classify-pipeline.test.js`
- Modify: `src/commands/run-classify-pipeline.js`

**Changes to `__tests__/run-classify-pipeline.test.js`:**

1. Add `acquireRateLimitToken` to the sxt-client mock (lines 30-37). Add a top-level mock fn and include it in the module mock:

```js
const mockAcquireRateLimitToken = jest.fn().mockResolvedValue(undefined)
```

Then in `unstable_mockModule('../src/lib/sxt-client.js', ...)` add:

```js
acquireRateLimitToken: mockAcquireRateLimitToken,
```

2. In the full-pipeline test (line 230), fix two stale assertions:

Replace line 273:

```js
// OLD: expect(mockBatcherInstance.pushContactDeletes).toHaveBeenCalledTimes(1)
// NEW: pushContactDeletes still called in current code, will be removed in Task 3
expect(mockBatcherInstance.pushContactDeletes).toHaveBeenCalledTimes(1)
```

Replace line 275 — `pushStateUpdates` is NEVER called by the pipeline (state updates use `execNoRL` directly):

```js
// OLD: expect(mockBatcherInstance.pushStateUpdates).toHaveBeenCalledTimes(1)
// NEW: State updates are written directly via execNoRL, not through batcher
// Remove this line entirely
```

3. Add `CREATOR_EMAIL` to `makeBatchRows` helper (line 133-141):

```js
function makeBatchRows(count = 2) {
  return Array.from({ length: count }, (_, i) => ({
    EMAIL_METADATA_ID: `em-${i + 1}`,
    MESSAGE_ID: `msg-${i + 1}`,
    USER_ID: 'user-1',
    THREAD_ID: `thread-${i + 1}`,
    SYNC_STATE_ID: 'ss-1',
    CREATOR_EMAIL: 'creator@test.com',
  }))
}
```

**Changes to `src/commands/run-classify-pipeline.js`:**

Change line 408:

```js
// BEFORE:
const dealId = uuidv7()
// AFTER:
const dealId = threadId
```

**Verification:**

```
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-classify-pipeline.test.js
```

Expected: test suite loads and all existing tests pass.

**Commit:**

```bash
git add __tests__/run-classify-pipeline.test.js src/commands/run-classify-pipeline.js
git commit -m "fix: repair classify pipeline tests and use thread_id as deal ID"
```

---

### Task 2: WriteBatcher — configurable coreSchema + coreContacts queue + simplified contacts SQL

Add `coreSchema` constructor option (used by `coreContacts` queue SQL). Add `coreContacts` queue for `email_core_staging.CONTACTS` with COALESCE ON CONFLICT. Update `contacts` case to 4-column `DEAL_CONTACTS` schema with ON CONFLICT.

**Files:**

- Modify: `src/lib/write-batcher.js`
- Modify: `__tests__/write-batcher.test.js`

**Changes to `src/lib/write-batcher.js`:**

1. Update constructor — add `coreSchema` option:

```js
constructor(executeSqlFn, schema, { flushIntervalMs = 5000, flushThreshold = 10, coreSchema = 'EMAIL_CORE_STAGING' } = {}) {
  this._executeSqlFn = executeSqlFn
  this._schema = schema
  this._coreSchema = coreSchema
  this._flushThreshold = flushThreshold

  this._queues = {
    evals: { items: [], waiters: [] },
    dealDeletes: { items: [], waiters: [] },
    deals: { items: [], waiters: [] },
    contactDeletes: { items: [], waiters: [] },
    contacts: { items: [], waiters: [] },
    coreContacts: { items: [], waiters: [] },
    stateUpdates: { items: [], waiters: [] },
    batchEvents: { items: [], waiters: [] },
  }

  this._timer = setInterval(() => this._flushAll(), flushIntervalMs)
}
```

2. Add `pushCoreContacts` method after `pushContacts`:

```js
/** Push pre-built VALUES strings for core contacts upsert (COALESCE ON CONFLICT) */
pushCoreContacts(rows) {
  return this._push('coreContacts', rows)
}
```

3. In `_executeQueue`, replace `contacts` case:

```js
case 'contacts': {
  const sql = `INSERT INTO ${s}.DEAL_CONTACTS (DEAL_ID, USER_ID, EMAIL, CONTACT_TYPE, CREATED_AT, UPDATED_AT) VALUES ${items.join(', ')} ON CONFLICT (DEAL_ID, USER_ID, EMAIL) DO UPDATE SET CONTACT_TYPE = COALESCE(EXCLUDED.CONTACT_TYPE, DEAL_CONTACTS.CONTACT_TYPE), UPDATED_AT = CURRENT_TIMESTAMP`
  await this._executeSqlFn(sql)
  break
}
```

4. Add `coreContacts` case after `contacts`:

```js
case 'coreContacts': {
  const cs = this._coreSchema
  const sql = `INSERT INTO ${cs}.CONTACTS (USER_ID, EMAIL, NAME, COMPANY_NAME, TITLE, PHONE_NUMBER, CREATED_AT, UPDATED_AT) VALUES ${items.join(', ')} ON CONFLICT (USER_ID, EMAIL) DO UPDATE SET NAME = COALESCE(EXCLUDED.NAME, CONTACTS.NAME), COMPANY_NAME = COALESCE(EXCLUDED.COMPANY_NAME, CONTACTS.COMPANY_NAME), TITLE = COALESCE(EXCLUDED.TITLE, CONTACTS.TITLE), PHONE_NUMBER = COALESCE(EXCLUDED.PHONE_NUMBER, CONTACTS.PHONE_NUMBER), UPDATED_AT = CURRENT_TIMESTAMP`
  await this._executeSqlFn(sql)
  break
}
```

**Changes to `__tests__/write-batcher.test.js`:**

1. Update constructor test (line 35-46) — add `coreContacts`:

```js
expect(batcher._queues.coreContacts).toBeDefined()
```

2. Replace existing contacts test (line 132-145) with new 4-column format:

```js
it('flushes contacts with simplified 4-column ON CONFLICT upsert', async () => {
  const batcher = makeBatcher(mockExec, { flushThreshold: 1 })

  const p = batcher.pushContacts([
    "('thread-1', 'user-1', 'email@co.com', 'primary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
  ])
  await p

  const sql = mockExec.mock.calls[0][0]
  expect(sql).toContain('INSERT INTO TEST_SCHEMA.DEAL_CONTACTS')
  expect(sql).toContain('DEAL_ID, USER_ID, EMAIL, CONTACT_TYPE')
  expect(sql).toContain('ON CONFLICT (DEAL_ID, USER_ID, EMAIL) DO UPDATE SET')
  expect(sql).toContain('COALESCE(EXCLUDED.CONTACT_TYPE, DEAL_CONTACTS.CONTACT_TYPE)')
  expect(sql).not.toContain('CONTACT_ID')
  expect(sql).not.toContain('IS_FAVORITE')

  batcher.stop()
})
```

3. Add coreContacts tests:

```js
describe('coreContacts queue', () => {
  it('has coreContacts queue in _queues', () => {
    const batcher = makeBatcher(mockExec)
    expect(batcher._queues.coreContacts).toBeDefined()
    batcher.stop()
  })

  it('flushes coreContacts with COALESCE ON CONFLICT SQL using coreSchema', async () => {
    const batcher = new WriteBatcher(mockExec, 'TEST_SCHEMA', {
      flushIntervalMs: 60000,
      flushThreshold: 1,
      coreSchema: 'MY_CORE_SCHEMA',
    })

    await batcher.pushCoreContacts([
      "('user-1', 'alice@co.com', 'Alice', NULL, 'CEO', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])

    expect(mockExec).toHaveBeenCalledTimes(1)
    const sql = mockExec.mock.calls[0][0]
    expect(sql).toContain('INSERT INTO MY_CORE_SCHEMA.CONTACTS')
    expect(sql).toContain('USER_ID, EMAIL, NAME, COMPANY_NAME, TITLE, PHONE_NUMBER')
    expect(sql).toContain('ON CONFLICT (USER_ID, EMAIL) DO UPDATE SET')
    expect(sql).toContain('COALESCE(EXCLUDED.NAME, CONTACTS.NAME)')
    expect(sql).toContain('COALESCE(EXCLUDED.COMPANY_NAME, CONTACTS.COMPANY_NAME)')
    expect(sql).toContain('COALESCE(EXCLUDED.TITLE, CONTACTS.TITLE)')
    expect(sql).toContain('COALESCE(EXCLUDED.PHONE_NUMBER, CONTACTS.PHONE_NUMBER)')

    batcher.stop()
  })

  it('defaults coreSchema to EMAIL_CORE_STAGING', async () => {
    const batcher = makeBatcher(mockExec, { flushThreshold: 1 })

    await batcher.pushCoreContacts([
      "('user-1', 'bob@co.com', 'Bob', NULL, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    ])

    const sql = mockExec.mock.calls[0][0]
    expect(sql).toContain('INSERT INTO EMAIL_CORE_STAGING.CONTACTS')

    batcher.stop()
  })
})
```

**Verification:**

```
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/write-batcher.test.js
```

**Commit:**

```bash
git add src/lib/write-batcher.js __tests__/write-batcher.test.js
git commit -m "feat: add coreContacts queue with configurable coreSchema to WriteBatcher"
```

---

### Task 3: Pipeline — two-table contacts upsert

Read `email-core-schema` input, pass `coreSchema` to WriteBatcher, add `toSqlNullable` helper, replace Step 6 contacts logic, remove `pushContactDeletes`, update tests.

**Files:**

- Modify: `src/commands/run-classify-pipeline.js`
- Modify: `__tests__/run-classify-pipeline.test.js`

**Changes to `src/commands/run-classify-pipeline.js`:**

1. Add `toSqlNullable` helper before `runClassifyPipeline`:

```js
function toSqlNullable(s) {
  return s ? `'${sanitizeString(s)}'` : 'NULL'
}
```

2. After `const schema = sanitizeSchema(core.getInput('schema'))` (line 27), add:

```js
const coreSchema = core.getInput('email-core-schema') || 'EMAIL_CORE_STAGING'
```

3. Update WriteBatcher constructor call (line 53) — add `coreSchema`:

```js
const batcher = new WriteBatcher(execNoRL, schema, { flushIntervalMs, flushThreshold, coreSchema })
```

4. Replace Step 6 (lines 424-460) with:

```js
// -----------------------------------------------------------------------
// Step 6: Save deal contacts via batcher (two-table upsert)
// -----------------------------------------------------------------------

if (dealThreads.length > 0) {
  const coreContactValues = []
  const dealContactValues = []

  for (const thread of dealThreads) {
    const mc = thread.main_contact
    if (!mc || !mc.email) continue

    const threadId = sanitizeId(thread.thread_id)
    const userId = userByThread[threadId] ? sanitizeId(userByThread[threadId]) : ''
    const contactEmail = sanitizeString(mc.email)
    const nameVal = toSqlNullable(mc.name)
    const companyVal = toSqlNullable(mc.company)
    const titleVal = toSqlNullable(mc.title)
    const phoneVal = toSqlNullable(mc.phone_number)

    // Core contacts — COALESCE preserves existing non-null values
    coreContactValues.push(
      `('${userId}', '${contactEmail}', ${nameVal}, ${companyVal}, ${titleVal}, ${phoneVal}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
    )

    // Deal contacts — simplified 4-column relationship upsert
    dealContactValues.push(
      `('${threadId}', '${userId}', '${contactEmail}', 'primary', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
    )
  }

  if (coreContactValues.length > 0) {
    await batcher.pushCoreContacts(coreContactValues)
  }

  if (dealContactValues.length > 0) {
    await batcher.pushContacts(dealContactValues)
  }

  console.log(`[run-classify-pipeline] ${dealContactValues.length} contacts saved (core + deal)`)
}
```

**Changes to `__tests__/run-classify-pipeline.test.js`:**

1. Add `pushCoreContacts` to `mockBatcherInstance` (line 86-96):

```js
pushCoreContacts: jest.fn().mockResolvedValue(undefined),
```

2. Reset it in `beforeEach`:

```js
mockBatcherInstance.pushCoreContacts.mockResolvedValue(undefined)
```

3. Update full-pipeline test assertions (lines 270-276):

```js
// Verify batcher was used for all save operations
expect(mockBatcherInstance.pushEvals).toHaveBeenCalledTimes(1)
expect(mockBatcherInstance.pushDealDeletes).toHaveBeenCalledTimes(1) // non-deal threads
expect(mockBatcherInstance.pushDeals).toHaveBeenCalledTimes(1) // deal threads
expect(mockBatcherInstance.pushCoreContacts).toHaveBeenCalledTimes(1) // core contacts
expect(mockBatcherInstance.pushContacts).toHaveBeenCalledTimes(1) // deal contacts
expect(mockBatcherInstance.pushContactDeletes).not.toHaveBeenCalled() // removed: ON CONFLICT handles it
expect(mockBatcherInstance.pushBatchEvents).toHaveBeenCalledTimes(1)
```

4. Update WriteBatcher constructor test (line 198-210) to expect `coreSchema`:

```js
expect(MockWriteBatcher).toHaveBeenCalledWith(expect.any(Function), 'dealsync_stg_v1', {
  flushIntervalMs: 3000,
  flushThreshold: 20,
  coreSchema: 'EMAIL_CORE_STAGING',
})
```

5. Add new tests:

```js
it('calls pushCoreContacts with (userId, email, name, company, title, phone) values', async () => {
  mockInputs()
  const rows = [
    {
      EMAIL_METADATA_ID: 'em-1',
      MESSAGE_ID: 'msg-1',
      USER_ID: 'user-42',
      THREAD_ID: 'thread-xyz',
      SYNC_STATE_ID: 'ss-1',
      CREATOR_EMAIL: 'creator@test.com',
    },
  ]
  const threads = [
    {
      thread_id: 'thread-xyz',
      is_deal: true,
      ai_score: 9,
      ai_summary: 'Great deal',
      category: 'new',
      deal_name: 'Big Deal',
      deal_type: 'sponsorship',
      deal_value: '1000',
      currency: 'USD',
      main_contact: {
        name: 'Alice',
        email: 'alice@brand.com',
        company: 'BrandCo',
        title: 'Manager',
        phone_number: '555-0101',
      },
    },
  ]

  mockRunPool.mockImplementation(async (claimFn, workerFn) => {
    mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce(rows)
    const batch = await claimFn()
    mockExecuteSql.mockResolvedValueOnce([])
    mockFetchEmails.mockResolvedValueOnce([
      { messageId: 'msg-1', id: 'em-1', threadId: 'thread-xyz', body: 'hi' },
    ])
    mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
    mockCallModel.mockResolvedValueOnce({ content: '[]' })
    mockParseAndValidate.mockReturnValueOnce(threads)
    mockExecuteSql.mockResolvedValue([])
    await workerFn(batch, { attempt: 0 })
    return { processed: 1, failed: 0 }
  })

  await runClassifyPipeline()

  expect(mockBatcherInstance.pushCoreContacts).toHaveBeenCalledTimes(1)
  const coreValues = mockBatcherInstance.pushCoreContacts.mock.calls[0][0]
  expect(coreValues).toHaveLength(1)
  expect(coreValues[0]).toContain('user-42')
  expect(coreValues[0]).toContain('alice@brand.com')
  expect(coreValues[0]).toContain("'Alice'")
  expect(coreValues[0]).toContain("'BrandCo'")
})

it('uses NULL literal for missing contact fields in coreContacts', async () => {
  mockInputs()
  const rows = [
    {
      EMAIL_METADATA_ID: 'em-1',
      MESSAGE_ID: 'msg-1',
      USER_ID: 'user-1',
      THREAD_ID: 'thread-1',
      SYNC_STATE_ID: 'ss-1',
      CREATOR_EMAIL: '',
    },
  ]
  const threads = [
    {
      thread_id: 'thread-1',
      is_deal: true,
      ai_score: 7,
      ai_summary: 'Deal',
      category: 'new',
      deal_name: 'Deal',
      deal_type: 'sponsorship',
      deal_value: '100',
      currency: 'USD',
      main_contact: { name: '', email: 'contact@co.com', company: '', title: '', phone_number: '' },
    },
  ]

  mockRunPool.mockImplementation(async (claimFn, workerFn) => {
    mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce(rows)
    const batch = await claimFn()
    mockExecuteSql.mockResolvedValueOnce([])
    mockFetchEmails.mockResolvedValueOnce([
      { messageId: 'msg-1', id: 'em-1', threadId: 'thread-1', body: 'hi' },
    ])
    mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
    mockCallModel.mockResolvedValueOnce({ content: '[]' })
    mockParseAndValidate.mockReturnValueOnce(threads)
    mockExecuteSql.mockResolvedValue([])
    await workerFn(batch, { attempt: 0 })
    return { processed: 1, failed: 0 }
  })

  await runClassifyPipeline()

  const coreValues = mockBatcherInstance.pushCoreContacts.mock.calls[0][0]
  expect(coreValues[0]).toMatch(/NULL/)
})

it('does NOT call pushContactDeletes (ON CONFLICT handles idempotency)', async () => {
  mockInputs()
  const rows = [
    {
      EMAIL_METADATA_ID: 'em-1',
      MESSAGE_ID: 'msg-1',
      USER_ID: 'user-1',
      THREAD_ID: 'thread-1',
      SYNC_STATE_ID: 'ss-1',
      CREATOR_EMAIL: '',
    },
  ]
  const threads = [
    {
      thread_id: 'thread-1',
      is_deal: true,
      ai_score: 7,
      ai_summary: 'Deal',
      category: 'new',
      deal_name: 'Deal',
      deal_type: 'sponsorship',
      deal_value: '100',
      currency: 'USD',
      main_contact: { name: 'X', email: 'x@co.com', company: '', title: '', phone_number: '' },
    },
  ]

  mockRunPool.mockImplementation(async (claimFn, workerFn) => {
    mockExecuteSql.mockResolvedValueOnce([]).mockResolvedValueOnce(rows)
    const batch = await claimFn()
    mockExecuteSql.mockResolvedValueOnce([])
    mockFetchEmails.mockResolvedValueOnce([
      { messageId: 'msg-1', id: 'em-1', threadId: 'thread-1', body: 'hi' },
    ])
    mockBuildPrompt.mockReturnValueOnce({ systemPrompt: 'sys', userPrompt: 'usr' })
    mockCallModel.mockResolvedValueOnce({ content: '[]' })
    mockParseAndValidate.mockReturnValueOnce(threads)
    mockExecuteSql.mockResolvedValue([])
    await workerFn(batch, { attempt: 0 })
    return { processed: 1, failed: 0 }
  })

  await runClassifyPipeline()

  expect(mockBatcherInstance.pushContactDeletes).not.toHaveBeenCalled()
})
```

**Verification:**

```
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/run-classify-pipeline.test.js
```

**Commit:**

```bash
git add src/commands/run-classify-pipeline.js __tests__/run-classify-pipeline.test.js
git commit -m "feat: two-table contacts upsert with toSqlNullable and configurable coreSchema"
```

---

### Task 4: Rebuild the bundle

```
npm run package
```

Verify:

```
grep -c "COALESCE(EXCLUDED.NAME" dist/index.js
grep -c "ON CONFLICT (DEAL_ID, USER_ID, EMAIL)" dist/index.js
```

Both should return `1`.

```bash
git add dist/index.js
git commit -m "build: regenerate dist/index.js for schema rev 4"
```

---

### Summary

| File                                      | Change                                                                                                                                       |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `__tests__/run-classify-pipeline.test.js` | Fix sxt-client mock (add acquireRateLimitToken), fix stale assertions, add CREATOR_EMAIL to makeBatchRows, add pushCoreContacts mock + tests |
| `src/commands/run-classify-pipeline.js`   | `dealId = threadId`, read `email-core-schema`, pass `coreSchema` to WriteBatcher, add `toSqlNullable`, replace Step 6 with two-table upsert  |
| `src/lib/write-batcher.js`                | Constructor takes `coreSchema` option, add `coreContacts` queue, update `contacts` SQL to 4-column ON CONFLICT                               |
| `__tests__/write-batcher.test.js`         | Update constructor test, replace contacts test, add coreContacts tests                                                                       |
| `dist/index.js`                           | Regenerated                                                                                                                                  |
