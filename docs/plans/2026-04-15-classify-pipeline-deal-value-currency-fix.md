# Classify Pipeline deal_value / deal_currency Fix — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix the `value = 0` / `currency = 'USD'` regression in `run-classify-pipeline` and prevent the whole class of "pipeline reads the wrong field name or wrong type from AI output" by introducing a Zod schema as the single source of truth and extracting a pure, tested deal-tuple mapper.

**Architecture:** Add `zod` as the AI-output contract. `src/lib/ai-schema.js` defines `AiThreadSchema` used by both `parseAndValidate` (for coercion/validation) and a new pure `threadToDealTuple()` in `src/lib/deal-mapper.js` (for SQL tuple construction). The classify pipeline replaces its inline mapping with a call to the mapper.

**Tech Stack:** Node 24 ESM, Jest (`--experimental-vm-modules`), Rollup, `zod@^3`, `@actions/core`.

**Design doc:** [docs/plans/2026-04-15-classify-pipeline-deal-value-currency-fix-design.md](./2026-04-15-classify-pipeline-deal-value-currency-fix-design.md)
**Bug report:** `/tmp/dealsync-action-bug-report.md`

---

## Context for the engineer

You're working in a GitHub Action (ESM, Node 24) that runs an AI email-classification pipeline on W3. Jest tests use ESM — no transform. Run a single test with:

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/<file>.test.js
```

After any source change, `npm run package` regenerates `dist/index.js` — this is what GitHub Actions actually executes. `npm run all` does format + test + package.

**Two confirmed bugs** in [src/commands/run-classify-pipeline.js:596-598](../../src/commands/run-classify-pipeline.js#L596-L598):

```js
const dealValue =
  typeof thread.deal_value === 'string' ? parseFloat(thread.deal_value) || 0 : 0
const currency = sanitizeString(thread.currency || 'USD')
```

1. `thread.deal_value` is always `number | null` at this point (coerced upstream in `parseAndValidate` at [src/lib/ai.js:264](../../src/lib/ai.js#L264)). The `typeof ... === 'string'` branch is never taken. Every deal gets `0`.
2. The AI output field is named `deal_currency`, not `currency`. The fallback fires unconditionally. Every deal gets `'USD'`.

Current `parseAndValidate` output shape (from [src/lib/ai.js:243-266](../../src/lib/ai.js#L243-L266)) — this is our contract:

```
{
  thread_id: string,
  is_deal: boolean,
  is_english: boolean,
  language: string | null,
  ai_score: number (1..10),
  category: 'likely_scam' | ... | 'low_confidence' | null,
  likely_scam: boolean,
  ai_insight: string,
  ai_summary: string (<=1000 chars),
  main_contact: object | null,
  deal_brand: string | null,
  deal_type: enum | 'other_business' | null,
  deal_name: string | null,
  deal_value: number | null,
  deal_currency: string | null,
}
```

The `thread_id` remap via `threadOrder` / `thread_index` is external context — **keep it outside the schema**. Schema validates the shape that consumers read; `parseAndValidate` still handles the thread-index indirection before passing rows through the schema.

---

## Task 1: Add zod dependency

**Files:**
- Modify: `package.json`
- Modify: `package-lock.json` (generated)

**Step 1: Install**

Run:

```bash
npm install zod@^3 --save
```

Expected: `package.json` gains `"zod": "^3.x.y"` under `dependencies`. `package-lock.json` updates.

**Step 2: Verify rollup bundles cleanly**

Run:

```bash
npm run package
```

Expected: `dist/index.js` regenerates with no errors. Bundle grows by ~12–30 KB.

**Step 3: Commit**

```bash
git add package.json package-lock.json dist/index.js
git commit -m "chore: add zod for AI output schema validation"
```

---

## Task 2: Define the AI thread schema

**Files:**
- Create: `src/lib/ai-schema.js`
- Test: `__tests__/ai-schema.test.js`

Constants to reuse: `VALID_CATEGORIES`, `VALID_DEAL_TYPES` — currently defined inside [src/lib/ai.js](../../src/lib/ai.js). Export them from `ai-schema.js` instead and re-import in `ai.js` (Task 3).

**Step 1: Write failing tests**

Create `__tests__/ai-schema.test.js`:

```js
import { AiThreadSchema, AiThreadArraySchema } from '../src/lib/ai-schema.js'

const baseDeal = {
  thread_id: 't1',
  is_deal: true,
  is_english: true,
  language: 'en',
  ai_score: 7,
  category: 'brand_deal',
  likely_scam: false,
  ai_insight: 'insight',
  ai_summary: 'summary',
  main_contact: { company: 'Acme' },
  deal_brand: 'Acme',
  deal_type: 'paid_partnership',
  deal_name: 'Acme Spring',
  deal_value: 2500,
  deal_currency: 'USD',
}

describe('AiThreadSchema', () => {
  test('valid thread parses cleanly', () => {
    const result = AiThreadSchema.safeParse(baseDeal)
    expect(result.success).toBe(true)
    expect(result.data.deal_value).toBe(2500)
    expect(result.data.deal_currency).toBe('USD')
  })

  test('stringy deal_value coerces to number', () => {
    const r = AiThreadSchema.safeParse({ ...baseDeal, deal_value: '2500' })
    expect(r.success).toBe(true)
    expect(r.data.deal_value).toBe(2500)
  })

  test('null deal_value preserved', () => {
    const r = AiThreadSchema.safeParse({ ...baseDeal, deal_value: null })
    expect(r.success).toBe(true)
    expect(r.data.deal_value).toBeNull()
  })

  test('ai_score clamps to 1..10', () => {
    expect(AiThreadSchema.safeParse({ ...baseDeal, ai_score: 99 }).data.ai_score).toBe(10)
    expect(AiThreadSchema.safeParse({ ...baseDeal, ai_score: -5 }).data.ai_score).toBe(1)
    expect(AiThreadSchema.safeParse({ ...baseDeal, ai_score: 3.6 }).data.ai_score).toBe(4)
  })

  test('unknown category coerces to low_confidence for deals', () => {
    const r = AiThreadSchema.safeParse({ ...baseDeal, category: 'bogus_value' })
    expect(r.success).toBe(true)
    expect(r.data.category).toBe('low_confidence')
  })

  test('unknown deal_type coerces to other_business for deals', () => {
    const r = AiThreadSchema.safeParse({ ...baseDeal, deal_type: 'bogus' })
    expect(r.success).toBe(true)
    expect(r.data.deal_type).toBe('other_business')
  })

  test('non-deal nullifies deal fields', () => {
    const r = AiThreadSchema.safeParse({
      ...baseDeal,
      is_deal: false,
      category: null,
      deal_type: null,
      deal_name: null,
      main_contact: null,
      deal_brand: null,
    })
    expect(r.success).toBe(true)
    expect(r.data.category).toBeNull()
    expect(r.data.deal_type).toBeNull()
  })

  test('ai_summary truncates to 1000 chars', () => {
    const long = 'a'.repeat(2000)
    const r = AiThreadSchema.safeParse({ ...baseDeal, ai_summary: long })
    expect(r.success).toBe(true)
    expect(r.data.ai_summary.length).toBe(1000)
  })

  test('extra unknown fields are stripped', () => {
    const r = AiThreadSchema.safeParse({ ...baseDeal, mystery_field: 'hi' })
    expect(r.success).toBe(true)
    expect(r.data.mystery_field).toBeUndefined()
  })

  test('array schema validates each element', () => {
    const r = AiThreadArraySchema.safeParse([baseDeal, { ...baseDeal, thread_id: 't2' }])
    expect(r.success).toBe(true)
    expect(r.data).toHaveLength(2)
  })
})
```

**Step 2: Run tests — expect failure**

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/ai-schema.test.js
```

Expected: FAIL — module `../src/lib/ai-schema.js` not found.

**Step 3: Implement the schema**

Create `src/lib/ai-schema.js`:

```js
import { z } from 'zod'

export const VALID_CATEGORIES = new Set([
  // COPY EXACT SET FROM src/lib/ai.js — do not guess.
])

export const VALID_DEAL_TYPES = new Set([
  // COPY EXACT SET FROM src/lib/ai.js — do not guess.
])

const clamp = (n, lo, hi) => Math.min(hi, Math.max(lo, n))

const numberOrNull = z
  .union([z.number(), z.string(), z.null(), z.undefined()])
  .transform((v) => {
    if (v == null || v === '') return null
    const n = typeof v === 'number' ? v : Number(v)
    return Number.isFinite(n) ? n : null
  })

export const AiThreadSchema = z
  .object({
    thread_id: z.any().transform((v) => String(v ?? '')),
    is_deal: z.any().transform(Boolean),
    is_english: z.any().transform((v) => v !== false),
    language: z.any().transform((v) => v ?? null),
    ai_score: z.any().transform((v) => clamp(Math.round(Number(v) || 5), 1, 10)),
    category: z.any(),
    likely_scam: z.any().transform(Boolean),
    ai_insight: z.any().transform((v) => String(v ?? '')),
    ai_summary: z.any().transform((v) => String(v ?? '').slice(0, 1000)),
    main_contact: z.any().transform((v) => v ?? null),
    deal_brand: z.any().transform((v) => v ?? null),
    deal_type: z.any(),
    deal_name: z.any().transform((v) => v ?? null),
    deal_value: numberOrNull,
    deal_currency: z
      .any()
      .transform((v) => (typeof v === 'string' && v.trim() ? v.trim() : null)),
  })
  .transform((r) => {
    const isDeal = Boolean(r.is_deal)
    const category = isDeal
      ? VALID_CATEGORIES.has(r.category) ? r.category : 'low_confidence'
      : null
    const dealType = isDeal
      ? VALID_DEAL_TYPES.has(r.deal_type) ? r.deal_type : 'other_business'
      : null
    return {
      ...r,
      category,
      likely_scam: r.likely_scam || r.category === 'likely_scam',
      main_contact: isDeal ? r.main_contact : null,
      deal_brand: isDeal ? r.deal_brand : null,
      deal_type: dealType,
      deal_name: isDeal ? r.deal_name : null,
    }
  })

export const AiThreadArraySchema = z.array(AiThreadSchema)
```

**Important:** before writing, open [src/lib/ai.js](../../src/lib/ai.js) and copy the actual `VALID_CATEGORIES` and `VALID_DEAL_TYPES` sets verbatim. Do not invent values.

**Step 4: Run tests — expect pass**

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/ai-schema.test.js
```

Expected: all tests PASS.

**Step 5: Commit**

```bash
git add src/lib/ai-schema.js __tests__/ai-schema.test.js
git commit -m "feat: add zod schema as single source of truth for AI thread output"
```

---

## Task 3: Wire schema into parseAndValidate

**Files:**
- Modify: [src/lib/ai.js](../../src/lib/ai.js) around lines 243–266 (the `parsed.map(...)` block) and the `VALID_CATEGORIES` / `VALID_DEAL_TYPES` definitions
- Test: `__tests__/ai.test.js` (extend if present; create if not)

**Step 1: Check for existing ai.test.js**

Run:

```bash
ls __tests__/
```

If `ai.test.js` exists, extend it. Otherwise, create one with a regression test.

**Step 2: Write/extend regression test**

Add to `__tests__/ai.test.js`:

```js
import { parseAndValidate } from '../src/lib/ai.js'

describe('parseAndValidate regression — contract shape', () => {
  test('returns the exact keys consumers read', () => {
    const raw = JSON.stringify([
      {
        thread_id: 't1',
        is_deal: true,
        is_english: true,
        language: 'en',
        ai_score: 8,
        category: 'brand_deal',
        likely_scam: false,
        ai_insight: 'x',
        ai_summary: 'y',
        main_contact: { company: 'Acme' },
        deal_brand: 'Acme',
        deal_type: 'paid_partnership',
        deal_name: 'Spring Campaign',
        deal_value: 2500,
        deal_currency: 'EUR',
      },
    ])
    const result = parseAndValidate(raw)
    expect(result).toHaveLength(1)
    const t = result[0]
    expect(t.thread_id).toBe('t1')
    expect(t.deal_value).toBe(2500)
    expect(t.deal_currency).toBe('EUR')
    expect(t.category).toBe('brand_deal')
    expect(t.deal_type).toBe('paid_partnership')
  })

  test('bad JSON throws / returns falsy — preserves Layer 2 retry semantics', () => {
    // Mirror whatever the current function does on bad input.
    // If it throws: expect(() => parseAndValidate('not json')).toThrow()
    // If it returns null/undefined: expect(parseAndValidate('not json')).toBeFalsy()
    // Read src/lib/ai.js before writing this assertion.
  })
})
```

Fill in the "bad JSON" test assertion by reading the current `parseAndValidate` error behavior in `src/lib/ai.js` — do not change its error contract; the Layer 2 corrective retry depends on it.

**Step 3: Run — expect pass against existing implementation**

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/ai.test.js
```

Expected: PASS (we're capturing current behavior as a regression guard before refactoring).

**Step 4: Refactor parseAndValidate to use the schema**

In `src/lib/ai.js`:

1. Remove local `VALID_CATEGORIES` and `VALID_DEAL_TYPES` definitions; import them from `./ai-schema.js`.
2. Replace the `parsed.map((r) => ({ ... }))` block (lines ~243–266) with:

```js
import { AiThreadArraySchema } from './ai-schema.js'

// ... inside parseAndValidate, after the "unwrap if object" block ...

// Apply thread_id re-mapping (requires external threadOrder context)
if (threadOrder) {
  for (const r of parsed) {
    if (r.thread_index != null) {
      r.thread_id = threadOrder[Math.max(0, Number(r.thread_index) - 1)] || r.thread_id
    }
  }
}

const result = AiThreadArraySchema.safeParse(parsed)
if (!result.success) {
  // Preserve existing error contract — mirror what the old code did.
  // If old code threw, throw here with result.error.message.
  throw new Error(`AI output schema validation failed: ${result.error.message}`)
}
return result.data
```

Match the existing error-handling contract exactly: if the old code threw on bad shape, throw. If it returned null/undefined, do the same. Grep for `parseAndValidate` callers in `src/` to confirm.

**Step 5: Run full test suite**

```bash
npm test
```

Expected: all tests PASS, including the regression test from Step 2 and the Task 2 schema tests.

**Step 6: Commit**

```bash
git add src/lib/ai.js __tests__/ai.test.js
git commit -m "refactor: route parseAndValidate through zod AI thread schema"
```

---

## Task 4: Extract pure deal-tuple mapper

**Files:**
- Create: `src/lib/deal-mapper.js`
- Test: `__tests__/deal-mapper.test.js`

**Step 1: Write failing tests**

Create `__tests__/deal-mapper.test.js`:

```js
import { threadToDealTuple } from '../src/lib/deal-mapper.js'

const baseThread = {
  thread_id: 'thread-abc',
  is_deal: true,
  category: 'brand_deal',
  deal_type: 'paid_partnership',
  deal_name: 'Spring Campaign',
  deal_value: 2500,
  deal_currency: 'USD',
  main_contact: { company: 'Acme Inc.' },
}

describe('threadToDealTuple', () => {
  test('happy path — numeric deal_value lands in tuple', () => {
    const tuple = threadToDealTuple(baseThread, { userId: 'user-1' })
    expect(tuple).toContain('2500')
    expect(tuple).toContain("'USD'")
    expect(tuple).toContain("'thread-abc'")
    expect(tuple).toContain("'user-1'")
    expect(tuple).toContain("'Acme Inc.'")
  })

  test('null deal_value → 0', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_value: null }, { userId: 'u' })
    expect(tuple).toMatch(/, 0, /)
  })

  test('NaN deal_value → 0', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_value: NaN }, { userId: 'u' })
    expect(tuple).toMatch(/, 0, /)
  })

  test('undefined deal_value → 0', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_value: undefined }, { userId: 'u' })
    expect(tuple).toMatch(/, 0, /)
  })

  test('negative deal_value → 0', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_value: -100 }, { userId: 'u' })
    expect(tuple).toMatch(/, 0, /)
  })

  test('deal_currency: EUR lands in tuple', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_currency: 'EUR' }, { userId: 'u' })
    expect(tuple).toContain("'EUR'")
  })

  test('null deal_currency falls back to USD', () => {
    const tuple = threadToDealTuple({ ...baseThread, deal_currency: null }, { userId: 'u' })
    expect(tuple).toContain("'USD'")
  })

  test("apostrophe in deal_name is SQL-escaped", () => {
    const tuple = threadToDealTuple(
      { ...baseThread, deal_name: "O'Brien's Deal" },
      { userId: 'u' }
    )
    expect(tuple).toContain("'O''Brien''s Deal'")
  })

  test('missing main_contact → empty brand', () => {
    const tuple = threadToDealTuple({ ...baseThread, main_contact: null }, { userId: 'u' })
    expect(tuple).toContain("''")
  })
})
```

**Step 2: Run — expect failure**

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/deal-mapper.test.js
```

Expected: FAIL — module not found.

**Step 3: Implement the mapper**

Create `src/lib/deal-mapper.js`:

```js
import { sanitizeId, sanitizeString } from './db.js'

export function threadToDealTuple(thread, { userId }) {
  const threadId = sanitizeId(thread.thread_id)
  const uid = userId ? sanitizeId(userId) : ''
  const dealId = threadId
  const dealName = sanitizeString(thread.deal_name || '')
  const dealType = sanitizeString(thread.deal_type || '')
  const rawValue = thread.deal_value
  const dealValue =
    typeof rawValue === 'number' && Number.isFinite(rawValue) && rawValue >= 0 ? rawValue : 0
  const rawCurrency =
    typeof thread.deal_currency === 'string' ? thread.deal_currency.trim() : ''
  const currency = sanitizeString(rawCurrency || 'USD')
  const brand = thread.main_contact
    ? sanitizeString(thread.main_contact.company || '')
    : ''
  const category = sanitizeString(thread.category || '')
  return `('${dealId}', '${uid}', '${threadId}', '', '${dealName}', '${dealType}', '${category}', ${dealValue}, '${currency}', '${brand}', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`
}
```

Verify `sanitizeId` and `sanitizeString` are actually exported from `src/lib/db.js`. If they live elsewhere, adjust the import.

**Step 4: Run — expect pass**

```bash
node --experimental-vm-modules node_modules/jest/bin/jest.js __tests__/deal-mapper.test.js
```

Expected: all tests PASS.

**Step 5: Commit**

```bash
git add src/lib/deal-mapper.js __tests__/deal-mapper.test.js
git commit -m "feat: extract threadToDealTuple as pure tested mapper"
```

---

## Task 5: Use the mapper in run-classify-pipeline

**Files:**
- Modify: [src/commands/run-classify-pipeline.js:588-607](../../src/commands/run-classify-pipeline.js#L588-L607)

**Step 1: Read the current block**

Open `src/commands/run-classify-pipeline.js` at line 588 and confirm the block still matches what's quoted in the bug report (`typeof thread.deal_value === 'string'`, `thread.currency`).

**Step 2: Replace the inline mapping**

Add near the top of the file, alongside existing lib imports:

```js
import { threadToDealTuple } from '../lib/deal-mapper.js'
```

Replace the `dealThreads.length > 0` block (currently lines ~589–607) with:

```js
if (dealThreads.length > 0) {
  const dealValues = dealThreads.map((thread) => {
    const userId = userByThread[thread.thread_id] || ''
    return threadToDealTuple(thread, { userId })
  })

  await batcher.pushDeals(dealValues)

  console.log(`[run-classify-pipeline] ${dealThreads.length} deals upserted`)
}
```

Notes:
- `sanitizeId` of `thread.thread_id` happens inside the mapper now — don't double-sanitize.
- `userByThread` is keyed by sanitized or raw thread_id; confirm by reading earlier in the same file. If the existing keying used a sanitized id, match it here.

**Step 3: Run the full test suite**

```bash
npm test
```

Expected: all existing tests still PASS plus the new mapper + schema tests.

**Step 4: Regenerate dist**

```bash
npm run package
```

Expected: `dist/index.js` updated cleanly.

**Step 5: Commit**

```bash
git add src/commands/run-classify-pipeline.js dist/index.js
git commit -m "fix: route classify pipeline deal upsert through threadToDealTuple

Replaces inline mapping where typeof check and field-name typo caused
every deal to persist with value=0 and currency='USD'. See bug report
at /tmp/dealsync-action-bug-report.md and design doc in docs/plans/."
```

---

## Task 6: Final verification

**Step 1: Full format + test + package**

```bash
npm run all
```

Expected: all pass, dist regenerated.

**Step 2: Sanity-grep for stale references**

```bash
```

Run:

```bash
npm test 2>&1 | tail -30
```

Use Grep tool to confirm no other code still references `thread.currency`:

Expected: no matches. If any exist, investigate whether they're the same bug and fix in-scope.

**Step 3: Deploy to testnet**

Follow the `deploy` skill. After deploy, classify a small batch of known deals.

**Step 4: Verify on SxT staging**

Use the `sxt` skill. Run:

```sql
SELECT VALUE, COUNT(*) AS N
FROM DEALSYNC_STG_V1.DEALS
WHERE CREATED_AT >= <deploy_time>
GROUP BY VALUE
ORDER BY N DESC;
```

Expected: a spread of non-zero values, not a single bucket at `0`.

**Step 5: Deploy to betanet, then prod**

Only after testnet verification shows non-zero values.

---

## Out of scope

- **Backfill.** ~4,771 prod rows (2026-04-04 → fix deploy) with `value = 0` that should have been real. Defer to a separate plan; source of truth is `AI_EVALUATION_AUDITS`.
- **Schema allowing `NULL` for `value`.** Current column is `NOT NULL`; keep `0` fallback until a schema change is scoped.
