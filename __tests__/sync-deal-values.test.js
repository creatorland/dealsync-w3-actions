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
  withTimeout: jest.fn(),
}))

jest.unstable_mockModule('../prompts/system.md', () => ({ default: 's' }))
jest.unstable_mockModule('../prompts/user.md', () => ({ default: 'u {{THREAD_DATA}}' }))
jest.unstable_mockModule('../prompts/system-llama.md', () => ({ default: 'sl' }))

const core = await import('@actions/core')
const { runSyncDealValues } = await import('../src/commands/sync-deal-values.js')

function setInputs(inputs) {
  core.getInput.mockImplementation((name) => inputs[name] ?? '')
}

const DEFAULTS = {
  'sxt-auth-url': 'https://auth',
  'sxt-auth-secret': 'secret',
  'sxt-api-url': 'https://api',
  'sxt-biscuit': 'bisc',
  'sxt-schema': 'dealsync_stg_v1',
  'backfill-start-date': '2026-03-31',
  'backfill-batch-size': '500',
  'backfill-dry-run': 'false',
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
  setInputs(DEFAULTS)
})

test('backfills affected deal from audit JSON', async () => {
  executeSql
    .mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1', USER_ID: 'u1' }])
    .mockResolvedValueOnce([{ AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(1)
  expect(result.skipped.auditMissing).toBe(0)
  expect(result.totalScanned).toBe(1)
  const updateCall = executeSql.mock.calls.find(([sql]) => sql.startsWith('UPDATE'))
  expect(updateCall[0]).toContain('VALUE = 2500')
  expect(updateCall[0]).toContain("CURRENCY = 'EUR'")
  expect(updateCall[0]).toContain("WHERE ID = 'deal-1'")
  expect(updateCall[0]).toContain('VALUE = 0 OR VALUE IS NULL')
})

test('skips when audit is missing', async () => {
  executeSql
    .mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1', USER_ID: 'u1' }])
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(0)
  expect(result.skipped.auditMissing).toBe(1)
  expect(executeSql.mock.calls.find(([sql]) => sql.startsWith('UPDATE'))).toBeUndefined()
})

test('skips when thread not present in audit payload', async () => {
  executeSql
    .mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-missing', USER_ID: 'u1' }])
    .mockResolvedValueOnce([{ AI_EVALUATION: auditJson }])
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

test('dry-run does not issue UPDATE but counts as recovered', async () => {
  setInputs({ ...DEFAULTS, 'backfill-dry-run': 'true' })
  executeSql
    .mockResolvedValueOnce([{ ID: 'deal-1', THREAD_ID: 'thread-1', USER_ID: 'u1' }])
    .mockResolvedValueOnce([{ AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(1)
  expect(executeSql.mock.calls.find(([sql]) => sql.startsWith('UPDATE'))).toBeUndefined()
})

test('pagination advances via cursor and stops on short page', async () => {
  setInputs({ ...DEFAULTS, 'backfill-batch-size': '2' })
  executeSql
    .mockResolvedValueOnce([
      { ID: 'deal-a', THREAD_ID: 'thread-1', USER_ID: 'u1' },
      { ID: 'deal-b', THREAD_ID: 'thread-1', USER_ID: 'u1' },
    ])
    .mockResolvedValueOnce([{ AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce([{ AI_EVALUATION: auditJson }])
    .mockResolvedValueOnce([])
    .mockResolvedValueOnce([])

  const result = await runSyncDealValues()

  expect(result.recovered).toBe(2)
  expect(result.totalScanned).toBe(2)
  const selectCalls = executeSql.mock.calls.filter(([sql]) => sql.startsWith('SELECT ID'))
  expect(selectCalls.length).toBeGreaterThanOrEqual(2)
  expect(selectCalls[1][0]).toContain("ID > 'deal-b'")
})
