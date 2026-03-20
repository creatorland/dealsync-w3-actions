import { jest } from '@jest/globals'

const outputs = {}
jest.unstable_mockModule('@actions/core', () => ({
  getInput: jest.fn(),
  setOutput: jest.fn((name, value) => {
    outputs[name] = value
  }),
  setFailed: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
}))

const core = await import('@actions/core')
const {
  runFilter,
  checkAuthenticationResults,
  checkSender,
  checkBulkHeaders,
  checkSubject,
  checkSenderName,
  checkFreeEmail,
} = await import('../src/filter.js')

function makeEmail(overrides = {}) {
  return {
    id: 'test-id-1',
    messageId: 'msg-1',
    userId: 'user-1',
    topLevelHeaders: [
      { name: 'From', value: 'Jane Smith <jane@example.com>' },
      { name: 'Subject', value: 'Partnership Opportunity' },
      { name: 'Authentication-Results', value: 'dkim=pass spf=pass' },
    ],
    labelIds: [],
    ...overrides,
  }
}

function mockInputs(map) {
  core.getInput.mockImplementation((name) => map[name] ?? '')
}

describe('dealsync filter', () => {
  beforeEach(() => {
    jest.clearAllMocks()
    for (const key of Object.keys(outputs)) delete outputs[key]
  })

  describe('runFilter()', () => {
    it('passes email through all rules into filtered_ids', async () => {
      mockInputs({ emails: JSON.stringify([makeEmail()]) })

      const result = await runFilter()

      expect(result.filtered_ids).toBe("'test-id-1'")
      expect(result.rejected_ids).toBe('')
    })

    it('returns empty outputs for empty input', async () => {
      mockInputs({ emails: '[]' })

      const result = await runFilter()

      expect(result.filtered_ids).toBe('')
      expect(result.rejected_ids).toBe('')
    })

    it('returns empty outputs for missing input', async () => {
      mockInputs({ emails: '' })

      const result = await runFilter()

      expect(result.filtered_ids).toBe('')
      expect(result.rejected_ids).toBe('')
    })

    it('splits multiple emails into filtered and rejected', async () => {
      const emails = [
        makeEmail({ id: 'good-1' }),
        makeEmail({
          id: 'bad-1',
          topLevelHeaders: [
            { name: 'From', value: 'noreply@spam.com' },
            { name: 'Subject', value: 'Hello' },
            { name: 'Authentication-Results', value: 'dkim=pass' },
          ],
        }),
        makeEmail({ id: 'good-2' }),
      ]
      mockInputs({ emails: JSON.stringify(emails) })

      const result = await runFilter()

      expect(result.filtered_ids).toBe("'good-1','good-2'")
      expect(result.rejected_ids).toBe("'bad-1'")
    })

    it('rejects IDs with invalid characters', async () => {
      const emails = [makeEmail({ id: "'; DROP TABLE" })]
      mockInputs({ emails: JSON.stringify(emails) })

      await expect(runFilter()).rejects.toThrow('Invalid ID format')
    })

    it('handles parse error', async () => {
      mockInputs({ emails: '{not valid json' })

      await expect(runFilter()).rejects.toThrow()
    })

    it('decrypts emails input when encrypted', async () => {
      const { encryptValue } = await import('../../shared/crypto.js')
      const key = 'a'.repeat(64)
      const emails = [makeEmail()]
      const encrypted = encryptValue(JSON.stringify(emails), key)

      mockInputs({ emails: encrypted, 'encryption-key': key })

      const result = await runFilter()
      expect(result.filtered_ids).toBe("'test-id-1'")
    })
  })

  describe('Rule 1: checkAuthenticationResults', () => {
    it('passes when auth header is missing', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@example.com>' },
          { name: 'Subject', value: 'Hello' },
        ],
      })
      expect(checkAuthenticationResults(email)).toBe(false)
    })

    it('passes when dkim=pass is present', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'Authentication-Results', value: 'dkim=pass spf=fail' }],
      })
      expect(checkAuthenticationResults(email)).toBe(false)
    })

    it('passes when spf=pass is present', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'Authentication-Results', value: 'dkim=fail spf=pass' }],
      })
      expect(checkAuthenticationResults(email)).toBe(false)
    })

    it('passes when dmarc=pass is present', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'Authentication-Results', value: 'dmarc=pass' }],
      })
      expect(checkAuthenticationResults(email)).toBe(false)
    })

    it('rejects when auth header exists but no pass values', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'Authentication-Results', value: 'dkim=fail spf=fail dmarc=fail' },
        ],
      })
      expect(checkAuthenticationResults(email)).toBe(true)
    })

    it('handles case-insensitive header name', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'authentication-results', value: 'dkim=fail spf=fail' }],
      })
      expect(checkAuthenticationResults(email)).toBe(true)
    })
  })

  describe('Rule 2: checkSender', () => {
    it('passes for normal sender', () => {
      expect(checkSender(makeEmail())).toBe(false)
    })

    it('rejects blocked prefix (noreply@)', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'From', value: 'No Reply <noreply@company.com>' }],
      })
      expect(checkSender(email)).toBe(true)
    })

    it('rejects blocked prefix (do-not-reply@)', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'From', value: 'do-not-reply@company.com' }],
      })
      expect(checkSender(email)).toBe(true)
    })

    it('rejects blocked domain suffix (.gov)', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'From', value: 'John <john@agency.gov>' }],
      })
      expect(checkSender(email)).toBe(true)
    })

    it('passes when from header is missing', () => {
      const email = makeEmail({ topLevelHeaders: [] })
      expect(checkSender(email)).toBe(false)
    })
  })

  describe('Rule 3: checkBulkHeaders', () => {
    it('passes for normal email', () => {
      expect(checkBulkHeaders(makeEmail())).toBe(false)
    })

    it('rejects when list-id header present', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@example.com>' },
          { name: 'List-Id', value: '<list.example.com>' },
        ],
      })
      expect(checkBulkHeaders(email)).toBe(true)
    })

    it('rejects when precedence is bulk', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@example.com>' },
          { name: 'Precedence', value: 'bulk' },
        ],
      })
      expect(checkBulkHeaders(email)).toBe(true)
    })

    it('rejects when marketing tool name found in header value', () => {
      const email = makeEmail({
        topLevelHeaders: [
          { name: 'From', value: 'Jane <jane@example.com>' },
          { name: 'X-Mailer', value: 'Mailchimp Campaign v3.2' },
        ],
      })
      expect(checkBulkHeaders(email)).toBe(true)
    })
  })

  describe('Rule 4: checkSubject', () => {
    it('passes for normal subject', () => {
      expect(checkSubject(makeEmail())).toBe(false)
    })

    it('rejects subject containing "newsletter"', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'Subject', value: 'Weekly Newsletter - March 2026' }],
      })
      expect(checkSubject(email)).toBe(true)
    })

    it('rejects subject containing "order confirmation"', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'Subject', value: 'Your Order Confirmation #1234' }],
      })
      expect(checkSubject(email)).toBe(true)
    })

    it('passes when subject header is missing', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'From', value: 'Jane <jane@example.com>' }],
      })
      expect(checkSubject(email)).toBe(false)
    })
  })

  describe('Rule 5: checkSenderName', () => {
    it('passes for personalized name', () => {
      expect(checkSenderName(makeEmail())).toBe(false)
    })

    it('rejects "The Team" sender name', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'From', value: 'The Team <team@example.com>' }],
      })
      expect(checkSenderName(email)).toBe(true)
    })

    it('rejects quoted non-personalized name', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'From', value: '"The Team" <team@example.com>' }],
      })
      expect(checkSenderName(email)).toBe(true)
    })

    it('passes when no display name present', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'From', value: 'jane@example.com' }],
      })
      expect(checkSenderName(email)).toBe(false)
    })
  })

  describe('Rule 6: checkFreeEmail', () => {
    it('passes for normal business email', () => {
      expect(checkFreeEmail(makeEmail())).toBe(false)
    })

    it('rejects info@ on gmail', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'From', value: 'Info <info@gmail.com>' }],
      })
      expect(checkFreeEmail(email)).toBe(true)
    })

    it('passes for personal name on gmail', () => {
      const email = makeEmail({
        topLevelHeaders: [{ name: 'From', value: 'Jane <jane@gmail.com>' }],
      })
      expect(checkFreeEmail(email)).toBe(false)
    })

    it('passes when from header missing', () => {
      const email = makeEmail({ topLevelHeaders: [] })
      expect(checkFreeEmail(email)).toBe(false)
    })
  })

  describe('ID sanitization', () => {
    it('allows alphanumeric IDs with hyphens and underscores', async () => {
      const emails = [makeEmail({ id: 'abc-123_DEF' })]
      mockInputs({ emails: JSON.stringify(emails) })

      const result = await runFilter()
      expect(result.filtered_ids).toBe("'abc-123_DEF'")
    })

    it('rejects IDs with SQL injection characters', async () => {
      const emails = [makeEmail({ id: "'; DROP TABLE emails--" })]
      mockInputs({ emails: JSON.stringify(emails) })

      await expect(runFilter()).rejects.toThrow('Invalid ID format')
    })
  })
})
