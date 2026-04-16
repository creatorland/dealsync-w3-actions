import { jest } from '@jest/globals'
import { buildScanCompleteEligibilitySql } from '../src/lib/scan-complete-sql.js'
import {
  rowToScanCompleteWebhookBody,
  getRowUserId,
  coerceNumber,
} from '../src/lib/scan-complete-payload.js'
import { firestoreDocumentHasScanCompleteSentAt } from '../src/lib/scan-complete-firestore.js'
import { postScanCompleteWebhook } from '../src/lib/scan-complete-webhook.js'

describe('buildScanCompleteEligibilitySql', () => {
  it('substitutes sanitized schemas', () => {
    const sql = buildScanCompleteEligibilitySql('EMAIL_CORE_STAGING', 'DEALSYNC_STG_V1')
    expect(sql).toContain('EMAIL_CORE_STAGING.sync_states')
    expect(sql).toContain('DEALSYNC_STG_V1.deal_states')
    expect(sql).not.toContain('{{')
  })

  it('rejects invalid schema', () => {
    expect(() => buildScanCompleteEligibilitySql('bad;schema', 'X')).toThrow('Invalid schema')
  })
})

describe('scan-complete-payload', () => {
  it('maps uppercase SxT columns to webhook body', () => {
    const body = rowToScanCompleteWebhookBody({
      USER_ID: 'u1',
      DB_NEW: 1,
      DB_IN_PROGRESS: 2,
      DB_COMPLETED: 3,
      DB_NOT_INTERESTED: 4,
      DB_LIKELY_SCAM: 5,
      DB_LOW_CONFIDENCE: 6,
      CONTACTS_ADDED: 7,
    })
    expect(body).toEqual({
      userId: 'u1',
      eventType: 'scan_complete',
      eventData: {
        dealCounts: {
          new: 1,
          inProgress: 2,
          completed: 3,
          likelyScam: 5,
          lowConfidence: 6,
          notInterested: 4,
        },
        contactsAdded: 7,
      },
    })
  })

  it('getRowUserId accepts user_id', () => {
    expect(getRowUserId({ user_id: 'abc' })).toBe('abc')
  })

  it('coerceNumber handles string decimals', () => {
    expect(coerceNumber('9')).toBe(9)
  })
})

describe('firestoreDocumentHasScanCompleteSentAt', () => {
  it('is false when field missing', () => {
    expect(firestoreDocumentHasScanCompleteSentAt({ fields: {} })).toBe(false)
    expect(firestoreDocumentHasScanCompleteSentAt(null)).toBe(false)
  })

  it('is true for integerValue', () => {
    expect(
      firestoreDocumentHasScanCompleteSentAt({
        fields: { scanCompleteSentAt: { integerValue: '1710000000' } },
      }),
    ).toBe(true)
  })

  it('is true for doubleValue', () => {
    expect(
      firestoreDocumentHasScanCompleteSentAt({
        fields: { scanCompleteSentAt: { doubleValue: 1710000000 } },
      }),
    ).toBe(true)
  })
})

describe('postScanCompleteWebhook', () => {
  const origFetch = global.fetch

  afterEach(() => {
    global.fetch = origFetch
  })

  it('POSTs JSON with x-shared-secret', async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      status: 201,
      text: async () => '',
    })
    const body = {
      userId: 'u',
      eventType: 'scan_complete',
      eventData: { dealCounts: {}, contactsAdded: 0 },
    }
    const r = await postScanCompleteWebhook('https://api.example.com/', 'sec', body)
    expect(r.ok).toBe(true)
    expect(global.fetch).toHaveBeenCalledWith(
      'https://api.example.com/dealsync-v2/webhooks',
      expect.objectContaining({
        method: 'POST',
        headers: expect.objectContaining({
          'Content-Type': 'application/json',
          'x-shared-secret': 'sec',
        }),
      }),
    )
    const call = global.fetch.mock.calls[0]
    expect(JSON.parse(call[1].body)).toEqual(body)
  })
})
