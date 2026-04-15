import {
  AiThreadSchema,
  AiThreadArraySchema,
  VALID_CATEGORIES,
  VALID_DEAL_TYPES,
} from '../src/lib/ai-schema.js'

const baseDeal = {
  thread_id: 't1',
  is_deal: true,
  is_english: true,
  language: 'en',
  ai_score: 7,
  category: 'in_progress',
  likely_scam: false,
  ai_insight: 'insight',
  ai_summary: 'summary',
  main_contact: { company: 'Acme' },
  deal_brand: 'Acme',
  deal_type: 'brand_collaboration',
  deal_name: 'Acme Spring',
  deal_value: 2500,
  deal_currency: 'USD',
}

describe('AiThreadSchema', () => {
  test('exports category and deal-type sets', () => {
    expect(VALID_CATEGORIES.has('likely_scam')).toBe(true)
    expect(VALID_DEAL_TYPES.has('brand_collaboration')).toBe(true)
  })

  test('valid thread parses cleanly', () => {
    const result = AiThreadSchema.safeParse(baseDeal)
    expect(result.success).toBe(true)
    expect(result.data.deal_value).toBe(2500)
    expect(result.data.deal_currency).toBe('USD')
    expect(result.data.category).toBe('in_progress')
    expect(result.data.deal_type).toBe('brand_collaboration')
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

  test('unparsable deal_value becomes null', () => {
    const r = AiThreadSchema.safeParse({ ...baseDeal, deal_value: 'abc' })
    expect(r.success).toBe(true)
    expect(r.data.deal_value).toBeNull()
  })

  test('ai_score clamps to 1..10', () => {
    expect(AiThreadSchema.safeParse({ ...baseDeal, ai_score: 99 }).data.ai_score).toBe(10)
    expect(AiThreadSchema.safeParse({ ...baseDeal, ai_score: -5 }).data.ai_score).toBe(1)
    expect(AiThreadSchema.safeParse({ ...baseDeal, ai_score: 3.6 }).data.ai_score).toBe(4)
    expect(AiThreadSchema.safeParse({ ...baseDeal, ai_score: 'nonsense' }).data.ai_score).toBe(5)
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

  test('non-deal nullifies deal-related fields', () => {
    const r = AiThreadSchema.safeParse({
      ...baseDeal,
      is_deal: false,
    })
    expect(r.success).toBe(true)
    expect(r.data.category).toBeNull()
    expect(r.data.deal_type).toBeNull()
    expect(r.data.deal_name).toBeNull()
    expect(r.data.main_contact).toBeNull()
    expect(r.data.deal_brand).toBeNull()
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

  test('likely_scam forced true when category is likely_scam', () => {
    const r = AiThreadSchema.safeParse({ ...baseDeal, category: 'likely_scam', likely_scam: false })
    expect(r.success).toBe(true)
    expect(r.data.likely_scam).toBe(true)
  })

  test('likely_scam forced true from RAW category even when is_deal=false', () => {
    const r = AiThreadSchema.safeParse({
      ...baseDeal,
      is_deal: false,
      category: 'likely_scam',
      likely_scam: false,
    })
    expect(r.success).toBe(true)
    expect(r.data.likely_scam).toBe(true)
    expect(r.data.category).toBeNull() // is_deal gate still nullifies output category
  })

  test('is_english defaults to true when missing', () => {
    const r = AiThreadSchema.safeParse({ ...baseDeal, is_english: undefined })
    expect(r.success).toBe(true)
    expect(r.data.is_english).toBe(true)
  })

  test('array schema validates each element', () => {
    const r = AiThreadArraySchema.safeParse([baseDeal, { ...baseDeal, thread_id: 't2' }])
    expect(r.success).toBe(true)
    expect(r.data).toHaveLength(2)
    expect(r.data[1].thread_id).toBe('t2')
  })
})
